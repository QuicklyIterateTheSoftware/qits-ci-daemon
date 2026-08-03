package eu.wohlben.qits.cidaemon;

import eu.wohlben.qits.cidaemon.protocol.Ack;
import eu.wohlben.qits.cidaemon.protocol.AckReceived;
import eu.wohlben.qits.cidaemon.protocol.Cancel;
import eu.wohlben.qits.cidaemon.protocol.CiDaemonMessage;
import eu.wohlben.qits.cidaemon.protocol.CiDaemonProtocol;
import eu.wohlben.qits.cidaemon.protocol.Hello;
import eu.wohlben.qits.cidaemon.protocol.InitFailed;
import eu.wohlben.qits.cidaemon.protocol.Initialized;
import eu.wohlben.qits.cidaemon.protocol.RunStep;
import eu.wohlben.qits.cidaemon.protocol.StepFinished;
import io.vertx.core.Vertx;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.jboss.logging.Logger;

/**
 * The whole life of a step container, in one flow:
 *
 * <pre>
 *   dial → Hello → Ack → AckReceived → clone+checkout → Initialized → RunStep → StepChunk* →
 *   StepFinished → exit
 * </pre>
 *
 * with {@code Heartbeat} underneath from dial to close, {@code InitFailed} standing in for {@code
 * Initialized} when the checkout fails, and {@code Cancel} as the only other thing the host may say.
 *
 * <p><b>Every ending is an exit.</b> That is the inversion of the qits-workspace-daemon shape this
 * class otherwise follows, and it is the reason the endings are enumerated rather than handled: a
 * daemon that fell through to "keep waiting" would leave a container alive with nothing to do,
 * holding a slot the host has already accounted for. See {@link ExitCode} for which ending is which,
 * and why only a delivered {@code StepFinished} is a zero.
 *
 * <p>A plain class with a plain constructor, not a bean: {@link Main} is the one place that resolves
 * configuration, and everything here arrives as an argument. That is also what lets the suite drive
 * this flow against a real in-JVM Vert.x server with a scripted checkout and a scripted step.
 */
public final class DaemonMain implements ControlSocket.Listener {

  private static final Logger LOG = Logger.getLogger(DaemonMain.class);

  /** Prepares the checkout. {@link Workspace#prepare()} in production. */
  @FunctionalInterface
  public interface Initializer {
    Workspace.Preparation prepare();
  }

  /** Builds the step's execution. {@link StepProcess}'s constructor in production. */
  @FunctionalInterface
  public interface Steps {
    Step create(RunStep request, Consumer<CiDaemonMessage> emit);
  }

  private final DaemonEnv env;
  private final ControlSocket socket;
  private final Initializer initializer;
  private final Steps steps;

  /**
   * Off-event-loop pool for the two blocking things this daemon does — the clone and the step.
   * Frames are handled on a Vert.x event loop, so neither may run there.
   */
  private final ExecutorService workers =
      Executors.newCachedThreadPool(
          runnable -> {
            Thread thread = new Thread(runnable, "ci-daemon-worker");
            thread.setDaemon(true);
            return thread;
          });

  private final CompletableFuture<Integer> exit = new CompletableFuture<>();
  private final AtomicBoolean stepStarted = new AtomicBoolean();
  private volatile Step step;

  public DaemonMain(
      Vertx vertx,
      DaemonEnv env,
      ControlSocket.Settings settings,
      Initializer initializer,
      Steps steps) {
    this.env = env;
    this.initializer = initializer;
    this.steps = steps;
    this.socket =
        new ControlSocket(
            vertx, env.daemonUrl(), env.daemonId(), env.daemonSecret(), settings, this);
  }

  /**
   * Run to a terminal condition and return the process exit code. Blocks the calling thread, which
   * is the application's main thread — there is nothing else for it to do, and the daemon exiting
   * <em>is</em> the container's completion.
   */
  public int run() {
    String missing = env.missing();
    if (missing != null) {
      // Nothing to report this over: the socket needs the very values that are absent. The
      // container's stdout is the only channel, and qits-ci captures a bounded `docker logs` tail
      // when a container never registers, so this line is the diagnosis a human gets.
      LOG.errorf("ci-daemon cannot start: %s is not set. Exiting.", missing);
      return ExitCode.MISCONFIGURED;
    }
    socket.start();
    try {
      return exit.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return ExitCode.SOCKET_CLOSED_EARLY;
    } catch (Exception e) {
      LOG.error("ci-daemon ended abnormally", e);
      return ExitCode.SOCKET_CLOSED_EARLY;
    } finally {
      workers.shutdownNow();
      socket.shutdown();
    }
  }

  @Override
  public void onConnected() {
    LOG.infof("ci-daemon registered as %s; awaiting Ack.", env.daemonId());
    socket
        .send(new Hello(env.daemonId(), CiDaemonProtocol.CAPABILITY_VERSION))
        .onFailure(t -> finish(ExitCode.SOCKET_CLOSED_EARLY, "Hello could not be sent"));
  }

  @Override
  public void onMessage(CiDaemonMessage message) {
    switch (message) {
      case Ack ack -> onAck(ack);
      case RunStep request -> onRunStep(request);
      case Cancel cancel -> onCancel(cancel);
      default ->
          // Everything else in the sealed set is daemon→host. A host echoing one back is not a
          // conversation this version has, and there is nothing to do about it but say so.
          LOG.debugf("ci-daemon ignored a %s from the host", message.getClass().getSimpleName());
    }
  }

  private void onAck(Ack ack) {
    if (ack.capabilityVersion() != CiDaemonProtocol.CAPABILITY_VERSION) {
      LOG.errorf(
          "ci-daemon speaks capability version %d, the host answered %d — exiting rather than"
              + " guessing.",
          CiDaemonProtocol.CAPABILITY_VERSION, ack.capabilityVersion());
      closeAndFinish(ExitCode.CAPABILITY_MISMATCH);
      return;
    }
    // Confirms host→daemon delivery, which Hello never did — see AckReceived's javadoc. Fired
    // before the clone starts and best-effort: a container probe is the only caller that waits for
    // it, and a probe that never sees it is REJECTED at its own deadline rather than this daemon
    // retrying a send the socket has already told it is gone.
    socket
        .send(new AckReceived())
        .onFailure(t -> LOG.debugf("ci-daemon could not confirm the Ack: %s", t.getMessage()));
    workers.execute(this::initialize);
  }

  /**
   * The clone and checkout, off the event loop. A failure ends the container here: there is no step
   * to run without a checkout, and {@code InitFailed} carries the reason the host branches on —
   * {@code SHA_GONE} in particular, which is what makes the force-push case a discarded run rather
   * than a recorded failure against a commit nobody can look at.
   */
  private void initialize() {
    Workspace.Preparation preparation;
    try {
      preparation = initializer.prepare();
    } catch (RuntimeException e) {
      LOG.error("ci-daemon initialization threw", e);
      preparation =
          new Workspace.Preparation(InitFailed.Reason.CLONE_FAILED, String.valueOf(e.getMessage()));
    }
    if (!preparation.ready()) {
      LOG.errorf("ci-daemon initialization failed: %s", preparation.failure());
      sendAndFinish(
          new InitFailed(preparation.failure(), preparation.detail()), ExitCode.INIT_FAILED_SENT);
      return;
    }
    socket
        .send(new Initialized())
        .onFailure(t -> finish(ExitCode.SOCKET_CLOSED_EARLY, "Initialized could not be sent"));
  }

  private void onRunStep(RunStep request) {
    if (!stepStarted.compareAndSet(false, true)) {
      // Exactly one per container lifetime. A second one is a host bug or a hostile frame; running
      // it would give one container's results two identities.
      LOG.warnf("ci-daemon ignored a second RunStep (%s)", request.correlationId());
      return;
    }
    workers.execute(
        () -> {
          Step running = steps.create(request, this::sendChunk);
          step = running;
          StepFinished finished = running.run();
          // The step's terminal frame, then the close, then the exit — in that order, each waiting
          // on the last, so the result cannot be lost to a process that exited while the write was
          // still queued.
          sendAndFinish(finished, ExitCode.OK);
        });
  }

  private void onCancel(Cancel cancel) {
    Step running = step;
    if (running == null) {
      LOG.warnf("ci-daemon received Cancel (%s) with no step running — exiting.", cancel.correlationId());
      closeAndFinish(ExitCode.CANCELLED_BEFORE_STEP);
      return;
    }
    LOG.infof("ci-daemon cancelling step %s", cancel.correlationId());
    // Off the event loop: the kill waits out its own grace period.
    workers.execute(running::cancel);
  }

  @Override
  public void onClosed() {
    // Already ending (the terminal frame's close, or a race with it) — nothing to say.
    finish(ExitCode.SOCKET_CLOSED_EARLY, "the control socket closed before a step could finish");
  }

  @Override
  public void onDialFailed(String detail) {
    LOG.errorf("ci-daemon could not reach qits-ci: %s", detail);
    finish(ExitCode.DIAL_FAILED, detail);
  }

  /** Chunks are best-effort: if the socket is gone, {@link #onClosed} is already ending us. */
  private void sendChunk(CiDaemonMessage chunk) {
    socket.send(chunk);
  }

  private void sendAndFinish(CiDaemonMessage message, int code) {
    socket
        .send(message)
        .onFailure(t -> LOG.errorf("ci-daemon could not deliver its terminal frame: %s", t.getMessage()))
        .eventually(socket::close)
        .onComplete(v -> finish(code, null));
  }

  private void closeAndFinish(int code) {
    socket.close().onComplete(v -> finish(code, null));
  }

  private void finish(int code, String why) {
    if (exit.complete(code)) {
      if (why != null) {
        LOG.warnf("ci-daemon exiting %d: %s", code, why);
      } else {
        LOG.infof("ci-daemon exiting %d", code);
      }
    }
  }
}
