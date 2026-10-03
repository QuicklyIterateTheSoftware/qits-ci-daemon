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
import eu.wohlben.qits.cidaemon.protocol.StepChunk;
import io.vertx.core.Vertx;
import java.util.List;
import java.util.TreeMap;
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
 * <p><b>A dropped socket is not an ending</b> while {@link ControlSocket} is re-dialling it. The
 * step keeps running through the outage, and what it must not lose is held here: an {@code
 * Initialized} that failed to send, the step's chunks (bounded, oldest dropped first) and the
 * terminal frame. On the new socket the daemon says {@code Hello} again, and the host's {@code Ack}
 * is what releases the held frames, in order — {@code Initialized}, chunks by {@code seq}, then the
 * terminal frame. Every {@code seq} keeps the number it was minted with, so a host that already saw
 * one drops the copy. Only a reconnect budget that runs out ends the process, as exit 6.
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

  /**
   * How much chunk text an outage may hold before the oldest is dropped. About a megabyte: an edge
   * redeploy is seconds, and a step that prints faster than this across one loses its oldest output
   * rather than the daemon its memory.
   */
  static final int MAX_HELD_CHUNK_CHARS = 1 << 20;

  private final CompletableFuture<Integer> exit = new CompletableFuture<>();
  private final AtomicBoolean acked = new AtomicBoolean();
  private final AtomicBoolean stepStarted = new AtomicBoolean();
  private volatile Step step;

  /**
   * Guards everything below. One lock across the decision to send and the send itself is what keeps
   * a chunk from the step's thread overtaking the replay of the ones held before it — and a host
   * that drops {@code seq <= lastSeq} would discard the held ones if it did.
   */
  private final Object outbox = new Object();

  /** True from the host's Ack on a socket until that socket drops. Frames go out only while true. */
  private boolean live;

  private boolean initializedPending;
  private final TreeMap<Long, StepChunk> heldChunks = new TreeMap<>();
  private long heldChars;
  private long droppedChunks;

  /** The terminal frame and the exit it ends in, once there is one; kept until it is written. */
  private CiDaemonMessage terminal;
  private int terminalCode;
  private boolean terminalInFlight;

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
        new ControlSocket(vertx, env.daemonUrl(), env.token(), settings, this);
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
    // The subject and never the token: it is what lets a human match this container to the run
    // the token was commissioned for, and it is the only part of the credential that may be read.
    LOG.infof(
        "ci-daemon presents the run's token (subject %s).",
        env.tokenSubject() == null || env.tokenSubject().isBlank()
            ? "unnamed"
            : env.tokenSubject());
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
    hello();
  }

  @Override
  public void onReconnected() {
    LOG.infof("ci-daemon re-registering as %s; awaiting Ack.", env.daemonId());
    hello();
  }

  @Override
  public void onDisconnected() {
    synchronized (outbox) {
      live = false;
    }
  }

  /**
   * A Hello that fails to send needs no handling of its own: the socket it was written to is gone,
   * and the re-dial that follows says it again.
   */
  private void hello() {
    socket
        .send(new Hello(env.daemonId(), CiDaemonProtocol.CAPABILITY_VERSION))
        .onFailure(t -> LOG.debugf("ci-daemon could not send Hello: %s", t.getMessage()));
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
    if (acked.compareAndSet(false, true)) {
      // Confirms host→daemon delivery, which Hello never did — see AckReceived's javadoc. Fired
      // before the clone starts and best-effort: a container probe is the only caller that waits
      // for it, and a probe that never sees it is REJECTED at its own deadline rather than this
      // daemon retrying a send the socket has already told it is gone. Only for the first Ack: the
      // one a reconnect earns re-admits a launch that was confirmed long ago, and a second clone
      // into the same checkout would be a second initialization of one container.
      socket
          .send(new AckReceived())
          .onFailure(t -> LOG.debugf("ci-daemon could not confirm the Ack: %s", t.getMessage()));
      workers.execute(this::initialize);
    }
    release();
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
    deliver(new Initialized());
  }

  private void onRunStep(RunStep request) {
    if (!stepStarted.compareAndSet(false, true)) {
      // Exactly one per container lifetime. A host re-admitting this launch after an outage may send
      // it again; anything else is a host bug or a hostile frame. Running it would give one
      // container's results two identities.
      LOG.infof("ci-daemon ignored a second RunStep (%s)", request.correlationId());
      return;
    }
    workers.execute(
        () -> {
          Step running = steps.create(request, this::deliver);
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
    finish(
        ExitCode.SOCKET_CLOSED_EARLY,
        "the control socket dropped and could not be re-established before a step could finish");
  }

  @Override
  public void onDialFailed(String detail) {
    LOG.errorf("ci-daemon could not reach qits-ci: %s", detail);
    finish(ExitCode.DIAL_FAILED, detail);
  }

  /**
   * Send a frame that must survive an outage — {@code Initialized} or a step chunk — or hold it
   * until the host has re-admitted this launch. Anything else that arrives here (there is nothing
   * else today) is sent best-effort.
   */
  private void deliver(CiDaemonMessage message) {
    synchronized (outbox) {
      if (live) {
        transmit(message);
      } else {
        hold(message);
      }
    }
  }

  /** Under {@link #outbox}. A write that fails means the socket is going; keep the frame. */
  private void transmit(CiDaemonMessage message) {
    socket
        .send(message)
        .onFailure(
            t -> {
              synchronized (outbox) {
                live = false;
                hold(message);
              }
            });
  }

  /** Under {@link #outbox}. */
  private void hold(CiDaemonMessage message) {
    switch (message) {
      case Initialized initialized -> initializedPending = true;
      case StepChunk chunk -> {
        if (heldChunks.put(chunk.seq(), chunk) == null) {
          heldChars += chunk.text().length();
        }
        while (heldChars > MAX_HELD_CHUNK_CHARS && heldChunks.size() > 1) {
          heldChars -= heldChunks.pollFirstEntry().getValue().text().length();
          if (droppedChunks++ == 0) {
            LOG.warnf(
                "ci-daemon holds more than %d characters of output for the control socket's"
                    + " return; dropping the oldest",
                MAX_HELD_CHUNK_CHARS);
          }
        }
      }
      default -> {}
    }
  }

  /**
   * The host has Acked on this socket: send what the outage held, in the order it was produced, and
   * go live. On the first Ack there is nothing held and this only opens the gate.
   */
  private void release() {
    synchronized (outbox) {
      if (droppedChunks > 0) {
        LOG.warnf("ci-daemon dropped %d chunk(s) of output during the outage", droppedChunks);
        droppedChunks = 0;
      }
      List<StepChunk> replay = List.copyOf(heldChunks.values());
      heldChunks.clear();
      heldChars = 0;
      live = true;
      if (initializedPending) {
        initializedPending = false;
        transmit(new Initialized());
      }
      if (!replay.isEmpty()) {
        LOG.infof("ci-daemon replaying %d chunk(s) held through the outage", replay.size());
      }
      replay.forEach(this::transmit);
      if (terminal != null && !terminalInFlight) {
        transmitTerminal();
      }
    }
  }

  /**
   * The terminal frame, then the close, then the exit — each waiting on the last, so the result
   * cannot be lost to a process that exited while the write was still queued. A write that fails
   * keeps the frame for the reconnect; only the reconnect budget running out gives up on it.
   */
  private void sendAndFinish(CiDaemonMessage message, int code) {
    synchronized (outbox) {
      terminal = message;
      terminalCode = code;
      if (live) {
        transmitTerminal();
      } else {
        LOG.infof(
            "ci-daemon holding its %s until the control socket is back",
            message.getClass().getSimpleName());
      }
    }
  }

  /** Under {@link #outbox}. */
  private void transmitTerminal() {
    CiDaemonMessage message = terminal;
    int code = terminalCode;
    terminalInFlight = true;
    socket
        .send(message)
        .onComplete(
            written -> {
              if (written.succeeded()) {
                closeAndFinish(code);
                return;
              }
              LOG.warnf(
                  "ci-daemon could not deliver its %s (%s); holding it for the reconnect",
                  message.getClass().getSimpleName(), written.cause().getMessage());
              synchronized (outbox) {
                terminalInFlight = false;
                live = false;
              }
            });
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
