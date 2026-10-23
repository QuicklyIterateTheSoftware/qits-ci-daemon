package eu.wohlben.qits.cidaemon;

import eu.wohlben.qits.cidaemon.protocol.Ack;
import eu.wohlben.qits.cidaemon.protocol.AckReceived;
import eu.wohlben.qits.cidaemon.protocol.Cancel;
import eu.wohlben.qits.cidaemon.protocol.CiDaemonMessage;
import eu.wohlben.qits.cidaemon.protocol.CiDaemonProtocol;
import eu.wohlben.qits.cidaemon.protocol.Close;
import eu.wohlben.qits.cidaemon.protocol.Hello;
import eu.wohlben.qits.cidaemon.protocol.InitFailed;
import eu.wohlben.qits.cidaemon.protocol.Initialized;
import eu.wohlben.qits.cidaemon.protocol.RunStep;
import eu.wohlben.qits.cidaemon.protocol.StepFinished;
import eu.wohlben.qits.cidaemon.protocol.StepChunk;
import io.vertx.core.Vertx;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.jboss.logging.Logger;

/**
 * The whole life of a CI lease's container, in one flow:
 *
 * <pre>
 *   dial → Hello → Ack → AckReceived → mirror + worktree → Initialized →
 *     (RunStep → StepChunk* → StepFinished)* → Close → worktree remove → flush telemetry → exit 0
 * </pre>
 *
 * with {@code Heartbeat} underneath from dial to close, {@code InitFailed} standing in for {@code
 * Initialized} when the worktree cannot be made, and {@code Cancel} killing the step that is
 * running. Steps run one at a time, in the order they arrive, on one lane that also runs the
 * initialization and the {@code Close} — so a {@code RunStep} that arrives early waits for the
 * worktree, and the worktree is removed only after the last step on the lane is done.
 *
 * <p><b>Every ending is an exit</b>, and every ending after initialization removes the worktree
 * first: the mirror lives on the target's cache volume and outlives this container, so a worktree
 * left registered in it is bookkeeping the next run's {@code worktree prune} has to find. See {@link
 * ExitCode} for which ending is which; only a {@code Close} answered is a zero.
 *
 * <p><b>A dropped socket is not an ending</b> while {@link ControlSocket} is re-dialling it. The
 * steps keep running through the outage, and what they must not lose is held here, in the order it
 * was produced: an {@code Initialized} that failed to send, every step's chunks (bounded, oldest
 * dropped first) and every {@code StepFinished} (never dropped). On the new socket the daemon says
 * {@code Hello} again, and the host's {@code Ack} releases the held frames in order. An {@code
 * Initialized} that was written but never answered by a {@code RunStep} is sent again too. Only a
 * reconnect budget that runs out ends the process, as exit 6.
 *
 * <p><b>Telemetry is flushed once, at the ending, before the socket is closed.</b> A {@code
 * StepFinished} no longer ends the container, so there is no reaping to race until qits-ci deletes
 * the lease after {@code Close}; the flush is composed in front of the close for the same reason it
 * used to sit in front of the terminal frame. An {@code InitFailed} is still flushed before it is
 * sent, because the host ends the lease the moment it reads one.
 *
 * <p>A plain class with a plain constructor, not a bean: {@link Main} is the one place that resolves
 * configuration, and everything here arrives as an argument. That is also what lets the suite drive
 * this flow against a real in-JVM Vert.x server with a scripted worktree and real steps.
 */
public final class DaemonMain implements ControlSocket.Listener {

  private static final Logger LOG = Logger.getLogger(DaemonMain.class);

  /** The run's checkout. {@link MirrorWorktree} in production. */
  public interface Worktree {
    /** Make the worktree, or say why not. */
    MirrorWorktree.Preparation prepare();

    /** Remove it again; idempotent, never throws, a no-op when nothing was made. */
    void remove();
  }

  /** Builds one step's execution. {@link StepProcess}'s constructor in production. */
  @FunctionalInterface
  public interface Steps {
    Step create(RunStep request, Consumer<CiDaemonMessage> emit);
  }

  private final DaemonEnv env;
  private final ControlSocket socket;
  private final Worktree worktree;
  private final Steps steps;
  private final Telemetry telemetry;

  /** Off-event-loop pool for what must not wait behind the lane: a cancel's kill and its grace. */
  private final ExecutorService workers =
      Executors.newCachedThreadPool(
          runnable -> {
            Thread thread = new Thread(runnable, "ci-daemon-worker");
            thread.setDaemon(true);
            return thread;
          });

  /**
   * The lane: initialization, every step and the teardown, one after another. One thread is the
   * whole ordering guarantee — two steps cannot share the worktree at once, and the worktree is not
   * removed under a step.
   */
  private final ExecutorService lane =
      Executors.newSingleThreadExecutor(
          runnable -> {
            Thread thread = new Thread(runnable, "ci-daemon-lane");
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
  private final AtomicBoolean ending = new AtomicBoolean();

  /** A RunStep has been accepted at all; the Initialized resend and exit 7 both ask. */
  private final AtomicBoolean anyStep = new AtomicBoolean();

  /** Highest {@code stepIndex} accepted; a RunStep at or below it is a replay and is ignored. */
  private int lastStepIndex = -1;

  private volatile boolean ready;
  private volatile Step step;
  private volatile boolean cancelRequested;

  /** The first step failure, as the root span's error at the ending; null while all passed. */
  private volatile String firstFailure;

  /**
   * First connect to first {@code Ack}; see {@link Spans}. The other children live on the lane that
   * runs them, so only this one, which ends on the event loop, is a field.
   */
  private volatile Spans.Span dialSpan;

  /**
   * Guards everything below. One lock across the decision to send and the send itself is what keeps
   * a frame from a step overtaking the replay of the ones held before it.
   */
  private final Object outbox = new Object();

  /** True from the host's Ack on a socket until that socket drops. Frames go out only while true. */
  private boolean live;

  private boolean initializedPending;

  /** The worktree is ready and {@code Initialized} was sent at least once. */
  private boolean initializedSent;

  /**
   * Held frames by the order they were produced in — chunks and {@code StepFinished}s alike. A key
   * is taken when a frame is first delivered, so a write that fails on a dying socket after later
   * frames were already held still goes back in its own place.
   */
  private final TreeMap<Long, CiDaemonMessage> held = new TreeMap<>();

  private long nextOrdinal;
  private long heldChars;
  private long droppedChunks;

  /** The terminal frame ({@code InitFailed}) and the exit it ends in; kept until it is written. */
  private CiDaemonMessage terminal;
  private int terminalCode;
  private boolean terminalInFlight;

  /** A daemon whose telemetry is off. */
  public DaemonMain(
      Vertx vertx, DaemonEnv env, ControlSocket.Settings settings, Worktree worktree, Steps steps) {
    this(vertx, env, settings, worktree, steps, Telemetry.off());
  }

  public DaemonMain(
      Vertx vertx,
      DaemonEnv env,
      ControlSocket.Settings settings,
      Worktree worktree,
      Steps steps,
      Telemetry telemetry) {
    this.env = env;
    this.worktree = worktree;
    this.steps = steps;
    this.telemetry = telemetry;
    this.socket = new ControlSocket(vertx, env.daemonUrl(), env.token(), settings, this);
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
      // container's stdout is the only channel, so this line is the diagnosis a human gets.
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
    telemetry.start();
    dialSpan = telemetry.span("dial");
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
      lane.shutdownNow();
      socket.shutdown();
      telemetry.shutdown();
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
      case Close close -> onClose();
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
      String why =
          "the host answered capability version "
              + ack.capabilityVersion()
              + ", this binary speaks "
              + CiDaemonProtocol.CAPABILITY_VERSION;
      endSpan(dialSpan, why);
      closeAndFinish(ExitCode.CAPABILITY_MISMATCH, why);
      return;
    }
    if (acked.compareAndSet(false, true)) {
      endSpan(dialSpan, null);
      // Confirms host→daemon delivery, which Hello never did — see AckReceived's javadoc. Fired
      // before the worktree is made and best-effort. Only for the first Ack: the one a reconnect
      // earns re-admits a launch that was confirmed long ago, and a second worktree for the same
      // run would be a second initialization of one lease.
      socket
          .send(new AckReceived())
          .onFailure(t -> LOG.debugf("ci-daemon could not confirm the Ack: %s", t.getMessage()));
      lane.execute(this::initialize);
    }
    release();
  }

  /**
   * The mirror and the worktree, on the lane. A failure ends the lease here: there is no step to run
   * without a checkout, and {@code InitFailed} carries the reason the host branches on — {@code
   * SHA_GONE} in particular, which is what makes the force-push case a discarded run rather than a
   * recorded failure against a commit nobody can look at.
   */
  private void initialize() {
    Spans.Span checkout = telemetry.span("checkout");
    MirrorWorktree.Preparation preparation;
    try {
      preparation = worktree.prepare();
    } catch (RuntimeException e) {
      LOG.error("ci-daemon initialization threw", e);
      preparation =
          new MirrorWorktree.Preparation(
              InitFailed.Reason.CLONE_FAILED, String.valueOf(e.getMessage()));
    }
    if (!preparation.ready()) {
      LOG.errorf("ci-daemon initialization failed: %s", preparation.failure());
      String why = "InitFailed " + preparation.failure() + ": " + preparation.detail();
      checkout.end(why);
      // Whatever half of it was made goes before the frame does: the host ends the lease on it.
      worktree.remove();
      sendAndFinish(
          new InitFailed(preparation.failure(), preparation.detail()),
          ExitCode.INIT_FAILED_SENT,
          why);
      return;
    }
    checkout.end();
    ready = true;
    synchronized (outbox) {
      initializedSent = true;
      deliver(new Initialized());
    }
  }

  /** On the event loop: accept or drop, then queue on the lane. */
  private void onRunStep(RunStep request) {
    synchronized (this) {
      if (ending.get()) {
        LOG.infof(
            "ci-daemon ignored RunStep %d (%s): the lease is ending",
            request.stepIndex(), request.correlationId());
        return;
      }
      if (request.stepIndex() <= lastStepIndex) {
        // A host re-admitting this launch after an outage may send a step again. Running it twice
        // would give one step two results.
        LOG.infof(
            "ci-daemon ignored a repeated RunStep %d (%s)",
            request.stepIndex(), request.correlationId());
        return;
      }
      lastStepIndex = request.stepIndex();
    }
    anyStep.set(true);
    lane.execute(() -> runStep(request));
  }

  /** On the lane. */
  private void runStep(RunStep request) {
    if (ending.get() || !ready) {
      // Initialization failed (InitFailed is on its way) or a Close got here first.
      return;
    }
    Spans.Span script = telemetry.span("script");
    script.attribute("qits.ci.step_index", (long) request.stepIndex());
    cancelRequested = false;
    Step running =
        steps.create(
            request,
            message -> {
              if (message instanceof StepChunk chunk) {
                telemetry.output(chunk);
              }
              deliver(message);
            });
    step = running;
    StepFinished finished;
    try {
      finished = running.run();
    } finally {
      step = null;
    }
    String why = failure(finished);
    script.attribute("qits.ci.exit_code", (long) finished.exitCode());
    script.attribute("qits.ci.timed_out", finished.timedOut());
    if (cancelRequested) {
      script.attribute("qits.ci.cancelled", true);
    }
    script.end(why);
    if (why != null && firstFailure == null) {
      firstFailure = "step " + request.stepIndex() + ": " + why;
    }
    // Not the terminal frame any more: the lease goes on to the next RunStep or the Close. It is
    // held through an outage like the chunks before it, and after them.
    deliver(finished);
  }

  private void onCancel(Cancel cancel) {
    Step running = step;
    if (running == null) {
      if (!anyStep.get()) {
        LOG.warnf(
            "ci-daemon received Cancel (%s) with no step run yet — exiting.",
            cancel.correlationId());
        teardown(ExitCode.CANCELLED_BEFORE_STEP, "cancelled before a step ran", true);
      } else {
        // Between steps there is nothing to kill; the host follows with Close.
        LOG.infof(
            "ci-daemon received Cancel (%s) between steps; nothing to kill",
            cancel.correlationId());
      }
      return;
    }
    LOG.infof("ci-daemon cancelling step %s", cancel.correlationId());
    cancelRequested = true;
    // Off the event loop and off the lane: the kill waits out its own grace period, and the lane is
    // the thread the step it kills is blocking.
    workers.execute(running::cancel);
  }

  /**
   * The host is done with the lease. A step still running is killed — its {@code StepFinished} is
   * still sent — and the teardown queues behind it on the lane, so the worktree is removed only once
   * nothing is using it.
   */
  private void onClose() {
    LOG.info("ci-daemon received Close; removing the worktree and exiting.");
    // The root span's error is read on the lane, once the step this Close may kill has finished.
    teardown(ExitCode.OK, null, true);
  }

  /**
   * Every ending after the dial: kill the running step, then — on the lane, behind it — remove the
   * worktree, flush, close (when the socket is still ours to close) and exit. Runs once.
   */
  private void teardown(int code, String rootError, boolean closeSocket) {
    teardown(code, rootError, closeSocket, null);
  }

  private void teardown(int code, String rootError, boolean closeSocket, String why) {
    if (!ending.compareAndSet(false, true)) {
      return;
    }
    Step running = step;
    if (running != null) {
      cancelRequested = true;
      workers.execute(running::cancel);
    }
    try {
      lane.execute(
          () -> {
            worktree.remove();
            String error = rootError != null ? rootError : firstFailure;
            if (closeSocket) {
              closeAndFinish(code, error);
            } else {
              flushAndFinish(code, why, error);
            }
          });
    } catch (java.util.concurrent.RejectedExecutionException e) {
      finish(code, why);
    }
  }

  @Override
  public void onClosed() {
    String why =
        "the control socket dropped and could not be re-established before the host closed the"
            + " lease";
    teardown(ExitCode.SOCKET_CLOSED_EARLY, "socket lost: " + why, false, why);
  }

  @Override
  public void onDialFailed(String detail) {
    LOG.errorf("ci-daemon could not reach qits-ci: %s", detail);
    endSpan(dialSpan, detail);
    // Nothing was made: the worktree comes after the first Ack.
    flushAndFinish(ExitCode.DIAL_FAILED, detail, "dial failed: " + detail);
  }

  /**
   * Send a frame that must survive an outage — {@code Initialized}, a step chunk, a {@code
   * StepFinished} — or hold it until the host has re-admitted this launch.
   */
  private void deliver(CiDaemonMessage message) {
    synchronized (outbox) {
      long ordinal = nextOrdinal++;
      if (live) {
        transmit(ordinal, message);
      } else {
        hold(ordinal, message);
      }
    }
  }

  /** Under {@link #outbox}. A write that fails means the socket is going; keep the frame. */
  private void transmit(long ordinal, CiDaemonMessage message) {
    socket
        .send(message)
        .onFailure(
            t -> {
              synchronized (outbox) {
                live = false;
                hold(ordinal, message);
              }
            });
  }

  /** Under {@link #outbox}. */
  private void hold(long ordinal, CiDaemonMessage message) {
    switch (message) {
      case Initialized initialized -> initializedPending = true;
      case StepChunk chunk -> {
        if (held.put(ordinal, chunk) == null) {
          heldChars += chunk.text().length();
        }
        dropOldestChunks();
      }
      case StepFinished finished -> held.put(ordinal, finished);
      default -> {}
    }
  }

  /** Under {@link #outbox}. Only chunks are dropped; a step's result never is. */
  private void dropOldestChunks() {
    var it = held.entrySet().iterator();
    while (heldChars > MAX_HELD_CHUNK_CHARS && it.hasNext()) {
      CiDaemonMessage oldest = it.next().getValue();
      if (oldest instanceof StepChunk chunk && heldChars - chunk.text().length() > 0) {
        it.remove();
        heldChars -= chunk.text().length();
        if (droppedChunks++ == 0) {
          LOG.warnf(
              "ci-daemon holds more than %d characters of output for the control socket's"
                  + " return; dropping the oldest",
              MAX_HELD_CHUNK_CHARS);
        }
      }
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
      var replay = new java.util.ArrayList<>(held.entrySet());
      held.clear();
      heldChars = 0;
      live = true;
      // Also re-sent when the first copy was written but no RunStep has come back: a write the
      // socket accepted is not a frame the host read, and a host still waiting for Initialized
      // would otherwise wait out its own init deadline. The host ignores a duplicate.
      if (initializedPending || (initializedSent && !anyStep.get())) {
        initializedPending = false;
        socket
            .send(new Initialized())
            .onFailure(
                t -> {
                  synchronized (outbox) {
                    live = false;
                    initializedPending = true;
                  }
                });
      }
      if (!replay.isEmpty()) {
        LOG.infof("ci-daemon replaying %d frame(s) held through the outage", replay.size());
      }
      replay.forEach(e -> transmit(e.getKey(), e.getValue()));
      if (terminal != null && !terminalInFlight) {
        transmitTerminal();
      }
    }
  }

  /**
   * For {@code InitFailed}, the one terminal frame left: the telemetry flush, then the frame, then
   * the close, then the exit — each waiting on the last, so the frame cannot be lost to a process
   * that exited while the write was still queued, and the telemetry cannot be lost to a lease the
   * host ends on the frame's arrival. A write
   * that fails keeps the frame for the reconnect; only the reconnect budget running out gives up on
   * it.
   *
   * <p>Composed, not awaited: this runs on a worker, the flush's requests and the frame's write run
   * on event loops, and the flush's bound is a Vert.x timer — so no thread parks, and the frame goes
   * out on whichever thread completes the flush. The frame is not the terminal one until then, which
   * is what keeps a re-Ack arriving mid-flush ({@link #release()}) from sending it early.
   */
  private void sendAndFinish(CiDaemonMessage message, int code, String rootError) {
    telemetry
        .flush(rootError)
        .onComplete(
            flushed -> {
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
            });
  }

  /** Why a finished step is an ERROR span, or null when it is not. */
  private String failure(StepFinished finished) {
    if (finished.timedOut()) {
      return "the script timed out (exit " + finished.exitCode() + ")";
    }
    if (finished.exitCode() != 0) {
      return "the script exited "
          + finished.exitCode()
          + (cancelRequested ? " (cancelled)" : "");
    }
    return null;
  }

  private static void endSpan(Spans.Span span, String error) {
    if (span != null) {
      span.end(error);
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
                // Already flushed, before the frame: the flush below returns that same outcome.
                closeAndFinish(code, null);
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

  /** An ending with nothing more to send: flush (bounded, once), close, exit. */
  private void closeAndFinish(int code, String rootError) {
    telemetry
        .flush(rootError)
        .onComplete(flushed -> socket.close().onComplete(v -> finish(code, null)));
  }

  /** An ending whose socket is already gone: flush (bounded, once), exit. */
  private void flushAndFinish(int code, String why, String rootError) {
    telemetry.flush(rootError).onComplete(flushed -> finish(code, why));
  }

  /**
   * The exit, announced by two lines: what became of the telemetry, then the code. The first is
   * the last thing a runner's {@code docker logs} tail of a reaped container can show about it.
   */
  private void finish(int code, String why) {
    // Under one lock, so two endings racing each other (a socket that drops while the terminal
    // frame's close completes) print one pair of lines, adjacent, rather than interleaved ones.
    synchronized (exit) {
      if (exit.isDone()) {
        return;
      }
      LOG.info(telemetry.outcome());
      if (why != null) {
        LOG.warnf("ci-daemon exiting %d: %s", code, why);
      } else {
        LOG.infof("ci-daemon exiting %d", code);
      }
      exit.complete(code);
    }
  }
}
