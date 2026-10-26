package eu.wohlben.qits.cidaemon;

import eu.wohlben.qits.cidaemon.protocol.CiDaemonMessage;
import eu.wohlben.qits.cidaemon.protocol.RunStep;
import eu.wohlben.qits.cidaemon.protocol.StepChunk;
import eu.wohlben.qits.cidaemon.protocol.StepFinished;
import eu.wohlben.qits.cidaemon.protocol.Stream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * The step's script, running as this daemon's child. {@code <shell> -c <script>} with the checkout as
 * its working directory — and <b>the script is one argv element</b>, never spliced into a command
 * string with anything around it. Inside this container that is the designed execution of hostile
 * code: it arrives from a repository, over the socket, and the daemon runs it on purpose. What
 * makes that safe is the sandbox the host built around the container, not anything this class does
 * to the text, and the one thing this class must not do is give the script a second parse.
 *
 * <p>The child inherits the daemon's environment — the lease's — which is how a step sees {@code
 * CI=true}, {@code QITS_CI=true} and the repository coordinates, and the {@link RunStep}'s own
 * {@code env} is laid over it for this child alone: a step's variable replaces the lease's value of
 * the same name and is gone when the step is. {@code QITS_TOKEN} is inherited on purpose
 * rather than tolerated: it is the run's own credential, and the clone and the step's maven, npm
 * and registry calls are exactly what it was commissioned to open.
 *
 * <p><b>Chunking, and why it is not per line.</b> stdout and stderr are pumped as separate streams —
 * a step whose real output is on stderr must not be indistinguishable from one that failed silently
 * — and each pump flushes when its buffer contains a newline, when it reaches {@link #maxChunkChars}
 * characters, or when the {@link #flushIntervalMillis} timer fires, whichever comes first. The
 * newline rule applies to <em>what was read</em>, not to each line: a chatty step fills the pipe, one
 * read returns hundreds of lines, and they leave as one frame. A step printing slowly gets its line
 * out immediately. Neither can produce a frame per byte, which is the chunk-flood the host's relay
 * would otherwise have to absorb.
 *
 * <p>{@code seq} is one counter across both streams <em>and every step of the lease</em> — it is
 * handed in, not started at zero here — allocated and handed to the consumer under the same lock.
 * One counter for the lease is what keeps "drop {@code seq <= lastSeq}" a correct dedup on the
 * host when a later step's output is replayed after an outage. Two pumps racing to allocate and then emitting out of order would show the host a gap —
 * the exact thing the counter exists to detect — so the allocation and the emission are one atomic
 * step rather than two.
 */
public final class StepProcess implements Step {

  private final Path workDir;
  private final RunStep request;
  private final Consumer<CiDaemonMessage> emit;
  private final int maxChunkChars;
  private final long flushIntervalMillis;
  private final long killGraceMillis;

  /**
   * The shell to run the script under, decided by {@code MirrorWorktree.probeTooling()} and passed in
   * rather than probed again here — the image contract is initialization's subject, and a second
   * probe could disagree with the one the run was admitted on.
   */
  private final String shell;

  /** Guards both pump buffers and the seq allocation; see the class javadoc on ordering. */
  private final Object lock = new Object();

  private final AtomicLong seq;

  /**
   * Whether the script runs under {@code setsid}, as the leader of a process group of its own —
   * decided at initialization by whether the image has {@code setsid}. With it, an ending signals
   * the whole group rather than the shell alone.
   */
  private final boolean processGroup;

  private volatile Process process;
  private volatile boolean cancelled;

  public StepProcess(
      Path workDir,
      RunStep request,
      Consumer<CiDaemonMessage> emit,
      AtomicLong seq,
      int maxChunkChars,
      long flushIntervalMillis,
      long killGraceMillis,
      String shell,
      boolean processGroup) {
    this.shell = shell == null || shell.isBlank() ? "sh" : shell;
    this.seq = seq;
    this.processGroup = processGroup;
    this.workDir = workDir;
    this.request = request;
    this.emit = emit;
    this.maxChunkChars = maxChunkChars;
    this.flushIntervalMillis = flushIntervalMillis;
    this.killGraceMillis = killGraceMillis;
  }

  @Override
  public StepFinished run() {
    List<String> argv = new ArrayList<>();
    if (processGroup) {
      // `setsid -w`: the shell becomes the leader of a new session and process group whose id is
      // its pid — a JVM child is never a group leader, so setsid execs in place rather than
      // forking — and -w makes it wait and pass the exit status on should it ever have to fork.
      argv.add("setsid");
      argv.add("-w");
    }
    // The script is the one argv element after `-c` and is never concatenated with anything. The
    // shell was chosen during initialization (MirrorWorktree.probeTooling) — bash when the image
    // has it, sh otherwise.
    argv.add(shell);
    argv.add("-c");
    argv.add(request.script() == null ? "" : request.script());
    Process started;
    try {
      ProcessBuilder builder = new ProcessBuilder(argv);
      builder.directory(workDir.toFile());
      // The step's env over the lease's, for this child only. A key the OS cannot hold (a '=' or a
      // NUL in it) is refused here as IllegalArgumentException, which ends as a failed spawn.
      builder.environment().putAll(request.env());
      started = builder.start();
    } catch (IOException | IllegalArgumentException e) {
      // A failed spawn still has to produce a terminal frame, or the host's await runs to timeout
      // and records a stalled step instead of a step that could not start. 127 is the shell's own
      // "command not found", which is what this is.
      emitChunk(Stream.ERR, "ci-daemon could not start " + shell + ": " + e.getMessage() + "\n");
      return new StepFinished(request.correlationId(), request.stepIndex(), 127, false);
    }
    process = started;

    Pump out = new Pump(started.getInputStream(), Stream.OUT);
    Pump err = new Pump(started.getErrorStream(), Stream.ERR);
    Thread outThread = pumpThread(out, "ci-daemon-step-out");
    Thread errThread = pumpThread(err, "ci-daemon-step-err");
    ScheduledExecutorService flusher =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "ci-daemon-step-flush");
              thread.setDaemon(true);
              return thread;
            });
    flusher.scheduleWithFixedDelay(
        () -> {
          out.flushPending();
          err.flushPending();
        },
        flushIntervalMillis,
        flushIntervalMillis,
        TimeUnit.MILLISECONDS);

    boolean timedOut = false;
    try {
      if (request.timeoutSeconds() > 0) {
        if (!started.waitFor(request.timeoutSeconds(), TimeUnit.SECONDS)) {
          // The deadline is enforced HERE, by the process's parent, so a timeout is a recorded
          // outcome rather than a container the host reaps for going quiet. The host keeps a longer
          // deadline behind this one for a daemon that stops answering at all.
          timedOut = !cancelled;
          terminate();
          started.waitFor();
        }
      } else {
        started.waitFor();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      terminate();
    }

    // Whatever the script left running in its group goes with it. A container per step used to
    // take such leftovers down with it; in a lease the next step would inherit them, and one still
    // holding the pipes open would keep the pumps below from ever reaching EOF.
    signalGroup(started, "KILL");

    // Drain before reporting: the pumps end at EOF and flush what is left, so once they are joined
    // there is no chunk still in flight that could arrive after the terminal frame. Bounded, for a
    // descendant that left the group (a setsid of its own) and still holds a pipe.
    join(outThread, killGraceMillis);
    join(errThread, killGraceMillis);
    flusher.shutdownNow();
    out.flushPending();
    err.flushPending();

    int exitCode;
    try {
      exitCode = started.exitValue();
    } catch (IllegalThreadStateException e) {
      exitCode = -1; // unreachable in practice: every path above waits for the child
    }
    // timedOut stays false for a cancellation. Both end the same way — SIGTERM, grace, SIGKILL — but
    // they are different outcomes to the person reading the run, and the exit code cannot tell them
    // apart because it is the kill's either way.
    return new StepFinished(request.correlationId(), request.stepIndex(), exitCode, timedOut);
  }

  @Override
  public void cancel() {
    cancelled = true;
    terminate();
  }

  /**
   * SIGTERM, then {@link #killGraceMillis}, then SIGKILL — to the step's whole process group when it
   * has one, so a {@code Cancel} or a timeout takes the build's background children down too.
   * Synchronized because a timeout and a {@code Cancel} can land together, and because it is called
   * from a thread other than the one inside {@link #run()}.
   *
   * <p>The group is the step's own (see {@link #run()}), never the daemon's: the daemon and its
   * pumps are in the group the container started, so signalling {@code -pid} cannot reach them.
   * Without {@code setsid} only the shell is signalled, which is what a per-step container got away
   * with and a lease no longer can.
   */
  private synchronized void terminate() {
    Process running = process;
    if (running == null || !running.isAlive()) {
      return;
    }
    signalGroup(running, "TERM");
    running.destroy();
    try {
      if (!running.waitFor(killGraceMillis, TimeUnit.MILLISECONDS)) {
        signalGroup(running, "KILL");
        running.destroyForcibly();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      signalGroup(running, "KILL");
      running.destroyForcibly();
    }
  }

  /**
   * {@code kill -s <signal> -- -<pgid>}, best effort: the JDK can signal a process but not a group,
   * and {@code kill} is the one portable way to say "the group" — one {@code kill(2)} on {@code
   * -pgid}, atomic against a group that is still forking, which walking {@code /proc} is not.
   *
   * <p><b>The step's shell's builtin {@code kill}, never a {@code kill} binary on PATH.</b> The
   * binary is not part of the image contract: on Alpine busybox supplies it, but on Debian it is
   * procps', and the ci-slot image (bookworm-slim) has no procps. Exec'ing {@code kill} there failed
   * with an IOException this method swallowed, so no group was ever signalled: a step's background
   * child outlived the step and held its pipes open until the pumps' bound (qits-1541). The shell is
   * already the image's contract — the step itself runs under it — and every shell the probe can
   * choose (bash, dash, busybox ash) has the builtin. The signal and the pgid are positional
   * parameters, not text spliced into the script, and both are values this class produced.
   *
   * <p>A group already gone answers nonzero, which is the common case after a clean exit and means
   * nothing.
   */
  private void signalGroup(Process leader, String signal) {
    if (!processGroup) {
      return;
    }
    try {
      Process kill =
          new ProcessBuilder(
                  shell,
                  "-c",
                  "kill -s \"$1\" -- \"-$2\"",
                  "kill",
                  signal,
                  String.valueOf(leader.pid()))
              .redirectErrorStream(true)
              .redirectOutput(ProcessBuilder.Redirect.DISCARD)
              .start();
      kill.waitFor(5, TimeUnit.SECONDS);
    } catch (IOException e) {
      // No shell to run the builtin in: the direct signal to the shell in terminate() is what is
      // left.
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private Thread pumpThread(Pump pump, String name) {
    Thread thread = new Thread(pump::pump, name + "-" + request.correlationId());
    thread.setDaemon(true);
    thread.start();
    return thread;
  }

  private static void join(Thread thread, long boundMillis) {
    try {
      thread.join(Math.max(boundMillis, 1_000));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void emitChunk(Stream channel, String text) {
    synchronized (lock) {
      emit.accept(new StepChunk(request.correlationId(), seq.getAndIncrement(), channel, text));
    }
  }

  /** One stream's reader and its pending buffer. */
  private final class Pump {

    private final InputStream stream;
    private final Stream channel;
    private final StringBuilder pending = new StringBuilder();

    private Pump(InputStream stream, Stream channel) {
      this.stream = stream;
      this.channel = channel;
    }

    /** Read to EOF, flushing by the rules in the class javadoc. Never throws. */
    private void pump() {
      char[] buffer = new char[maxChunkChars];
      // A Reader, not raw bytes: a multi-byte character split across two reads must not become two
      // mangled ones, and a step's output is text the host shows to a human.
      try (Reader reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
        int read;
        while ((read = reader.read(buffer)) != -1) {
          if (read > 0) {
            append(buffer, read);
          }
        }
      } catch (IOException e) {
        // The stream closed under us because the child died; the exit code carries the outcome.
      } finally {
        flushPending();
      }
    }

    private void append(char[] buffer, int length) {
      synchronized (lock) {
        pending.append(buffer, 0, length);
        int lastNewline = pending.lastIndexOf("\n");
        if (lastNewline >= 0) {
          // Flush through the last complete line and keep the partial tail, so a chunk boundary
          // falls where a reader would expect one. The tail leaves on the timer or the next read.
          flushLocked(lastNewline + 1);
        } else if (pending.length() >= maxChunkChars) {
          flushLocked(pending.length());
        }
      }
    }

    /** Flush whatever is buffered — the timer's job, and the tail at EOF. */
    private void flushPending() {
      synchronized (lock) {
        flushLocked(pending.length());
      }
    }

    private void flushLocked(int upTo) {
      if (upTo <= 0) {
        return;
      }
      String text = pending.substring(0, upTo);
      pending.delete(0, upTo);
      emit.accept(new StepChunk(request.correlationId(), seq.getAndIncrement(), channel, text));
    }
  }
}
