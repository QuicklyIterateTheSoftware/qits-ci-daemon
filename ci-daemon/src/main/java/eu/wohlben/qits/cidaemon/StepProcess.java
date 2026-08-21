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
 * <p>The child inherits the daemon's environment, which is how a step sees {@code CI=true},
 * {@code QITS_CI=true} and the repository coordinates. It also means the child can read {@code
 * QITS_CI_DAEMON_SECRET}. That is not a leak to plug here: the secret authorizes exactly "deliver
 * data about this run" and the step's own output is already the data it would deliver, so a script
 * that used it would be impersonating itself.
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
 * <p>{@code seq} is one counter across both streams, allocated and handed to the consumer under the
 * same lock. Two pumps racing to allocate and then emitting out of order would show the host a gap —
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
   * The shell to run the script under, decided by {@link Workspace#probeTooling()} and passed in
   * rather than probed again here — the image contract is initialization's subject, and a second
   * probe could disagree with the one the run was admitted on.
   */
  private final String shell;

  /** Guards both pump buffers and the seq allocation; see the class javadoc on ordering. */
  private final Object lock = new Object();

  private final AtomicLong seq = new AtomicLong();

  private volatile Process process;
  private volatile boolean cancelled;

  public StepProcess(
      Path workDir,
      RunStep request,
      Consumer<CiDaemonMessage> emit,
      int maxChunkChars,
      long flushIntervalMillis,
      long killGraceMillis,
      String shell) {
    this.shell = shell == null || shell.isBlank() ? "sh" : shell;
    this.workDir = workDir;
    this.request = request;
    this.emit = emit;
    this.maxChunkChars = maxChunkChars;
    this.flushIntervalMillis = flushIntervalMillis;
    this.killGraceMillis = killGraceMillis;
  }

  @Override
  public StepFinished run() {
    ProcessBuilder builder =
        // Three argv elements, always: the script is the third and is never concatenated with
        // anything. The shell was chosen during initialization (see Workspace.probeTooling) — bash
        // when the image has it, sh otherwise — so reaching here with neither means the image
        // changed under us.
        new ProcessBuilder(shell, "-c", request.script() == null ? "" : request.script());
    builder.directory(workDir.toFile());
    Process started;
    try {
      started = builder.start();
    } catch (IOException e) {
      // A failed spawn still has to produce a terminal frame, or the host's await runs to timeout
      // and records a stalled container instead of a step that could not start. 127 is the shell's
      // own "command not found", which is what this is.
      emitChunk(Stream.ERR, "ci-daemon could not start " + shell + ": " + e.getMessage() + "\n");
      return new StepFinished(request.correlationId(), 127, false);
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

    // Drain before reporting: the pumps end at EOF and flush what is left, so once they are joined
    // there is no chunk still in flight that could arrive after the terminal frame.
    join(outThread);
    join(errThread);
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
    return new StepFinished(request.correlationId(), exitCode, timedOut);
  }

  @Override
  public void cancel() {
    cancelled = true;
    terminate();
  }

  /**
   * SIGTERM, then {@link #killGraceMillis}, then SIGKILL. Synchronized because a timeout and a
   * {@code Cancel} can land together, and because it is called from a thread other than the one
   * inside {@link #run()}.
   *
   * <p>This signals the shell, and a shell that spawned background children does not necessarily
   * take them with it. That is deliberately not chased: the container is torn down behind this, and
   * a process-group kill from inside a sandbox whose whole purpose is to be discarded buys nothing
   * but a way to kill the daemon's own pumps by accident.
   */
  private synchronized void terminate() {
    Process running = process;
    if (running == null || !running.isAlive()) {
      return;
    }
    running.destroy();
    try {
      if (!running.waitFor(killGraceMillis, TimeUnit.MILLISECONDS)) {
        running.destroyForcibly();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      running.destroyForcibly();
    }
  }

  private Thread pumpThread(Pump pump, String name) {
    Thread thread = new Thread(pump::pump, name + "-" + request.correlationId());
    thread.setDaemon(true);
    thread.start();
    return thread;
  }

  private static void join(Thread thread) {
    try {
      thread.join();
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
