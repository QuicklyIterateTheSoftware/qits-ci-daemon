package eu.wohlben.qits.cidaemon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.cidaemon.protocol.CiDaemonMessage;
import eu.wohlben.qits.cidaemon.protocol.RunStep;
import eu.wohlben.qits.cidaemon.protocol.StepChunk;
import eu.wohlben.qits.cidaemon.protocol.StepFinished;
import eu.wohlben.qits.cidaemon.protocol.Stream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/**
 * Drives a real {@code bash}, because a step process is not a thing worth simulating: what these
 * tests are about is exactly what a real child does — the streams it writes on, when its output
 * arrives, and what happens when it is killed.
 *
 * <p>Steps get pipes, never terminals. Nothing here allocates a PTY and nothing should: the
 * workspace daemon needs one because a human types into its commands, and a ci step has no human.
 */
@EnabledOnOs(OS.LINUX)
class StepProcessTest {

  @TempDir Path workDir;

  private final List<CiDaemonMessage> emitted = Collections.synchronizedList(new ArrayList<>());

  /**
   * Which shell {@link #run} builds the step with. Defaults to what every existing image provides,
   * so every case here keeps asserting the behaviour a bash image has always had; the fallback case
   * sets it to {@code sh}.
   */
  private String shell = "bash";

  @Test
  void aStepsStdoutAndStderrArriveAsSeparateStreamsAndItsExitCodeIsReported() {
    StepFinished finished = run("echo to-stdout; echo to-stderr >&2; exit 3", 30);

    assertEquals(3, finished.exitCode());
    assertFalse(finished.timedOut());
    assertEquals("to-stdout\n", textOf(Stream.OUT));
    assertEquals("to-stderr\n", textOf(Stream.ERR));
  }

  @Test
  void aScriptRunsUnderShWhenTheImageHasNoBash() {
    // What docker:28-dind gets: no bash in the image, so Workspace resolves the shell to sh and the
    // step runs anyway. A POSIX script behaves identically under either, which is what makes the
    // fallback safe rather than a second dialect nobody tested.
    shell = "sh";

    StepFinished finished = run("echo to-stdout; echo to-stderr >&2; exit 3", 30);

    assertEquals(3, finished.exitCode());
    assertFalse(finished.timedOut());
    assertEquals("to-stdout\n", textOf(Stream.OUT));
    assertEquals("to-stderr\n", textOf(Stream.ERR));
  }

  @Test
  void theScriptIsOneArgvElementSoNothingGivesItASecondParse() {
    // Quotes, semicolons, a subshell and a newline: if any layer between here and bash re-parsed
    // or re-quoted this, the output would not come back byte for byte. This is the property the
    // whole hostile-code stance rests on — the script is bash's to interpret and nobody else's.
    String script = "printf '%s\\n' 'a; b \"c\" $(echo d) `echo e`'\nprintf '%s\\n' \"second line\"";

    StepFinished finished = run(script, 30);

    assertEquals(0, finished.exitCode());
    assertEquals("a; b \"c\" $(echo d) `echo e`\nsecond line\n", textOf(Stream.OUT));
  }

  @Test
  void aChattyStepIsChunkedByReadRatherThanFramedPerLine() {
    // 20000 lines of about five bytes each. The flush-on-newline rule applies to what was READ, not
    // to each line, so a step that floods the pipe fills an 8KiB read with hundreds of lines and
    // they leave as one frame. At worst-case efficiency this would still be well under 2000 frames;
    // a per-line implementation would produce 20000 and starve the host's relay.
    StepFinished finished = run("seq 1 20000", 60);

    assertEquals(0, finished.exitCode());
    List<StepChunk> chunks = chunks(Stream.OUT);
    assertTrue(
        chunks.size() < 2000,
        () -> "expected chunking by read, got " + chunks.size() + " frames for 20000 lines");
    StringBuilder reassembled = new StringBuilder();
    chunks.forEach(chunk -> reassembled.append(chunk.text()));
    assertEquals(20000, reassembled.toString().lines().count());
  }

  @Test
  void aTrailingLineWithNoNewlineStillReachesTheHost() {
    // Nothing flushes it on a newline and it never reaches the size cap, so only the interval timer
    // or EOF can — and a step whose last word never arrived would be a quietly truncated log.
    StepFinished finished = run("printf 'no trailing newline'", 30);

    assertEquals(0, finished.exitCode());
    assertEquals("no trailing newline", textOf(Stream.OUT));
  }

  @Test
  void chunkSequenceNumbersAreMonotonicAcrossBothStreamsTogether() {
    run("for i in $(seq 1 200); do echo out-$i; echo err-$i >&2; done", 60);

    List<Long> sequence = new ArrayList<>();
    synchronized (emitted) {
      emitted.stream().filter(StepChunk.class::isInstance).map(StepChunk.class::cast)
          .forEach(chunk -> sequence.add(chunk.seq()));
    }
    // One counter for the correlation, not one per stream: the host asserts order and detects gaps
    // on this, and two independent counters would look like a permanent gap in both.
    List<Long> expected = new ArrayList<>();
    for (long i = 0; i < sequence.size(); i++) {
      expected.add(i);
    }
    assertEquals(expected, sequence);
  }

  @Test
  void aStepKilledAtItsDeadlineReportsTimedOutRatherThanItsExitCode() {
    long startedAt = System.nanoTime();
    StepFinished finished = run("echo starting; sleep 60", 1);
    long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000L;

    assertTrue(finished.timedOut(), "the deadline is enforced here, by the child's own parent");
    // The exit code is the kill's, not the script's — indistinguishable from a script that trapped
    // a signal and exited that way, which is exactly why timedOut is a field and not an inference.
    assertNotEquals(0, finished.exitCode());
    assertTrue(elapsedMillis < 30_000, () -> "took " + elapsedMillis + "ms to enforce a 1s deadline");
    assertEquals("starting\n", textOf(Stream.OUT));
  }

  @Test
  void aStepWithNoDeadlineRunsToCompletion() {
    StepFinished finished = run("echo done", 0);

    assertFalse(finished.timedOut());
    assertEquals(0, finished.exitCode());
  }

  @Test
  void aCancelledStepFinishesWithoutBeingReportedAsATimeout() throws Exception {
    Path marker = workDir.resolve("running");
    StepProcess process =
        new StepProcess(
            workDir,
            new RunStep("c1", 0, "touch running; sleep 60", 300, Map.of()),
            emitted::add,
            new AtomicLong(),
            8192,
            100,
            5000,
            "bash",
            true);
    CompletableFuture<StepFinished> finished =
        CompletableFuture.supplyAsync(process::run);

    for (int i = 0; i < 200 && !Files.exists(marker); i++) {
      Thread.sleep(25);
    }
    assertTrue(Files.exists(marker), "the step never started");
    process.cancel();

    StepFinished result = finished.get(30, TimeUnit.SECONDS);
    // A cancellation and a timeout end the same way — SIGTERM, grace, SIGKILL — and the exit code
    // cannot tell them apart. They are different outcomes to the person reading the run.
    assertFalse(result.timedOut());
    assertEquals("c1", result.correlationId());
  }

  @Test
  void theStepRunsInTheCheckoutAndInheritsTheDaemonsEnvironment() throws Exception {
    Files.writeString(workDir.resolve("in-the-checkout"), "yes\n");

    StepFinished finished = run("cat in-the-checkout; test -n \"$PATH\" && echo path-inherited", 30);

    assertEquals(0, finished.exitCode());
    // The inherited environment is how a step sees CI=true and QITS_CI=true: the launcher sets them
    // on the container, the daemon is started with them, and the child gets them for free.
    assertEquals("yes\npath-inherited\n", textOf(Stream.OUT));
  }

  @Test
  void aStepThatCannotBeSpawnedStillProducesATerminalFrame() {
    // Not reachable through the real bash path, so it is driven at the seam: what matters is that
    // no failure leaves the host's await hanging to timeout instead of recording an outcome.
    StepProcess process =
        new StepProcess(
            Path.of("/nonexistent-directory-for-this-test"),
            new RunStep("c1", 0, "echo hi", 30, Map.of()),
            emitted::add,
            new AtomicLong(),
            8192,
            100,
            5000,
            "bash",
            true);

    StepFinished finished = process.run();

    assertEquals(127, finished.exitCode());
    assertFalse(finished.timedOut());
    assertTrue(textOf(Stream.ERR).contains("ci-daemon could not start bash"));
  }

  @Test
  void theStepsOwnEnvIsLaidOverTheInheritedOneForThatChildOnly() {
    StepFinished finished =
        run(
            new RunStep(
                "c1",
                4,
                "echo \"$QITS_TEST_STEP_VAR $HOME\"",
                30,
                Map.of("QITS_TEST_STEP_VAR", "from-the-step", "HOME", "/overridden")),
            new AtomicLong());

    assertEquals(0, finished.exitCode());
    assertEquals(4, finished.stepIndex(), "the result names its step");
    assertEquals("from-the-step /overridden\n", textOf(Stream.OUT));
    assertFalse(System.getenv().containsKey("QITS_TEST_STEP_VAR"), "the daemon's own env is untouched");
  }

  @Test
  void seqContinuesFromTheLeasesCounterRatherThanRestartingPerStep() {
    AtomicLong seq = new AtomicLong(41);

    run(new RunStep("c2", 1, "echo one", 30, Map.of()), seq);

    assertEquals(41L, chunks(Stream.OUT).getFirst().seq());
    assertTrue(seq.get() > 41, "the shared counter moved on");
  }

  @Test
  void aCancelKillsTheStepsWholeProcessGroupNotOnlyItsShell() throws Exception {
    Path pidFile = workDir.resolve("child.pid");
    StepProcess process =
        new StepProcess(
            workDir,
            new RunStep("c1", 0, "sleep 300 & echo $! > child.pid; wait", 300, Map.of()),
            emitted::add,
            new AtomicLong(),
            8192,
            100,
            2000,
            "bash",
            true);
    CompletableFuture<StepFinished> finished = CompletableFuture.supplyAsync(process::run);
    for (int i = 0; i < 200 && (!Files.exists(pidFile) || Files.size(pidFile) == 0); i++) {
      Thread.sleep(25);
    }
    long child = Long.parseLong(Files.readString(pidFile).strip());
    assertTrue(running(child));

    process.cancel();
    finished.get(30, TimeUnit.SECONDS);

    // The background child shared the step's group and went with it; in a lease the next step would
    // otherwise inherit it.
    for (int i = 0; i < 100 && running(child); i++) {
      Thread.sleep(50);
    }
    assertFalse(running(child));
  }

  @Test
  void aBackgroundChildLeftRunningByAFinishedStepDoesNotOutliveItOrHoldItsResult()
      throws Exception {
    long startedAt = System.nanoTime();
    StepFinished finished = run("sleep 300 & echo $! > child.pid; exit 0", 30);
    long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000L;

    assertEquals(0, finished.exitCode());
    assertTrue(elapsedMillis < 10_000, () -> "the leftover held the step open: " + elapsedMillis);
    long child = Long.parseLong(Files.readString(workDir.resolve("child.pid")).strip());
    for (int i = 0; i < 100 && running(child); i++) {
      Thread.sleep(50);
    }
    assertFalse(running(child));
  }

  // --- helpers ------------------------------------------------------------------------------------

  private StepFinished run(String script, int timeoutSeconds) {
    return run(new RunStep("c1", 0, script, timeoutSeconds, Map.of()), new AtomicLong());
  }

  /**
   * Running, as opposed to gone or a zombie: an orphan is reparented to a PID 1 that may never reap
   * it, and {@code ProcessHandle.isAlive} reports a zombie as alive.
   */
  private static boolean running(long pid) {
    try {
      String stat = Files.readString(Path.of("/proc", String.valueOf(pid), "stat"));
      return !stat.substring(stat.lastIndexOf(')') + 2).startsWith("Z");
    } catch (java.io.IOException gone) {
      return false;
    }
  }

  private StepFinished run(RunStep request, AtomicLong seq) {
    return new StepProcess(workDir, request, emitted::add, seq, 8192, 100, 5000, shell, true).run();
  }

  private List<StepChunk> chunks(Stream channel) {
    synchronized (emitted) {
      return emitted.stream()
          .filter(StepChunk.class::isInstance)
          .map(StepChunk.class::cast)
          .filter(chunk -> chunk.stream() == channel)
          .toList();
    }
  }

  private String textOf(Stream channel) {
    StringBuilder text = new StringBuilder();
    chunks(channel).forEach(chunk -> text.append(chunk.text()));
    return text.toString();
  }
}
