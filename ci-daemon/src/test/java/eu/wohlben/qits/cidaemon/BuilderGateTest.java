package eu.wohlben.qits.cidaemon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.cidaemon.protocol.CiDaemonMessage;
import eu.wohlben.qits.cidaemon.protocol.RunStep;
import eu.wohlben.qits.cidaemon.protocol.StepChunk;
import eu.wohlben.qits.cidaemon.protocol.StepFinished;
import eu.wohlben.qits.cidaemon.protocol.Stream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/**
 * The {@code buildctl} shim (qits-1807), run for real: written out of the module's resource by
 * {@link BuilderGate#writeShim}, executed by {@code sh} — dash here, the ci-slot image's {@code
 * /bin/sh}, which is what keeps it honest about bashisms — with {@code flock} from util-linux, in
 * front of a fake "real" buildctl that records when it ran.
 */
@EnabledOnOs(OS.LINUX)
class BuilderGateTest {

  private static final String BUILDER = "tcp://qits-system-runner-buildkitd-test:1234";

  @TempDir Path dir;

  private Path shim;
  private Path realBuildctl;
  private Path log;
  private Path gateDir;
  private final List<Process> holders = new ArrayList<>();

  @BeforeEach
  void writeShimAndFakeBuildctl() throws IOException {
    shim = BuilderGate.writeShim(dir.resolve("bin"));
    log = dir.resolve("buildctl.log");
    gateDir = dir.resolve("cache").resolve(".qits-builder-gate");
    Files.createDirectories(gateDir.getParent());
    realBuildctl = dir.resolve("real").resolve("buildctl");
    Files.createDirectories(realBuildctl.getParent());
    Files.writeString(
        realBuildctl,
        "#!/bin/sh\n"
            + "echo \"start $(date +%s%N) $*\" >> '"
            + log
            + "'\n"
            + "sleep 1\n"
            + "echo \"end $(date +%s%N) $*\" >> '"
            + log
            + "'\n");
    Files.setPosixFilePermissions(realBuildctl, PosixFilePermissions.fromString("rwxr-xr-x"));
  }

  @AfterEach
  void releaseHeldTickets() {
    holders.forEach(Process::destroyForcibly);
  }

  @Test
  void twoBuildsOnAOneSlotBuilderRunOneAfterTheOtherAndTheSecondSaysItIsWaiting() throws Exception {
    Path clockA = Files.createDirectories(dir.resolve("clock-a"));
    Path clockB = Files.createDirectories(dir.resolve("clock-b"));
    Process first = shim(clockA, Map.of(), "build", "--frontend", "dockerfile.v0", "first");
    awaitLog("start");
    Process second = shim(clockB, Map.of(), "build", "second");

    // The second build's marker is there while it waits — what stops its step's clock.
    boolean markerSeen = false;
    for (int i = 0; i < 100 && !markerSeen; i++) {
      try (var entries = Files.list(clockB)) {
        markerSeen = entries.anyMatch(path -> path.getFileName().toString().startsWith("wait-"));
      }
      Thread.sleep(20);
    }

    assertEquals(0, finish(first));
    assertEquals(0, finish(second));
    assertTrue(markerSeen, "the waiting build left no marker for its step's clock");
    assertEquals("", stderr(first));
    assertTrue(
        stderr(second).contains("[builder] waiting for " + BUILDER + " (1 builds ahead)"),
        stderr(second));
    try (var entries = Files.list(clockB)) {
      assertEquals(0, entries.count(), "the marker is removed once the build holds a place");
    }
    List<String> lines = Files.readAllLines(log);
    assertEquals(4, lines.size(), lines::toString);
    assertTrue(
        lines.get(0).startsWith("start ") && lines.get(0).endsWith(" first"), lines::toString);
    assertTrue(lines.get(1).startsWith("end ") && lines.get(1).endsWith(" first"), lines::toString);
    assertTrue(
        lines.get(2).endsWith(" second") && lines.get(3).endsWith(" second"), lines::toString);
    long firstEnd = Long.parseLong(lines.get(1).split(" ")[1]);
    long secondStart = Long.parseLong(lines.get(2).split(" ")[1]);
    assertTrue(firstEnd <= secondStart, "the builds overlapped: " + lines);
    assertEquals(
        "start 0 build --frontend dockerfile.v0 first",
        lines.get(0).replaceFirst("start [0-9]+", "start 0"),
        "the real buildctl gets the argv unchanged");
  }

  @Test
  void aTicketWhoseHolderIsGoneIsSweptAndDoesNotHoldTheQueue() throws Exception {
    Path queue = Files.createDirectories(gateDir.resolve("q"));
    Path dead = queue.resolve("00000000000000000001-gone-1");
    Files.writeString(dead, ""); // nobody holds its lock: a build whose container died

    long startedAt = System.nanoTime();
    Process build = shim(null, Map.of(), "build", "x");
    int code = finish(build);
    long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000L;

    assertEquals(0, code, stderr(build));
    assertEquals("", stderr(build), "a build that never waited says nothing");
    assertFalse(Files.exists(dead), "the dead ticket was swept");
    assertTrue(elapsedMillis < 5_000, () -> "took " + elapsedMillis + "ms behind a dead ticket");
  }

  @Test
  void aBuildThatWaitsPastItsBoundExits75NamingTheBuilder() throws Exception {
    holdTicket("00000000000000000001-holder-1");
    Path clock = Files.createDirectories(dir.resolve("clock"));

    Process build = shim(clock, Map.of("QITS_CI_BUILDER_WAIT_MAX_SECONDS", "2"), "build", "x");
    int code = finish(build);

    assertEquals(75, code, stderr(build));
    assertTrue(
        stderr(build)
            .contains(
                "[builder] " + BUILDER + " did not start this build within 2s (1 builds ahead)"),
        stderr(build));
    assertFalse(Files.exists(log), "the real buildctl never ran");
    try (var entries = Files.list(clock)) {
      assertEquals(0, entries.count(), "the marker goes with the build that gave up");
    }
    try (var entries = Files.list(gateDir.resolve("q"))) {
      assertEquals(1, entries.count(), "only the holder's ticket is left");
    }
  }

  @Test
  void aCommandThatIsNotABuildRunsAtOnceEvenWhileTheGateIsFull() throws Exception {
    holdTicket("00000000000000000001-holder-1");

    long startedAt = System.nanoTime();
    Process du = shim(null, Map.of(), "--addr", BUILDER, "du");
    int code = finish(du);
    long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000L;

    assertEquals(0, code);
    assertEquals("", stderr(du));
    assertTrue(Files.readString(log).contains(" --addr " + BUILDER + " du"));
    assertTrue(elapsedMillis < 4_000, () -> "du waited " + elapsedMillis + "ms");
  }

  @Test
  void aGateWhoseDirectoryCannotBeMadeBuildsUngatedAndSaysSo() throws Exception {
    Process build =
        shim(
            null,
            Map.of("QITS_CI_BUILDER_GATE_DIR", dir.resolve("no-such-parent/gate").toString()),
            "build",
            "x");

    assertEquals(0, finish(build));
    assertTrue(stderr(build).contains("[builder] gate unavailable at "), stderr(build));
    assertTrue(Files.readString(log).contains("build x"));
  }

  @Test
  void installResolvesTheRealBuildctlPastTheShimsOwnDirectory() {
    Path bin = shim.getParent();
    String path = bin + ":" + realBuildctl.getParent() + ":/usr/bin";

    BuilderGate gate = BuilderGate.install(bin, dir.resolve("clock"), path, 3600);

    assertTrue(gate.shimInstalled());
    assertEquals(Optional.of(realBuildctl.toString()), gate.realBuildctl());
    Map<String, String> added = gate.stepEnv(Map.of("PATH", "/usr/bin"), Optional.empty());
    assertEquals(bin + ":/usr/bin", added.get("PATH"));
    assertEquals(realBuildctl.toString(), added.get("QITS_CI_REAL_BUILDCTL"));
  }

  @Test
  void anImageWithoutBuildctlGetsNoShimAndItsStepsKeepTheirPath() {
    Path bin = dir.resolve("other-bin");

    BuilderGate gate = BuilderGate.install(bin, dir.resolve("clock"), "/nonexistent:" + dir, 3600);

    assertFalse(gate.shimInstalled());
    assertFalse(Files.exists(bin.resolve("buildctl")));
    Map<String, String> added = gate.stepEnv(Map.of("PATH", "/usr/bin"), Optional.empty());
    assertFalse(added.containsKey("PATH"));
    assertFalse(added.containsKey("QITS_CI_REAL_BUILDCTL"));
  }

  @Test
  void aStepWhoseBuildWaitsForTheBuilderIsNotTimedOutForTheWait() throws Exception {
    // End to end: the step calls `buildctl build` through PATH, the shim queues behind a ticket
    // held for 4s, and the step's 3s deadline only counts the build itself.
    Process holder = holdTicket("00000000000000000001-holder-1");
    Thread releaser =
        new Thread(
            () -> {
              try {
                Thread.sleep(4_000);
              } catch (InterruptedException e) {
                return;
              }
              holder.destroyForcibly();
            });
    releaser.start();
    Path work = Files.createDirectories(dir.resolve("work"));
    BuilderGate gate =
        new BuilderGate(shim.getParent(), realBuildctl.toString(), dir.resolve("clock"), 60);
    List<CiDaemonMessage> emitted = Collections.synchronizedList(new ArrayList<>());
    RunStep request =
        new RunStep(
            "c1",
            0,
            "buildctl build the-image",
            3,
            Map.of("BUILDKIT_HOST", BUILDER, "QITS_CI_BUILDER_GATE_DIR", gateDir.toString()));

    StepFinished finished =
        new StepProcess(
                work,
                request,
                emitted::add,
                new AtomicLong(),
                8192,
                100,
                2000,
                "bash",
                true,
                gate,
                100)
            .run();
    releaser.join();

    String err;
    synchronized (emitted) {
      err =
          emitted.stream()
              .filter(StepChunk.class::isInstance)
              .map(StepChunk.class::cast)
              .filter(chunk -> chunk.stream() == Stream.ERR)
              .map(StepChunk::text)
              .collect(Collectors.joining());
    }
    assertFalse(finished.timedOut(), err);
    assertEquals(0, finished.exitCode(), err);
    assertTrue(err.contains("[builder] waiting for " + BUILDER + " (1 builds ahead)"), err);
    assertTrue(err.contains("[step clock paused: waiting for builder " + BUILDER + " since "), err);
    assertTrue(err.contains("[step clock resumed: waited "), err);
    assertTrue(Files.readString(log).contains("build the-image"));
  }

  // --- helpers
  // ------------------------------------------------------------------------------------

  private Process shim(Path clockDir, Map<String, String> extra, String... args)
      throws IOException {
    List<String> argv = new ArrayList<>(List.of("sh", shim.toString()));
    argv.addAll(List.of(args));
    ProcessBuilder builder = new ProcessBuilder(argv);
    Map<String, String> env = builder.environment();
    env.remove("QITS_CI_STEP_CLOCK_DIR");
    env.remove("QITS_CI_BUILDER_WAIT_MAX_SECONDS");
    env.remove("QITS_CI_BUILDER_SLOTS");
    env.put("QITS_CI_REAL_BUILDCTL", realBuildctl.toString());
    env.put("QITS_CI_BUILDER_GATE_DIR", gateDir.toString());
    env.put("BUILDKIT_HOST", BUILDER);
    if (clockDir != null) {
      env.put("QITS_CI_STEP_CLOCK_DIR", clockDir.toString());
    }
    env.putAll(new HashMap<>(extra));
    builder.redirectOutput(ProcessBuilder.Redirect.DISCARD);
    builder.redirectError(dir.resolve("stderr-" + System.nanoTime()).toFile());
    Process process = builder.start();
    stderrFiles.put(process, builder.redirectError().file().toPath());
    return process;
  }

  private final Map<Process, Path> stderrFiles = new HashMap<>();

  private String stderr(Process process) throws IOException {
    return Files.readString(stderrFiles.get(process));
  }

  private static int finish(Process process) throws InterruptedException {
    assertTrue(process.waitFor(30, TimeUnit.SECONDS), "the shim never finished");
    return process.exitValue();
  }

  /**
   * A queued ticket whose lock is held by a live process, as a running build's is. The holder execs
   * {@code sleep} with fd 9 open, so killing it is what frees the lock.
   */
  private Process holdTicket(String name) throws Exception {
    Path ticket = Files.createDirectories(gateDir.resolve("q")).resolve(name);
    Process holder =
        new ProcessBuilder(
                "sh",
                "-c",
                "exec 9>\"$1\"; flock -x 9; echo locked; exec sleep 60",
                "sh",
                ticket.toString())
            .redirectErrorStream(true)
            .start();
    holders.add(holder);
    BufferedReader out =
        new BufferedReader(new InputStreamReader(holder.getInputStream(), StandardCharsets.UTF_8));
    assertEquals("locked", out.readLine());
    return holder;
  }

  private void awaitLog(String word) throws Exception {
    for (int i = 0; i < 250; i++) {
      if (Files.exists(log) && Files.readString(log).contains(word)) {
        return;
      }
      Thread.sleep(20);
    }
    throw new AssertionError("the real buildctl never logged " + word);
  }
}
