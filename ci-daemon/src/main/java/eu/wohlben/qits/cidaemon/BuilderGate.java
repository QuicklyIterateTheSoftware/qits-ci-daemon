package eu.wohlben.qits.cidaemon;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.jboss.logging.Logger;

/**
 * The builder gate (qits-1807): a {@code buildctl} shim in front of the real one, and the per-step
 * clock directory its wait marker goes in.
 *
 * <p>Every lease on a target dials the one buildkitd at {@code $BUILDKIT_HOST}. A build buildkitd
 * had not started yet used to wait inside {@code buildctl build}, on the step's clock, so a step on
 * a busy target timed out having built nothing. The shim ({@code qits-buildctl-shim.sh}, a resource
 * of this module) queues a build on a lock file in the target's shared {@code /cache/git} before
 * buildctl dials, and while it waits leaves {@code $QITS_CI_STEP_CLOCK_DIR/wait-<pid>}, which
 * {@link StepProcess} reads as "stop the clock". Nothing about the build itself changes: once it
 * holds a place the shim execs the real buildctl with the same argv.
 *
 * <p>Installed once, at startup, by {@link #install}: resolve the real {@code buildctl} on PATH
 * (never the shim's own directory, or a second daemon start would find its own shim), write the
 * shim beside nothing else in {@link #DEFAULT_BIN_DIR}, and prepend that directory to every step's
 * PATH. An image without buildctl gets no shim and its steps run exactly as before; the clock
 * directory is still handed out, because a marker is the only input the clock reads and an empty
 * directory means "never paused".
 */
public final class BuilderGate {

  private static final Logger LOG = Logger.getLogger(BuilderGate.class);

  /** Where the shim is written. Per container: {@code /tmp} is the lease's own. */
  public static final Path DEFAULT_BIN_DIR = Path.of("/tmp/qits-ci-bin");

  /** Where each step's clock directory is made, as {@code <runId>-<stepIndex>}. */
  public static final Path DEFAULT_CLOCK_ROOT = Path.of("/tmp/qits-step-clock");

  /** The shim's source, a resource named in {@code quarkus.native.resources.includes}. */
  static final String SHIM_RESOURCE = "/eu/wohlben/qits/cidaemon/qits-buildctl-shim.sh";

  static final String CLOCK_DIR = "QITS_CI_STEP_CLOCK_DIR";
  static final String REAL_BUILDCTL = "QITS_CI_REAL_BUILDCTL";
  static final String WAIT_MAX = "QITS_CI_BUILDER_WAIT_MAX_SECONDS";

  /** The same one-segment rule as {@code MirrorWorktree.SEGMENT}; anything else is replaced. */
  private static final Pattern UNSAFE = Pattern.compile("[^A-Za-z0-9._-]");

  private final Path binDir;
  private final String realBuildctl;
  private final Path clockRoot;
  private final long defaultWaitMaxSeconds;

  /**
   * @param binDir the shim's directory, prepended to each step's PATH; {@code null} for no shim
   * @param realBuildctl the buildctl the shim execs; {@code null} for no shim
   * @param clockRoot where the step clock directories go; {@code null} switches the paused clock
   *     off
   * @param defaultWaitMaxSeconds the cap on a step's paused time when its env does not name one
   */
  public BuilderGate(Path binDir, String realBuildctl, Path clockRoot, long defaultWaitMaxSeconds) {
    boolean shim = binDir != null && realBuildctl != null && !realBuildctl.isBlank();
    this.binDir = shim ? binDir : null;
    this.realBuildctl = shim ? realBuildctl : null;
    this.clockRoot = clockRoot;
    this.defaultWaitMaxSeconds = Math.max(0, defaultWaitMaxSeconds);
  }

  /** No shim, no clock directory: a step runs on a plain wall clock, as it did before qits-1807. */
  public static BuilderGate none() {
    return new BuilderGate(null, null, null, 0);
  }

  /**
   * Resolve the real {@code buildctl} on {@code path} and write the shim into {@code binDir}. Never
   * throws: a gate that cannot be installed is a step that builds ungated, which is what every step
   * did before, and is not a reason to refuse the lease.
   */
  public static BuilderGate install(
      Path binDir, Path clockRoot, String path, long defaultWaitMaxSeconds) {
    Optional<Path> real = resolve("buildctl", path, binDir);
    if (real.isEmpty()) {
      return new BuilderGate(null, null, clockRoot, defaultWaitMaxSeconds);
    }
    try {
      writeShim(binDir);
    } catch (IOException | RuntimeException e) {
      LOG.warnf("ci-daemon installs no builder gate: %s", e.getMessage());
      return new BuilderGate(null, null, clockRoot, defaultWaitMaxSeconds);
    }
    LOG.infof("ci-daemon gates %s through %s", real.get(), binDir.resolve("buildctl"));
    return new BuilderGate(binDir, real.get().toString(), clockRoot, defaultWaitMaxSeconds);
  }

  /**
   * The first executable {@code name} on {@code path}, skipping {@code skip} (the shim's own dir).
   */
  static Optional<Path> resolve(String name, String path, Path skip) {
    if (path == null || path.isBlank()) {
      return Optional.empty();
    }
    Path skipped = skip == null ? null : skip.toAbsolutePath().normalize();
    for (String entry : path.split(File.pathSeparator)) {
      if (entry.isEmpty()) {
        continue; // an empty entry is the cwd — the untrusted checkout, never a buildctl to trust
      }
      Path dir;
      try {
        dir = Path.of(entry).toAbsolutePath().normalize();
      } catch (java.nio.file.InvalidPathException e) {
        continue;
      }
      if (dir.equals(skipped)) {
        continue;
      }
      Path candidate = dir.resolve(name);
      if (Files.isRegularFile(candidate) && Files.isExecutable(candidate)) {
        return Optional.of(candidate);
      }
    }
    return Optional.empty();
  }

  /** Written to a temp name and renamed, so a step never execs half a shim. */
  static Path writeShim(Path binDir) throws IOException {
    byte[] source;
    try (InputStream in = BuilderGate.class.getResourceAsStream(SHIM_RESOURCE)) {
      if (in == null) {
        throw new IOException("the shim resource " + SHIM_RESOURCE + " is not in the binary");
      }
      source = in.readAllBytes();
    }
    Files.createDirectories(binDir);
    Path target = binDir.resolve("buildctl");
    Path temp = Files.createTempFile(binDir, ".buildctl-", ".tmp");
    try {
      Files.write(temp, source);
      Files.setPosixFilePermissions(temp, PosixFilePermissions.fromString("rwxr-xr-x"));
      try {
        Files.move(
            temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      Files.deleteIfExists(temp);
    }
    return target;
  }

  boolean shimInstalled() {
    return binDir != null;
  }

  Optional<Path> binDir() {
    return Optional.ofNullable(binDir);
  }

  Optional<String> realBuildctl() {
    return Optional.ofNullable(realBuildctl);
  }

  /**
   * Make this step's clock directory, {@code <clockRoot>/<runId>-<stepIndex>}, emptied first in
   * case an earlier daemon left one. Empty when the clock is off or the directory cannot be made —
   * the step then runs on the wall clock, which is the old behaviour rather than a failure.
   */
  Optional<Path> clockDirFor(String runId, int stepIndex) {
    if (clockRoot == null) {
      return Optional.empty();
    }
    String run = runId == null || runId.isBlank() ? "run" : UNSAFE.matcher(runId).replaceAll("_");
    if (run.startsWith(".")) {
      run = "_" + run.substring(1); // never "..", never a hidden name
    }
    if (run.length() > 128) {
      run = run.substring(0, 128);
    }
    Path dir = clockRoot.resolve(run + "-" + stepIndex);
    try {
      deleteClockDir(dir);
      Files.createDirectories(dir);
      return Optional.of(dir);
    } catch (IOException | RuntimeException e) {
      LOG.warnf("ci-daemon runs step %d on the wall clock: %s", stepIndex, e.getMessage());
      return Optional.empty();
    }
  }

  /** Flat: the shim writes files, never directories, and nothing here follows a link. */
  static void deleteClockDir(Path dir) {
    if (dir == null || !Files.isDirectory(dir, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    try (var entries = Files.list(dir)) {
      entries.forEach(
          entry -> {
            try {
              Files.deleteIfExists(entry);
            } catch (IOException ignored) {
              // a subdirectory the step made on purpose; the next run's cleanup is no worse off
            }
          });
      Files.deleteIfExists(dir);
    } catch (IOException ignored) {
      // best effort: /tmp is the container's and goes with it
    }
  }

  /**
   * The cap on one step's paused time: the step env's {@code QITS_CI_BUILDER_WAIT_MAX_SECONDS} when
   * it is a non-negative number, the daemon's default otherwise.
   */
  long waitMaxSeconds(Map<String, String> stepEnv) {
    String raw = stepEnv.get(WAIT_MAX);
    if (raw != null) {
      try {
        long parsed = Long.parseLong(raw.strip());
        if (parsed >= 0) {
          return parsed;
        }
      } catch (NumberFormatException ignored) {
        // the shim falls back to its own default for the same garbage; so does the clock
      }
    }
    return defaultWaitMaxSeconds;
  }

  /**
   * What the gate adds to a step's environment, given the environment the step would otherwise get
   * (lease env with the step's own laid over it). Set last, so a step cannot point the clock
   * somewhere else or the shim at another binary.
   */
  Map<String, String> stepEnv(Map<String, String> effective, Optional<Path> clockDir) {
    Map<String, String> added = new LinkedHashMap<>();
    clockDir.ifPresent(dir -> added.put(CLOCK_DIR, dir.toString()));
    if (clockRoot != null && !effective.containsKey(WAIT_MAX)) {
      // The shim's bound and the clock's cap are one number: the shim would otherwise default to
      // its own 3600 while the daemon was configured to something else.
      added.put(WAIT_MAX, String.valueOf(defaultWaitMaxSeconds));
    }
    if (binDir != null) {
      added.put(REAL_BUILDCTL, realBuildctl);
      String path = effective.get("PATH");
      added.put(
          "PATH",
          path == null || path.isEmpty() ? binDir.toString() : binDir + File.pathSeparator + path);
    }
    return added;
  }
}
