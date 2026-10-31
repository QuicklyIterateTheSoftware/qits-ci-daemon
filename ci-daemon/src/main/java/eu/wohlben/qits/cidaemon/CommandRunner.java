package eu.wohlben.qits.cidaemon;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A one-shot command invocation seam: runs an argv to completion and returns its exit code plus its
 * combined stdout+stderr. {@link MirrorWorktree} is the only caller; the seam exists so its failure
 * mapping — which is load-bearing, since {@code SHA_GONE} drives the host's commit-gone discard —
 * can be tested without arranging every failure against a real git.
 *
 * <p><b>Combined</b> streams on purpose, unlike {@link StepProcess} which keeps them apart. Git
 * writes its diagnostics to stderr and its progress to stderr as well, so a clone failure whose
 * detail came from stdout alone would report nothing at all. Nobody parses this text: it is the
 * bounded {@code detail} a human reads.
 *
 * <p>A spawn failure — no such binary — surfaces as a nonzero {@link Result} rather than an
 * exception, which is what lets a {@code git --version} probe answer "this image does not satisfy
 * the contract" without a second code path.
 */
@FunctionalInterface
public interface CommandRunner {

  /** The outcome of one invocation: its process exit code and its combined output. */
  record Result(int exitCode, String output) {
    public boolean ok() {
      return exitCode == 0;
    }
  }

  /** Run {@code argv} in {@code dir} ({@code null} = inherit the daemon's working directory). */
  Result run(File dir, String... argv);

  /**
   * As {@link #run(File, String...)}, with {@code env} laid over the daemon's environment for this
   * child alone. What a {@code Continue} needs (qits-732): the git credential helper the bootstrap
   * wrote answers with {@code $QITS_TOKEN} as git's child sees it, and the daemon's own environment
   * still holds the first run's token, which a process cannot rewrite in itself. A runner that does
   * not fork — a test's scripted one — may ignore it; the default does.
   */
  default Result run(File dir, Map<String, String> env, String... argv) {
    return run(dir, argv);
  }

  /**
   * The production runner: forks {@code argv} with stderr merged into stdout, bounded by {@code
   * timeoutSeconds}. Any spawn, timeout or interrupt failure surfaces as {@code exitCode -1} with
   * the reason as its output, so a caller only ever branches on {@link Result#ok()}.
   */
  static CommandRunner forking(long timeoutSeconds) {
    return new CommandRunner() {
      @Override
      public Result run(File dir, String... argv) {
        return run(dir, Map.of(), argv);
      }

      @Override
      public Result run(File dir, Map<String, String> env, String... argv) {
        return fork(timeoutSeconds, dir, env, argv);
      }
    };
  }

  private static Result fork(long timeoutSeconds, File dir, Map<String, String> env, String... argv) {
    Process process;
    try {
      ProcessBuilder builder =
          new ProcessBuilder(argv)
              .directory(dir != null && dir.isDirectory() ? dir : null)
              .redirectErrorStream(true);
      if (env != null && !env.isEmpty()) {
        builder.environment().putAll(env);
      }
      process = builder.start();
    } catch (Exception e) {
      // Missing binary, or a directory that vanished. Not distinguished from a nonzero exit
      // here — the caller's probe order is what turns "cannot spawn git" into TOOLING_MISSING.
      return new Result(-1, String.valueOf(e.getMessage()));
    }
    try {
      byte[] out = process.getInputStream().readAllBytes();
      if (!process.waitFor(timeoutSeconds, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        return new Result(-1, "timed out after " + timeoutSeconds + "s");
      }
      return new Result(process.exitValue(), new String(out, StandardCharsets.UTF_8));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      process.destroyForcibly();
      return new Result(-1, "interrupted");
    } catch (Exception e) {
      return new Result(-1, String.valueOf(e.getMessage()));
    }
  }
}
