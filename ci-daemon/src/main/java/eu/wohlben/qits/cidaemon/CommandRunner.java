package eu.wohlben.qits.cidaemon;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * A one-shot command invocation seam: runs an argv to completion and returns its exit code plus its
 * combined stdout+stderr. {@link Workspace} is the only caller; the seam exists so its failure
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
   * The production runner: forks {@code argv} with stderr merged into stdout, bounded by {@code
   * timeoutSeconds}. Any spawn, timeout or interrupt failure surfaces as {@code exitCode -1} with
   * the reason as its output, so a caller only ever branches on {@link Result#ok()}.
   */
  static CommandRunner forking(long timeoutSeconds) {
    return (dir, argv) -> {
      Process process;
      try {
        process =
            new ProcessBuilder(argv)
                .directory(dir != null && dir.isDirectory() ? dir : null)
                .redirectErrorStream(true)
                .start();
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
    };
  }
}
