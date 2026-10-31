package eu.wohlben.qits.cidaemon.protocol;

import java.util.Map;
import java.util.TreeSet;

/**
 * qits-ci → {@code ci-daemon} (capability 3, qits-732): the lease that ran one run of a release
 * request now runs the request's next run, in the same container and the same worktree. Phase 1
 * runs QA on the fold, phase 2 builds the cut tag; the tag is the fold plus a version commit, so
 * moving the existing worktree keeps every warm {@code target/} and {@code node_modules/} a fresh
 * lease would rebuild from nothing.
 *
 * <p>Sent only between runs — after the current run's last {@link StepFinished}, with no step
 * running — and answered by {@link Continued} once the worktree is at {@code sha}, or by {@link
 * ContinueFailed} when it could not be moved (or the daemon was not idle). After {@link Continued}
 * the host sends {@link RunStep}s from {@code stepIndex} 0 again.
 *
 * <p>{@code branch} is the new run's ref name — for phase 2 the version tag, fetched as {@code
 * refs/tags/<branch>} — and {@code sha} the commit the worktree moves to. {@code token} is the new
 * run's own {@code ci-run} token, which from here on is the daemon's bearer for the socket, its
 * telemetry and git. {@code env} is the new run's run-scoped environment ({@code QITS_CI_BRANCH},
 * {@code QITS_CI_SHA}, {@code QITS_CI_RUN_ID}, {@code QITS_TOKEN}, {@code QITS_TOKEN_SUBJECT},
 * {@code QITS_MAVEN_AUTH_USR}, {@code QITS_MAVEN_AUTH_PSW}, {@code QITS_CI_REGISTRY_AUTH_CONFIG}),
 * laid between the lease's environment and each later step's own. Never {@code null} after
 * decoding — an absent field is an empty map.
 */
public record Continue(
    String runId, String branch, String sha, String token, Map<String, String> env)
    implements CiDaemonMessage {

  public Continue {
    env = env == null ? Map.of() : Map.copyOf(env);
  }

  /**
   * Redacted: the token and several {@code env} values open everything the new run may do, and a
   * record's generated {@code toString} would print them into any log line that formats a frame.
   * The env's keys are named, its values never.
   */
  @Override
  public String toString() {
    return "Continue[runId="
        + runId
        + ", branch="
        + branch
        + ", sha="
        + sha
        + ", token="
        + (token == null || token.isBlank() ? "<absent>" : "<set>")
        + ", env="
        + new TreeSet<>(env.keySet())
        + "]";
  }
}
