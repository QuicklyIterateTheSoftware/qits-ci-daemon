package eu.wohlben.qits.cidaemon;

import eu.wohlben.qits.cidaemon.protocol.Continue;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The run this lease is serving right now — the one thing about a lease that a {@code Continue}
 * changes (qits-732). {@link DaemonEnv} stays what the container was launched with; this holds what
 * the daemon presents from here on: the bearer the socket re-dials with, the run id the telemetry
 * labels, and the environment layer every later step child gets between the lease's environment
 * and its own {@code RunStep.env}.
 *
 * <p>One small mutable holder rather than a mutable {@link DaemonEnv}: the launch values are also
 * what initialization and the missing-variable check read, and those must never see a later run's.
 * Swapped as a whole under one lock, so a reader never sees the new token with the old run id.
 */
final class RunIdentity {

  private record Snapshot(
      String runId, String branch, String sha, String token, String tokenSubject,
      Map<String, String> env) {}

  private volatile Snapshot current;

  RunIdentity(DaemonEnv launch) {
    // No overlay for the first run: its values are already the process's own environment.
    this.current =
        new Snapshot(
            launch.runId(),
            launch.branch(),
            launch.sha(),
            launch.token(),
            launch.tokenSubject(),
            Map.of());
  }

  /**
   * Become {@code next}'s run. The overlay is its {@code env} with {@code QITS_TOKEN} set to its
   * token — the frame's field wins over an env entry, because the field is the bearer the host
   * actually commissioned and the two must not disagree in a step.
   */
  synchronized void continueAs(Continue next) {
    Map<String, String> env = new LinkedHashMap<>(next.env());
    if (next.token() != null && !next.token().isBlank()) {
      env.put("QITS_TOKEN", next.token());
    }
    if (next.runId() != null && !next.runId().isBlank()) {
      env.putIfAbsent("QITS_CI_RUN_ID", next.runId());
    }
    String token = next.token() == null || next.token().isBlank() ? token() : next.token();
    current =
        new Snapshot(
            next.runId(),
            next.branch(),
            next.sha(),
            token,
            env.getOrDefault("QITS_TOKEN_SUBJECT", null),
            Map.copyOf(env));
  }

  String runId() {
    return current.runId();
  }

  String branch() {
    return current.branch();
  }

  String sha() {
    return current.sha();
  }

  /** The bearer: the socket's handshake header, the telemetry's, git's credential helper's. */
  String token() {
    return current.token();
  }

  String tokenSubject() {
    return current.tokenSubject();
  }

  /** The layer between the lease's environment and a step's own; empty until a Continue. */
  Map<String, String> env() {
    return current.env();
  }

  /** Redacted, like {@link DaemonEnv}: the token and the overlay's values are never printed. */
  @Override
  public String toString() {
    Snapshot s = current;
    return "RunIdentity[runId="
        + s.runId()
        + ", sha="
        + s.sha()
        + ", token="
        + (s.token() == null || s.token().isBlank() ? "<absent>" : "<set>")
        + ", env="
        + new java.util.TreeSet<>(s.env().keySet())
        + "]";
  }
}
