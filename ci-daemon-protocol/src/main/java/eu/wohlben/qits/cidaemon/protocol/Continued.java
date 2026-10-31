package eu.wohlben.qits.cidaemon.protocol;

/**
 * {@code ci-daemon} → qits-ci (capability 3, qits-732): the {@link Continue} for {@code runId} is
 * done — the worktree is at its sha, the daemon's run-scoped identity is the new run's, and the
 * next {@link RunStep} may start again at {@code stepIndex} 0. Held through a socket outage and
 * sent again on the next {@link Ack} until a {@link RunStep} answers it, like {@link Initialized}.
 */
public record Continued(String runId) implements CiDaemonMessage {}
