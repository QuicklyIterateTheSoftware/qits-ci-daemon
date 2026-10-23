package eu.wohlben.qits.cidaemon.protocol;

import java.util.Map;

/**
 * qits-ci → {@code ci-daemon}: one step's script. Since capability 2 a lease's daemon answers any
 * number of these, one after another, all in the same worktree — {@code stepIndex} names which step
 * of the run this is, and comes back on that step's {@link StepFinished}.
 *
 * <p>{@code script} is repo-controlled hostile code by design. The container is the sandbox and
 * this daemon is the parent process that runs it; the host parsed the pipeline config against its
 * own bare cache and sends only this one step's script, never the config file.
 *
 * <p>{@code env} is the step's own environment, layered over the lease's: a key here replaces the
 * process env's value for this step's child only, and nothing of it outlives the step. It is the
 * step-specific part of what used to be a per-step container's environment (the step index, its
 * name, a step's own variables); what every step shares stays in the lease env. Never {@code null}
 * after decoding — an absent field is an empty map.
 *
 * <p>{@code timeoutSeconds} is enforced here, in-band, by the daemon that owns the child process —
 * SIGTERM, grace, SIGKILL, then {@link StepFinished} with {@code timedOut}. The host keeps its own
 * longer deadline as a backstop for a daemon that stops answering at all, but the in-band
 * enforcement is what makes a timeout a recorded outcome instead of a reaped container.
 */
public record RunStep(
    String correlationId, int stepIndex, String script, int timeoutSeconds, Map<String, String> env)
    implements CiDaemonMessage {

  public RunStep {
    env = env == null ? Map.of() : Map.copyOf(env);
  }
}
