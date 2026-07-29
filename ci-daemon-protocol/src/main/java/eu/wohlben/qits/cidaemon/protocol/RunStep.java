package eu.wohlben.qits.cidaemon.protocol;

/**
 * qits-ci → {@code ci-daemon}: the step's script, sent as the reply to {@link Initialized}. Exactly
 * one per container lifetime — one step, one container, and the daemon exits after answering it.
 *
 * <p>{@code script} is repo-controlled hostile code by design. The container is the sandbox and
 * this daemon is the parent process that runs it; the host parsed the pipeline config against its
 * own bare cache and sends only this one step's script, never the config file.
 *
 * <p>{@code timeoutSeconds} is enforced here, in-band, by the daemon that owns the child process —
 * SIGTERM, grace, SIGKILL, then {@link StepFinished} with {@code timedOut}. The host keeps its own
 * longer deadline as a backstop for a daemon that stops answering at all, but the in-band
 * enforcement is what makes a timeout a recorded outcome instead of a reaped container.
 */
public record RunStep(String correlationId, String script, int timeoutSeconds)
    implements CiDaemonMessage {}
