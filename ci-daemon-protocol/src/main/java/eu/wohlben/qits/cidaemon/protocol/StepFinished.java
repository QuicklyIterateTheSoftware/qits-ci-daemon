package eu.wohlben.qits.cidaemon.protocol;

/**
 * The terminal frame of a {@link RunStep}: the step process is gone and its exit code is known.
 * Also the answer to a {@link Cancel} — a cancelled step still finishes, it just finishes because
 * the daemon killed it.
 *
 * <p>{@code timedOut} is not derivable from {@code exitCode} and that is why it is a field. When
 * the daemon enforces {@code timeoutSeconds} it signals the child, so the exit code it reports is
 * the kill's (143, or 137 after the grace expires) and not the script's — indistinguishable from a
 * step that trapped a signal and exited that way on its own. The host records a timeout as a
 * timeout, not as a failure with a suspicious exit code.
 */
public record StepFinished(String correlationId, int exitCode, boolean timedOut)
    implements CiDaemonMessage {}
