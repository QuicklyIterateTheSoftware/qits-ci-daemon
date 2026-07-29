package eu.wohlben.qits.cidaemon.protocol;

/**
 * qits-ci → {@code ci-daemon}: kill the running step's child process. The daemon answers with
 * {@link StepFinished} — a cancellation is a terminal outcome reported on the same frame as any
 * other, so the host's await completes normally rather than by timing out on a socket it then has
 * to reap.
 *
 * <p>The only other thing the host ever sends besides {@link Ack} and {@link RunStep}. A {@code
 * Cancel} arriving before {@link RunStep} has no child to kill; the daemon exits.
 */
public record Cancel(String correlationId) implements CiDaemonMessage {}
