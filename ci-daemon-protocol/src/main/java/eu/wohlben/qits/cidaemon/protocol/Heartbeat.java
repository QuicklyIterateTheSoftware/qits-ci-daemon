package eu.wohlben.qits.cidaemon.protocol;

/**
 * A periodic liveness ping from {@code ci-daemon}, every 10s from dial until close, so the host can
 * tell a silent-but-alive container from a wedged one. No fields: the connection identifies the
 * daemon.
 *
 * <p>It runs underneath everything, including a step producing no output for minutes — which is the
 * case it exists for, since the host's step timeout is a backstop and not a liveness probe.
 */
public record Heartbeat() implements CiDaemonMessage {}
