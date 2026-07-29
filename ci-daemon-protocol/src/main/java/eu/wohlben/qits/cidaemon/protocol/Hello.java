package eu.wohlben.qits.cidaemon.protocol;

/**
 * The first frame {@code ci-daemon} sends after the upgrade: the identity it was launched with
 * ({@code $QITS_CI_DAEMON_ID}) plus its {@link CiDaemonProtocol#CAPABILITY_VERSION}. The host
 * replies with {@link Ack}.
 *
 * <p>{@code daemonId} is a claim, not a credential. The connection was already authenticated by the
 * {@code X-Qits-Ci-Daemon-Id}/{@code X-Qits-Ci-Daemon-Secret} handshake headers before this frame
 * was read; the host checks this field against the launch record it matched there and closes on a
 * disagreement rather than trusting it. It is on the wire so a log line about a frame names the
 * container it came from without a lookup.
 */
public record Hello(String daemonId, int capabilityVersion) implements CiDaemonMessage {}
