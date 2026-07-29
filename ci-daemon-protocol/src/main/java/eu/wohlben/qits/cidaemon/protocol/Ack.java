package eu.wohlben.qits.cidaemon.protocol;

/**
 * qits-ci's acknowledgement of a {@link Hello}: the handshake is complete, and the host's own
 * {@link CiDaemonProtocol#CAPABILITY_VERSION} is in it.
 *
 * <p>The version travels back deliberately. A daemon that sees one it does not know exits nonzero
 * and the container's log says why; there is no compat mode at version 1, and no negotiation — the
 * host does not adapt to an old daemon, because qits-ci pins the daemon version per run and a
 * mismatch means a deploy raced a run rather than that two supported versions are in play.
 */
public record Ack(int capabilityVersion) implements CiDaemonMessage {}
