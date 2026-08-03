package eu.wohlben.qits.cidaemon.protocol;

/**
 * {@code ci-daemon} → qits-ci: the {@link Ack} arrived. Sent the moment a matching-version {@link
 * Ack} is processed, before the clone even starts, so it proves exactly one thing and no more:
 * host→daemon delivery over this socket works. {@link Hello} already proves the other direction
 * (daemon→host), so the pair is what turns "the daemon dialled" into a real round trip — the
 * distinction the container probe (qits-ci's {@code CiDaemonContainerProbe}) exists to draw,
 * because a real run's {@code RunStep} is host→daemon too and a daemon that can only be heard, never
 * spoken to, would still pass a proof that stopped at {@link Hello}.
 *
 * <p>No fields: the connection identifies the daemon and there is nothing else to say about
 * receiving a frame with one field the daemon already checked.
 *
 * <p><b>Deliberately not a {@link CiDaemonProtocol#CAPABILITY_VERSION} bump.</b> A real run never
 * waits for this frame — only the probe does — so an old daemon that never sends it costs a probe a
 * {@code REJECTED} verdict and costs nothing else: it is still adopted for real runs under whatever
 * pin already proved it, and a peer that does not recognise the frame at all drops it rather than
 * failing, on both sides (this daemon's {@code ControlSocket#onFrame}, qits-ci's {@code
 * CiDaemonSocket#onMessage}) — that tolerance is what a version bump exists to protect when it is
 * missing, and here it is not. A bump would instead make an old, already-pinned daemon fail
 * <em>every</em> real run's {@link Ack} the moment qits-ci deployed this change, for no gain a probe
 * verdict does not already give more cheaply.
 */
public record AckReceived() implements CiDaemonMessage {}
