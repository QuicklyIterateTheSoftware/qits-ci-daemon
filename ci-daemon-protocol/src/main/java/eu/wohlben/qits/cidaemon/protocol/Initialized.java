package eu.wohlben.qits.cidaemon.protocol;

/**
 * {@code ci-daemon} → qits-ci: the clone and checkout succeeded and the container is ready for its
 * step. The host answers with {@link RunStep} — the step's script arrives as the reply to this
 * frame, which is what keeps the host from ever initiating anything toward a container.
 *
 * <p>No fields: the connection identifies the daemon and the daemon has exactly one step to
 * initialize for. Anything the host wants to know about the checkout it already knows — it chose
 * the sha.
 */
public record Initialized() implements CiDaemonMessage {}
