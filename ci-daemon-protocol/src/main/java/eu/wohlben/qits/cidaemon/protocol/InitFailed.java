package eu.wohlben.qits.cidaemon.protocol;

/**
 * {@code ci-daemon} → qits-ci: setup failed, no step will run, the container is done. Terminal in
 * place of {@link Initialized}.
 *
 * <p>This is what retires the prelude sentinel: the old runner inferred a failed setup from a
 * marker string in the output tail, so a step that merely echoed the marker looked like a broken
 * clone. A {@link Reason} the host can switch on is the whole point — {@link Reason#SHA_GONE} in
 * particular carries the force-push semantic the run orchestrator acts on (it re-reads the config
 * source to confirm, then discards the run rather than recording a failure against a commit that no
 * longer exists).
 *
 * <p>{@code detail} is free text for the human reading the run — the tail of whatever git said —
 * and is bounded at the source on the same budget as a {@link StepChunk}. It is attacker-influenced
 * data like everything else from a container: recorded, never parsed for meaning.
 */
public record InitFailed(Reason reason, String detail) implements CiDaemonMessage {

  /**
   * Why setup failed, in the vocabulary the host branches on. Deliberately three values and not a
   * free-form string: each maps to a distinct recorded outcome, and a fourth situation should
   * arrive as a fourth constant with a capability bump rather than hide inside {@code detail}.
   */
  public enum Reason {
    /** {@code git clone} did not produce a checkout — bad url, auth, network, disk. */
    CLONE_FAILED,

    /**
     * The clone succeeded but {@code git checkout $QITS_CI_SHA} did not find the commit. The
     * shallow fetch is by branch, so a force-push between the host's ancestor check and the
     * container's clone lands here — the backstop for the race the host cannot close.
     */
    SHA_GONE,

    /** The image does not satisfy the contract: no {@code git}, or no {@code bash}. */
    TOOLING_MISSING
  }
}
