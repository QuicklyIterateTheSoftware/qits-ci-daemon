package eu.wohlben.qits.cidaemon.protocol;

/**
 * {@code ci-daemon} → qits-ci (capability 3, qits-732): the {@link Continue} for {@code runId} did
 * not take. The daemon stays alive and the host decides what follows — a {@link Close}, normally.
 *
 * <p>{@code detail} is the bounded tail of whatever git said, recorded and never parsed, exactly as
 * {@link InitFailed}'s is.
 */
public record ContinueFailed(String runId, Reason reason, String detail)
    implements CiDaemonMessage {

  /** Why, in the vocabulary the host branches on — the {@link InitFailed.Reason} shape. */
  public enum Reason {
    /** Neither {@code refs/tags/<branch>} nor the sha could be fetched into the mirror. */
    FETCH_FAILED,

    /** The commit is in the mirror but the worktree could not be moved to it. */
    CHECKOUT_FAILED,

    /**
     * A step was running or queued, or the worktree was not initialized yet. The current run is
     * left exactly as it was.
     */
    NOT_IDLE
  }
}
