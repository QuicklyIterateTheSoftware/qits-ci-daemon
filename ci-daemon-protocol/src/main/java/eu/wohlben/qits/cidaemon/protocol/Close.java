package eu.wohlben.qits.cidaemon.protocol;

/**
 * qits-ci → {@code ci-daemon}: the run is over — every step it wanted has finished, or the run was
 * cancelled. The daemon kills a step still running (its {@link StepFinished} is still sent), removes
 * the run's worktree with {@code git worktree remove --force}, closes the socket and exits 0. There
 * is no answer frame: the socket closing is the answer, and qits-ci deletes the lease after it.
 *
 * <p>A frame of its own rather than the host simply hanging up, because a hang-up is what an edge
 * redeploy looks like too, and the daemon re-dials through that. Only this frame means "done".
 */
public record Close() implements CiDaemonMessage {}
