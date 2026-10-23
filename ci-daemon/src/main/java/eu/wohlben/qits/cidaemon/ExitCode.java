package eu.wohlben.qits.cidaemon;

/**
 * The process exit codes this daemon can end with. Every one of them is a terminal condition —
 * <b>this daemon always exits</b>, which is the one deliberate inversion of the qits-workspace-daemon
 * shape it otherwise mirrors. A workspace container's daemon is the container's whole reason to be
 * alive and must outlive every failure; a CI lease's daemon has one run's steps to run, and when the
 * host says {@code Close} there is nothing for it to stay alive <em>for</em>.
 *
 * <p><b>{@link #OK} means the host's {@link eu.wohlben.qits.cidaemon.protocol.Close} was answered</b>
 * — the worktree removed and the socket closed — and nothing else does, whatever the steps' own exit
 * codes were: those travel in each {@code StepFinished}. {@link
 * #INIT_FAILED_SENT} is the case worth stating: the daemon did its job, reported a structured
 * failure, and the host has everything it needs; the container still did not run its step, and
 * {@code docker inspect} of the reaped container should say so. The exit code describes the
 * container's outcome, not the daemon's correctness — the socket already carried the latter.
 */
public final class ExitCode {

  /** The host sent {@code Close} and the lease was torn down. The only clean ending. */
  public static final int OK = 0;

  /** The env contract was not satisfied — no url, id, token, repository, branch, sha or run id. */
  public static final int MISCONFIGURED = 2;

  /** The control socket could not be reached within the dial budget. */
  public static final int DIAL_FAILED = 3;

  /**
   * The host's {@code Ack} carried a capability version this binary does not know. There is no
   * compat mode: one honest log line beats a half-understood conversation.
   */
  public static final int CAPABILITY_MISMATCH = 4;

  /** The mirror or the worktree could not be made; an {@code InitFailed} was delivered. */
  public static final int INIT_FAILED_SENT = 5;

  /**
   * The socket dropped and could not be re-established within the reconnect budget before the host
   * closed the lease. A drop alone is not this ending — an edge redeploy drops every socket and
   * the daemon re-dials through it; a host that has not taken the launch back by the end of the
   * budget has reaped it, and then there is nothing to reconnect to.
   */
  public static final int SOCKET_CLOSED_EARLY = 6;

  /** A {@code Cancel} arrived before any step existed to cancel. */
  public static final int CANCELLED_BEFORE_STEP = 7;

  private ExitCode() {}
}
