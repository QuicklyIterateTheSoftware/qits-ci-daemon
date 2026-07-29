package eu.wohlben.qits.cidaemon;

/**
 * The process exit codes this daemon can end with. Every one of them is a terminal condition —
 * <b>this daemon always exits</b>, which is the one deliberate inversion of the qits-workspace-daemon
 * shape it otherwise mirrors. A workspace container's daemon is the container's whole reason to be
 * alive and must outlive every failure; a step container's daemon has exactly one step to run, so
 * there is nothing for it to stay alive <em>for</em>.
 *
 * <p><b>{@link #OK} means a {@link eu.wohlben.qits.cidaemon.protocol.StepFinished} was delivered</b>,
 * and nothing else does — not even the paths where the daemon behaved perfectly. {@link
 * #INIT_FAILED_SENT} is the case worth stating: the daemon did its job, reported a structured
 * failure, and the host has everything it needs; the container still did not run its step, and
 * {@code docker inspect} of the reaped container should say so. The exit code describes the
 * container's outcome, not the daemon's correctness — the socket already carried the latter.
 */
public final class ExitCode {

  /** A step ran and its {@code StepFinished} reached the host. The only clean ending. */
  public static final int OK = 0;

  /** The env contract was not satisfied — no url, id, secret, repository, branch or sha. */
  public static final int MISCONFIGURED = 2;

  /** The control socket could not be reached within the dial budget. */
  public static final int DIAL_FAILED = 3;

  /**
   * The host's {@code Ack} carried a capability version this binary does not know. There is no
   * compat mode at version 1 and no soft landing planned for version 2: one honest log line beats a
   * half-understood conversation about a container that lives for one step.
   */
  public static final int CAPABILITY_MISMATCH = 4;

  /** Clone or checkout failed; an {@code InitFailed} was delivered and no step ran. */
  public static final int INIT_FAILED_SENT = 5;

  /**
   * The socket closed before a {@code RunStep} arrived. Not a reason to re-dial: the host has
   * reaped us, or decided not to give us work, and either way there is nothing to reconnect to.
   */
  public static final int SOCKET_CLOSED_EARLY = 6;

  /** A {@code Cancel} arrived before any step existed to cancel. */
  public static final int CANCELLED_BEFORE_STEP = 7;

  private ExitCode() {}
}
