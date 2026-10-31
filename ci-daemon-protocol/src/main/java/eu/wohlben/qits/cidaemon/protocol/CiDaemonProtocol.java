package eu.wohlben.qits.cidaemon.protocol;

/**
 * The single source of truth for the ci-daemon control-socket wire contract's tags and field names.
 *
 * <p>Messages are JSON objects with a {@code "type"} discriminator ({@link Type}) and a flat set of
 * fields ({@link Field}). The records in this package model each message's shape; qits-ci
 * (de)serializes them with its Jackson {@code ObjectMapper}, the {@code ci-daemon} binary maps them
 * to/from a Vert.x {@code JsonObject} field-by-field — both against these constants, so a rename is
 * caught in one place.
 *
 * <p>The contract is one run's worth of conversation, because that is a lease container's whole
 * life: {@link Hello}/{@link Ack}/{@link AckReceived}, then {@link Initialized} or {@link
 * InitFailed}, then any number of {@link RunStep}s, each answered by {@link StepChunk}* and a
 * terminal {@link StepFinished}, and finally {@link Close} — or, since capability 3, a {@link
 * Continue} answered by {@link Continued} or {@link ContinueFailed}, and the next run's {@link
 * RunStep}s in the same worktree. {@link Cancel} kills the step that is
 * running, and {@link Heartbeat} runs underneath from dial to close. {@link AckReceived} is the one frame in that
 * list no real run ever waits for — see its own javadoc for who does and why it costs no capability
 * bump.
 *
 * <p><b>One handshake, and identity travels in the frame, not a header.</b> The daemon authenticates
 * the connection with {@code Authorization: Bearer $QITS_TOKEN} alone — the run's own token — and
 * names its launch id in-band, in the {@code daemonId} field of the first {@link Hello} frame
 * itself. The host binds that launch id to the token's own subject and refuses a mismatch; there is
 * no separate id/secret handshake header pair to validate first. The workspace control socket takes
 * its caller's identity from a path parameter, which is its known impersonation bug (migration-plan
 * §9 item 22); this contract does not reproduce it.
 */
public final class CiDaemonProtocol {

  /**
   * The capability version {@code ci-daemon} announces in its {@link Hello} and the host echoes in
   * its {@link Ack}. Bumped when the wire contract changes in a way either side must branch on.
   *
   * <p>Version 2 (qits-1541) is one daemon per CI lease: {@link RunStep} gained {@code stepIndex}
   * and {@code env}, {@link StepFinished} gained {@code stepIndex}, and {@link Close} ends the
   * lease. Version 1 — one container, one step, one shallow clone — is gone, not kept beside it.
   *
   * <p>Version 3 (qits-732): one lease may carry two runs of one release request — phase 1's QA
   * on the fold and phase 2's build of the cut tag — joined by {@link Continue}. After the first
   * run's last {@link StepFinished} the host does not {@link Close}; it sends {@link Continue}, the
   * daemon moves the same worktree to the new sha under the new run's identity and answers {@link
   * Continued} (or {@link ContinueFailed}), and {@link RunStep}s start again at {@code stepIndex}
   * 0. Nothing else changed, so a daemon speaking 3 still accepts a host's {@link Ack} of 2: that
   * host simply never sends {@link Continue}.
   *
   * <p>There is no compat mode: a daemon that reads an {@link Ack} carrying a version it does not
   * know exits nonzero rather than guessing, because a half-understood protocol has nothing to
   * degrade to. The daemon binary is pinned per run by qits-ci (the protocol jar's version is the
   * binary's), so a version mismatch means a deploy landed between run creation and container
   * start — rare, loud, and cheap to retry.
   */
  public static final int CAPABILITY_VERSION = 3;

  private CiDaemonProtocol() {}

  /** The {@code "type"} discriminator values. */
  public static final class Type {
    // ci-daemon -> qits-ci
    public static final String HELLO = "hello";
    public static final String ACK_RECEIVED = "ackReceived";
    public static final String INITIALIZED = "initialized";
    public static final String INIT_FAILED = "initFailed";
    public static final String STEP_CHUNK = "stepChunk";
    public static final String STEP_FINISHED = "stepFinished";
    public static final String HEARTBEAT = "heartbeat";
    public static final String CONTINUED = "continued";
    public static final String CONTINUE_FAILED = "continueFailed";
    // qits-ci -> ci-daemon
    public static final String ACK = "ack";
    public static final String RUN_STEP = "runStep";
    public static final String CANCEL = "cancel";
    public static final String CLOSE = "close";
    public static final String CONTINUE = "continue";

    private Type() {}
  }

  /** The JSON field names shared by both codecs. */
  public static final class Field {
    public static final String TYPE = "type";
    public static final String DAEMON_ID = "daemonId";
    public static final String CAPABILITY_VERSION = "capabilityVersion";
    public static final String REASON = "reason";
    public static final String DETAIL = "detail";
    public static final String CORRELATION_ID = "correlationId";
    public static final String SEQ = "seq";
    public static final String STREAM = "stream";
    public static final String TEXT = "text";
    public static final String EXIT_CODE = "exitCode";
    public static final String TIMED_OUT = "timedOut";
    public static final String SCRIPT = "script";
    public static final String TIMEOUT_SECONDS = "timeoutSeconds";
    public static final String STEP_INDEX = "stepIndex";
    public static final String ENV = "env";
    public static final String RUN_ID = "runId";
    public static final String BRANCH = "branch";
    public static final String SHA = "sha";
    public static final String TOKEN = "token";

    private Field() {}
  }
}
