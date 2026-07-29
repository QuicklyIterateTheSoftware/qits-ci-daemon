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
 * <p>The contract is one step's worth of conversation, because that is a container's whole life:
 * {@link Hello}/{@link Ack}, then {@link Initialized} or {@link InitFailed}, then exactly one
 * {@link RunStep} answered by {@link StepChunk}* and a terminal {@link StepFinished}, with {@link
 * Cancel} as the only other thing the host may send and {@link Heartbeat} running underneath from
 * dial to close.
 *
 * <p><b>Identity is not on the wire.</b> The daemon presents {@code X-Qits-Ci-Daemon-Id} and {@code
 * X-Qits-Ci-Daemon-Secret} as handshake headers and the host validates them before the first frame
 * is read; the {@code daemonId} in {@link Hello} is an assertion the host checks against the
 * connection it already authenticated, never the thing that identifies it. The workspace control
 * socket takes its caller's identity from a path parameter, which is its known impersonation bug
 * (migration-plan §9 item 22); this contract does not reproduce it.
 */
public final class CiDaemonProtocol {

  /**
   * The capability version {@code ci-daemon} announces in its {@link Hello} and the host echoes in
   * its {@link Ack}. Bumped when the wire contract changes in a way either side must branch on.
   *
   * <p>There is no compat mode at version 1 and no soft landing planned for version 2: a daemon
   * that reads an {@link Ack} carrying a version it does not know exits nonzero rather than
   * guessing, because a container's whole life is one step and a half-understood protocol has
   * nothing to degrade to. The daemon binary is pinned per run by qits-ci (the run row records
   * which version produced its results), so a version mismatch means a deploy landed between run
   * creation and container start — rare, loud, and cheap to retry.
   */
  public static final int CAPABILITY_VERSION = 1;

  private CiDaemonProtocol() {}

  /** The {@code "type"} discriminator values. */
  public static final class Type {
    // ci-daemon -> qits-ci
    public static final String HELLO = "hello";
    public static final String INITIALIZED = "initialized";
    public static final String INIT_FAILED = "initFailed";
    public static final String STEP_CHUNK = "stepChunk";
    public static final String STEP_FINISHED = "stepFinished";
    public static final String HEARTBEAT = "heartbeat";
    // qits-ci -> ci-daemon
    public static final String ACK = "ack";
    public static final String RUN_STEP = "runStep";
    public static final String CANCEL = "cancel";

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

    private Field() {}
  }
}
