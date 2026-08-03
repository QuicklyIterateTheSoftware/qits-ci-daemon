package eu.wohlben.qits.cidaemon.protocol;

/**
 * The sealed set of control-socket messages. {@link CiDaemonCodec} encodes any of these to a
 * framework-free {@code Map} and decodes one back, so both sides can {@code switch} exhaustively
 * over the received type.
 */
public sealed interface CiDaemonMessage
    permits Hello,
        AckReceived,
        Initialized,
        InitFailed,
        StepChunk,
        StepFinished,
        Heartbeat,
        Ack,
        RunStep,
        Cancel {}
