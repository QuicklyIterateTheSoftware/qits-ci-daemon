package eu.wohlben.qits.cidaemon.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The wire contract's fast, framework-free guard: every message survives {@code encode → decode}
 * unchanged, and the discriminator round-trips through the {@link CiDaemonProtocol.Type} constants.
 * qits-ci and {@code ci-daemon} only bridge the map to their JSON library, so this test covers the
 * shared mapping both depend on.
 *
 * <p>It lives in qits-ci's vendored copy too, byte-identical. That is the drift detector: a copy
 * edited on one side fails here rather than in production.
 */
class CiDaemonCodecTest {

  private static CiDaemonMessage roundTrip(CiDaemonMessage message) {
    return CiDaemonCodec.decode(CiDaemonCodec.encode(message));
  }

  @Test
  void helloRoundTrips() {
    Hello hello = new Hello("daemon-1", CiDaemonProtocol.CAPABILITY_VERSION);
    assertEquals(hello, roundTrip(hello));
    assertEquals(
        CiDaemonProtocol.Type.HELLO, CiDaemonCodec.encode(hello).get(CiDaemonProtocol.Field.TYPE));
  }

  @Test
  void ackReceivedRoundTrips() {
    assertEquals(new AckReceived(), roundTrip(new AckReceived()));
    assertEquals(
        CiDaemonProtocol.Type.ACK_RECEIVED,
        CiDaemonCodec.encode(new AckReceived()).get(CiDaemonProtocol.Field.TYPE));
  }

  @Test
  void initializedRoundTrips() {
    assertEquals(new Initialized(), roundTrip(new Initialized()));
    assertEquals(
        CiDaemonProtocol.Type.INITIALIZED,
        CiDaemonCodec.encode(new Initialized()).get(CiDaemonProtocol.Field.TYPE));
  }

  @Test
  void initFailedRoundTripsEveryReason() {
    for (InitFailed.Reason reason : InitFailed.Reason.values()) {
      InitFailed failed = new InitFailed(reason, "git exited 128");
      assertEquals(failed, roundTrip(failed));
    }
    assertEquals(
        CiDaemonProtocol.Type.INIT_FAILED,
        CiDaemonCodec.encode(new InitFailed(InitFailed.Reason.SHA_GONE, null))
            .get(CiDaemonProtocol.Field.TYPE));
  }

  @Test
  void initFailedToleratesAnAbsentDetail() {
    InitFailed failed = new InitFailed(InitFailed.Reason.TOOLING_MISSING, null);
    assertEquals(failed, roundTrip(failed));
  }

  @Test
  void stepChunkRoundTripsBothStreams() {
    StepChunk out = new StepChunk("c1", 0L, Stream.OUT, "line\n");
    StepChunk err = new StepChunk("c1", 1L, Stream.ERR, "oops\n");
    assertEquals(out, roundTrip(out));
    assertEquals(err, roundTrip(err));
    assertEquals(
        CiDaemonProtocol.Type.STEP_CHUNK,
        CiDaemonCodec.encode(out).get(CiDaemonProtocol.Field.TYPE));
  }

  @Test
  void stepChunkKeepsASequenceBeyondIntRange() {
    // seq is a long on the wire and must stay one: a step that runs long enough to overflow an int
    // would silently restart its ordering, which is exactly the gap the counter exists to detect.
    StepChunk chunk = new StepChunk("c1", 3_000_000_000L, Stream.OUT, "x");
    assertEquals(chunk, roundTrip(chunk));
  }

  @Test
  void stepFinishedRoundTripsBothTimeoutStates() {
    StepFinished ok = new StepFinished("c1", 0, false);
    StepFinished killed = new StepFinished("c1", 137, true);
    assertEquals(ok, roundTrip(ok));
    assertEquals(killed, roundTrip(killed));
    assertEquals(
        CiDaemonProtocol.Type.STEP_FINISHED,
        CiDaemonCodec.encode(ok).get(CiDaemonProtocol.Field.TYPE));
  }

  @Test
  void heartbeatRoundTrips() {
    assertEquals(new Heartbeat(), roundTrip(new Heartbeat()));
    assertEquals(
        CiDaemonProtocol.Type.HEARTBEAT,
        CiDaemonCodec.encode(new Heartbeat()).get(CiDaemonProtocol.Field.TYPE));
  }

  @Test
  void ackRoundTripsTheHostsCapabilityVersion() {
    Ack ack = new Ack(CiDaemonProtocol.CAPABILITY_VERSION);
    assertEquals(ack, roundTrip(ack));
    assertEquals(
        CiDaemonProtocol.Type.ACK, CiDaemonCodec.encode(ack).get(CiDaemonProtocol.Field.TYPE));
  }

  @Test
  void ackFromAFutureHostKeepsItsVersionRatherThanBeingClamped() {
    // The daemon exits nonzero on a version it does not know, and it can only do that if the codec
    // hands it the number it actually received.
    Ack ack = new Ack(CiDaemonProtocol.CAPABILITY_VERSION + 7);
    assertEquals(ack, roundTrip(ack));
  }

  @Test
  void runStepRoundTrips() {
    RunStep step = new RunStep("c1", "set -e\nmvn -q verify\n", 900);
    assertEquals(step, roundTrip(step));
    assertEquals(
        CiDaemonProtocol.Type.RUN_STEP,
        CiDaemonCodec.encode(step).get(CiDaemonProtocol.Field.TYPE));
  }

  @Test
  void cancelRoundTrips() {
    Cancel cancel = new Cancel("c1");
    assertEquals(cancel, roundTrip(cancel));
    assertEquals(
        CiDaemonProtocol.Type.CANCEL,
        CiDaemonCodec.encode(cancel).get(CiDaemonProtocol.Field.TYPE));
  }

  @Test
  void anAbsentFieldDecodesToItsEmptyValue() {
    // A frame from a peer that predates a field must decode rather than fail — the same tolerance
    // the workspace codec has, exercised here on the fields most likely to be added around.
    Map<String, Object> map = new LinkedHashMap<>(CiDaemonCodec.encode(new Hello("daemon-1", 1)));
    map.remove(CiDaemonProtocol.Field.DAEMON_ID);
    map.remove(CiDaemonProtocol.Field.CAPABILITY_VERSION);
    assertEquals(new Hello(null, 0), CiDaemonCodec.decode(map));

    Map<String, Object> chunk =
        new LinkedHashMap<>(CiDaemonCodec.encode(new StepChunk("c1", 4L, Stream.OUT, "x")));
    chunk.remove(CiDaemonProtocol.Field.SEQ);
    assertEquals(new StepChunk("c1", 0L, Stream.OUT, "x"), CiDaemonCodec.decode(chunk));
  }

  @Test
  void decodeRejectsMissingType() {
    assertThrows(IllegalArgumentException.class, () -> CiDaemonCodec.decode(Map.of()));
  }

  @Test
  void decodeRejectsUnknownType() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CiDaemonCodec.decode(Map.of(CiDaemonProtocol.Field.TYPE, "nope")));
  }

  @Test
  void decodeRejectsAnUnknownInitFailureReason() {
    // A reason this version does not know is a capability mismatch, not a nullable field. Throwing
    // makes the host log an undecodable frame; mapping it to null would record the run as failed
    // for no stated cause.
    Map<String, Object> map =
        Map.of(
            CiDaemonProtocol.Field.TYPE,
            CiDaemonProtocol.Type.INIT_FAILED,
            CiDaemonProtocol.Field.REASON,
            "DISK_FULL");
    assertThrows(IllegalArgumentException.class, () -> CiDaemonCodec.decode(map));
  }

  @Test
  void theCapabilityVersionIsOne() {
    // Pinned so a bump is a deliberate edit here and in the vendored copy, never a side effect.
    assertEquals(1, CiDaemonProtocol.CAPABILITY_VERSION);
  }
}
