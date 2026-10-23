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
 * <p>It used to live in qits-ci's vendored copy too, byte-identical, as the drift detector for two
 * sources that could be edited apart. There is one source now — qits-ci depends on the released
 * {@code eu.wohlben.qits:qits-ci-daemon-protocol} rather than copying it — so this suite guards the
 * contract itself rather than a copy of it, and it runs once, here, on the tree that is about to be
 * published.
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
    StepFinished ok = new StepFinished("c1", 0, 0, false);
    StepFinished killed = new StepFinished("c1", 2, 137, true);
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
  void stepFinishedCarriesItsStepIndexOnTheWire() {
    Map<String, Object> map = CiDaemonCodec.encode(new StepFinished("c1", 3, 1, false));
    assertEquals(3, map.get(CiDaemonProtocol.Field.STEP_INDEX));
  }

  @Test
  void runStepRoundTripsWithItsIndexAndEnv() {
    RunStep step =
        new RunStep("c1", 1, "set -e\nmvn -q verify\n", 900, Map.of("QITS_CI_STEP_NAME", "verify"));
    assertEquals(step, roundTrip(step));
    Map<String, Object> map = CiDaemonCodec.encode(step);
    assertEquals(CiDaemonProtocol.Type.RUN_STEP, map.get(CiDaemonProtocol.Field.TYPE));
    assertEquals(1, map.get(CiDaemonProtocol.Field.STEP_INDEX));
    // Nested, never flattened: a step variable named like a frame field must not collide with it.
    assertEquals(Map.of("QITS_CI_STEP_NAME", "verify"), map.get(CiDaemonProtocol.Field.ENV));
  }

  @Test
  void runStepEnvMayArriveAsAnIterableOfEntriesTheWayAVertxJsonObjectDoes() {
    Map<String, Object> map =
        new LinkedHashMap<>(CiDaemonCodec.encode(new RunStep("c1", 0, "true", 30, Map.of())));
    Map<String, Object> env = new LinkedHashMap<>();
    env.put("A", "1");
    env.put("N", 7L); // a JSON number still reaches the step as text
    env.put("GONE", null); // an environment variable cannot hold null; it is left out
    Iterable<Map.Entry<String, Object>> jsonObjectLike = env.entrySet()::iterator;
    map.put(CiDaemonProtocol.Field.ENV, jsonObjectLike);
    assertEquals(
        new RunStep("c1", 0, "true", 30, Map.of("A", "1", "N", "7")), CiDaemonCodec.decode(map));
  }

  @Test
  void runStepWithoutAnEnvDecodesToAnEmptyOne() {
    Map<String, Object> map =
        new LinkedHashMap<>(CiDaemonCodec.encode(new RunStep("c1", 0, "true", 30, null)));
    map.remove(CiDaemonProtocol.Field.ENV);
    assertEquals(Map.of(), ((RunStep) CiDaemonCodec.decode(map)).env());
  }

  @Test
  void runStepRejectsAnEnvThatIsNotAnObject() {
    Map<String, Object> map =
        new LinkedHashMap<>(CiDaemonCodec.encode(new RunStep("c1", 0, "true", 30, Map.of())));
    map.put(CiDaemonProtocol.Field.ENV, "A=1");
    assertThrows(IllegalArgumentException.class, () -> CiDaemonCodec.decode(map));
  }

  @Test
  void closeRoundTrips() {
    assertEquals(new Close(), roundTrip(new Close()));
    assertEquals(
        CiDaemonProtocol.Type.CLOSE,
        CiDaemonCodec.encode(new Close()).get(CiDaemonProtocol.Field.TYPE));
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
  void theCapabilityVersionIsTwo() {
    // Pinned so a bump is a deliberate edit rather than a side effect. It reaches qits-ci as a
    // version bump of this jar, and the host's side of the skew is decided there.
    assertEquals(2, CiDaemonProtocol.CAPABILITY_VERSION);
  }
}
