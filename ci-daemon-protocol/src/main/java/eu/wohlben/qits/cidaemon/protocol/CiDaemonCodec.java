package eu.wohlben.qits.cidaemon.protocol;

import eu.wohlben.qits.cidaemon.protocol.CiDaemonProtocol.Field;
import eu.wohlben.qits.cidaemon.protocol.CiDaemonProtocol.Type;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The one place the control-socket messages become (and un-become) a flat {@code Map<String,
 * Object>} — the wire's lowest common denominator. Framework-free on purpose: qits-ci bridges the
 * map to/from JSON with its Jackson {@code ObjectMapper}, the {@code ci-daemon} binary with a
 * Vert.x {@code JsonObject} ({@code new JsonObject(map)} / {@code jsonObject.getMap()}), so neither
 * side reimplements the field mapping and a rename lands in exactly one file.
 *
 * <p>Numbers are read through {@link Number} so it doesn't matter whether the JSON layer decoded an
 * {@code int} as {@code Integer} (Jackson) or {@code Long} (Vert.x). Absent optional fields decode
 * to {@code null}.
 */
public final class CiDaemonCodec {

  private CiDaemonCodec() {}

  /** Flatten a message to its wire map, including the {@code "type"} discriminator. */
  public static Map<String, Object> encode(CiDaemonMessage message) {
    Map<String, Object> map = new LinkedHashMap<>();
    switch (message) {
      case Hello m -> {
        map.put(Field.TYPE, Type.HELLO);
        map.put(Field.DAEMON_ID, m.daemonId());
        map.put(Field.CAPABILITY_VERSION, m.capabilityVersion());
      }
      case AckReceived _ -> map.put(Field.TYPE, Type.ACK_RECEIVED); // no fields beyond the tag
      case Initialized _ -> map.put(Field.TYPE, Type.INITIALIZED); // no fields beyond the tag
      case InitFailed m -> {
        map.put(Field.TYPE, Type.INIT_FAILED);
        map.put(Field.REASON, m.reason() == null ? null : m.reason().name());
        map.put(Field.DETAIL, m.detail());
      }
      case StepChunk m -> {
        map.put(Field.TYPE, Type.STEP_CHUNK);
        map.put(Field.CORRELATION_ID, m.correlationId());
        map.put(Field.SEQ, m.seq());
        map.put(Field.STREAM, m.stream().name());
        map.put(Field.TEXT, m.text());
      }
      case StepFinished m -> {
        map.put(Field.TYPE, Type.STEP_FINISHED);
        map.put(Field.CORRELATION_ID, m.correlationId());
        map.put(Field.STEP_INDEX, m.stepIndex());
        map.put(Field.EXIT_CODE, m.exitCode());
        map.put(Field.TIMED_OUT, m.timedOut());
      }
      case Heartbeat _ -> map.put(Field.TYPE, Type.HEARTBEAT); // no fields beyond the tag
      case Ack m -> {
        map.put(Field.TYPE, Type.ACK);
        map.put(Field.CAPABILITY_VERSION, m.capabilityVersion());
      }
      case RunStep m -> {
        map.put(Field.TYPE, Type.RUN_STEP);
        map.put(Field.CORRELATION_ID, m.correlationId());
        map.put(Field.STEP_INDEX, m.stepIndex());
        map.put(Field.SCRIPT, m.script());
        map.put(Field.TIMEOUT_SECONDS, m.timeoutSeconds());
        // A nested object, not flattened keys: the step env's keys are the step's own and could be
        // anything, so they must never share a namespace with the frame's fields.
        map.put(Field.ENV, new LinkedHashMap<>(m.env()));
      }
      case Cancel m -> {
        map.put(Field.TYPE, Type.CANCEL);
        map.put(Field.CORRELATION_ID, m.correlationId());
      }
      case Close _ -> map.put(Field.TYPE, Type.CLOSE); // no fields beyond the tag
    }
    return map;
  }

  /** Rebuild a message from its wire map, dispatching on the {@code "type"} discriminator. */
  public static CiDaemonMessage decode(Map<String, Object> map) {
    String type = str(map, Field.TYPE);
    if (type == null) {
      throw new IllegalArgumentException("ci-daemon message has no '" + Field.TYPE + "' field");
    }
    return switch (type) {
      case Type.HELLO ->
          new Hello(str(map, Field.DAEMON_ID), intVal(map, Field.CAPABILITY_VERSION));
      case Type.ACK_RECEIVED -> new AckReceived();
      case Type.INITIALIZED -> new Initialized();
      case Type.INIT_FAILED -> new InitFailed(reason(map, Field.REASON), str(map, Field.DETAIL));
      case Type.STEP_CHUNK ->
          new StepChunk(
              str(map, Field.CORRELATION_ID),
              longVal(map, Field.SEQ),
              Stream.valueOf(str(map, Field.STREAM)),
              str(map, Field.TEXT));
      case Type.STEP_FINISHED ->
          new StepFinished(
              str(map, Field.CORRELATION_ID),
              intVal(map, Field.STEP_INDEX),
              intVal(map, Field.EXIT_CODE),
              boolVal(map, Field.TIMED_OUT));
      case Type.HEARTBEAT -> new Heartbeat();
      case Type.ACK -> new Ack(intVal(map, Field.CAPABILITY_VERSION));
      case Type.RUN_STEP ->
          new RunStep(
              str(map, Field.CORRELATION_ID),
              intVal(map, Field.STEP_INDEX),
              str(map, Field.SCRIPT),
              intVal(map, Field.TIMEOUT_SECONDS),
              stringMap(map, Field.ENV));
      case Type.CANCEL -> new Cancel(str(map, Field.CORRELATION_ID));
      case Type.CLOSE -> new Close();
      default -> throw new IllegalArgumentException("unknown ci-daemon message type: " + type);
    };
  }

  private static String str(Map<String, Object> map, String key) {
    Object value = map.get(key);
    return value == null ? null : value.toString();
  }

  private static int intVal(Map<String, Object> map, String key) {
    Object value = map.get(key);
    return value instanceof Number number ? number.intValue() : 0;
  }

  private static long longVal(Map<String, Object> map, String key) {
    Object value = map.get(key);
    return value instanceof Number number ? number.longValue() : 0L;
  }

  /**
   * A nested string map, as whatever the JSON layer decoded it to: a plain {@code Map} (Jackson,
   * or a map built by hand) or an {@code Iterable} of entries (a Vert.x {@code JsonObject}, which
   * is not a {@code Map}) — named structurally so this module still depends on nothing. Absent
   * decodes to an empty map; a {@code null} value is left out, because an environment variable
   * cannot hold one.
   */
  private static Map<String, String> stringMap(Map<String, Object> map, String key) {
    Object value = map.get(key);
    Map<String, String> out = new LinkedHashMap<>();
    Iterable<?> entries;
    if (value instanceof Map<?, ?> nested) {
      entries = nested.entrySet();
    } else if (value instanceof Iterable<?> iterable) {
      entries = iterable;
    } else if (value == null) {
      return out;
    } else {
      throw new IllegalArgumentException("'" + key + "' is not an object: " + value);
    }
    for (Object entry : entries) {
      if (entry instanceof Map.Entry<?, ?> e && e.getKey() != null && e.getValue() != null) {
        out.put(e.getKey().toString(), e.getValue().toString());
      }
    }
    return out;
  }

  private static boolean boolVal(Map<String, Object> map, String key) {
    Object value = map.get(key);
    return value instanceof Boolean bool && bool;
  }

  /**
   * An absent {@code reason} decodes to {@code null} rather than throwing, so a malformed {@link
   * InitFailed} still reaches the host as a failure it can record with its {@code detail}. An
   * <em>unknown</em> reason still throws, because that is a protocol the host does not speak and
   * quietly turning it into "no reason" would hide a capability mismatch.
   */
  private static InitFailed.Reason reason(Map<String, Object> map, String key) {
    String value = str(map, key);
    return value == null ? null : InitFailed.Reason.valueOf(value);
  }
}
