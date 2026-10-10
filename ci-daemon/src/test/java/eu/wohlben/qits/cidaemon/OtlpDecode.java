package eu.wohlben.qits.cidaemon;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The suite's reading of OTLP {@code ExportLogsServiceRequest} and {@code ExportTraceServiceRequest}:
 * a generic protobuf walk by field number and wire type, written from opentelemetry-proto's field
 * numbers rather than from {@link Otlp}'s constants, so the encoder is checked against the schema and
 * not against itself. Copied and extended from qits-ci-runner-daemon's.
 *
 * <p>Ids are decoded to lowercase hex, which is how every OTel tool prints them.
 */
final class OtlpDecode {

  /** One decoded log record. {@code traceId}/{@code spanId} are "" when the record names none. */
  record Line(
      long timeUnixNano,
      long observedTimeUnixNano,
      int severityNumber,
      String severityText,
      String body,
      Map<String, Object> attributes,
      String traceId,
      String spanId) {}

  /** One decoded span. {@code parentSpanId} is "" for a root. */
  record Span(
      String traceId,
      String spanId,
      String parentSpanId,
      String name,
      int kind,
      long startTimeUnixNano,
      long endTimeUnixNano,
      Map<String, Object> attributes,
      int statusCode,
      String statusMessage) {}

  /** A decoded logs request: its one resource and its one scope's records. */
  record Logs(Map<String, Object> resource, String scopeName, String scopeVersion, List<Line> lines) {}

  /** A decoded traces request: its one resource and its one scope's spans. */
  record Traces(
      Map<String, Object> resource, String scopeName, String scopeVersion, List<Span> spans) {}

  static Logs logs(byte[] bytes) {
    Map<Integer, List<Object>> rl = only(fields(bytes).get(1), "ResourceLogs");
    Map<String, Object> resource = keyValues(fields((byte[]) rl.get(1).getFirst()).get(1));
    Map<Integer, List<Object>> scopeLogs = only(rl.get(2), "ScopeLogs");
    Map<Integer, List<Object>> scope = fields((byte[]) scopeLogs.get(1).getFirst());
    List<Line> lines = new ArrayList<>();
    for (Object record : scopeLogs.getOrDefault(2, List.of())) {
      Map<Integer, List<Object>> r = fields((byte[]) record);
      lines.add(
          new Line(
              (Long) r.get(1).getFirst(),
              (Long) r.get(11).getFirst(),
              ((Long) r.get(2).getFirst()).intValue(),
              string(r.get(3).getFirst()),
              string(fields((byte[]) r.get(5).getFirst()).get(1).getFirst()),
              keyValues(r.get(6)),
              hex(r.get(9)),
              hex(r.get(10))));
    }
    return new Logs(
        resource, string(scope.get(1).getFirst()), string(scope.get(2).getFirst()), lines);
  }

  static Traces traces(byte[] bytes) {
    Map<Integer, List<Object>> rs = only(fields(bytes).get(1), "ResourceSpans");
    Map<String, Object> resource = keyValues(fields((byte[]) rs.get(1).getFirst()).get(1));
    Map<Integer, List<Object>> scopeSpans = only(rs.get(2), "ScopeSpans");
    Map<Integer, List<Object>> scope = fields((byte[]) scopeSpans.get(1).getFirst());
    List<Span> spans = new ArrayList<>();
    for (Object encoded : scopeSpans.getOrDefault(2, List.of())) {
      Map<Integer, List<Object>> s = fields((byte[]) encoded);
      Map<Integer, List<Object>> status =
          s.containsKey(15) ? fields((byte[]) s.get(15).getFirst()) : Map.of();
      spans.add(
          new Span(
              hex(s.get(1)),
              hex(s.get(2)),
              hex(s.get(4)),
              string(s.get(5).getFirst()),
              ((Long) s.get(6).getFirst()).intValue(),
              (Long) s.get(7).getFirst(),
              (Long) s.get(8).getFirst(),
              keyValues(s.get(9)),
              status.containsKey(3) ? ((Long) status.get(3).getFirst()).intValue() : 0,
              status.containsKey(2) ? string(status.get(2).getFirst()) : ""));
    }
    return new Traces(
        resource, string(scope.get(1).getFirst()), string(scope.get(2).getFirst()), spans);
  }

  private static Map<Integer, List<Object>> only(List<Object> entries, String what) {
    if (entries == null || entries.size() != 1) {
      throw new AssertionError(
          "expected one " + what + ", got " + (entries == null ? 0 : entries.size()));
    }
    return fields((byte[]) entries.getFirst());
  }

  /** AnyValue: 1 string, 2 bool, 3 int64. */
  private static Map<String, Object> keyValues(List<Object> entries) {
    Map<String, Object> map = new LinkedHashMap<>();
    if (entries == null) {
      return map;
    }
    for (Object entry : entries) {
      Map<Integer, List<Object>> kv = fields((byte[]) entry);
      Map<Integer, List<Object>> any = fields((byte[]) kv.get(2).getFirst());
      Object value;
      if (any.containsKey(1)) {
        value = string(any.get(1).getFirst());
      } else if (any.containsKey(2)) {
        value = ((Long) any.get(2).getFirst()) != 0;
      } else if (any.containsKey(3)) {
        value = any.get(3).getFirst();
      } else {
        throw new AssertionError("an AnyValue of a kind the daemon never writes: " + any.keySet());
      }
      map.put(string(kv.get(1).getFirst()), value);
    }
    return map;
  }

  private static String hex(List<Object> value) {
    return value == null ? "" : HexFormat.of().formatHex((byte[]) value.getFirst());
  }

  private static String string(Object bytes) {
    return new String((byte[]) bytes, StandardCharsets.UTF_8);
  }

  /** Every field of one message: varints and fixed64s as Long, length-delimited as byte[]. */
  static Map<Integer, List<Object>> fields(byte[] bytes) {
    Map<Integer, List<Object>> fields = new LinkedHashMap<>();
    int[] at = {0};
    while (at[0] < bytes.length) {
      long tag = varint(bytes, at);
      int field = (int) (tag >>> 3);
      Object value =
          switch ((int) (tag & 7)) {
            case 0 -> varint(bytes, at);
            case 1 -> {
              long v = 0;
              for (int i = 0; i < 8; i++) {
                v |= (bytes[at[0] + i] & 0xFFL) << (8 * i);
              }
              at[0] += 8;
              yield v;
            }
            case 2 -> {
              int length = (int) varint(bytes, at);
              byte[] v = new byte[length];
              System.arraycopy(bytes, at[0], v, 0, length);
              at[0] += length;
              yield v;
            }
            default -> throw new AssertionError("unexpected wire type in tag " + tag);
          };
      fields.computeIfAbsent(field, k -> new ArrayList<>()).add(value);
    }
    return fields;
  }

  private static long varint(byte[] bytes, int[] at) {
    long value = 0;
    int shift = 0;
    while (true) {
      byte b = bytes[at[0]++];
      value |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return value;
      }
      shift += 7;
    }
  }
}
