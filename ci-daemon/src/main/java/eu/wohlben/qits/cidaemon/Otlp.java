package eu.wohlben.qits.cidaemon;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * OTLP's {@code ExportLogsServiceRequest} and {@code ExportTraceServiceRequest}, written as protobuf
 * by hand: the fields a log line and a span need of {@code opentelemetry/proto}, and nothing else.
 *
 * <p><b>Why not the OpenTelemetry SDK, or {@code quarkus-opentelemetry}.</b> qits-observability
 * takes {@code http/protobuf} and nothing else, so the choice was a protobuf runtime plus the SDK's
 * exporter stack — generated classes, reflection configuration and megabytes in a static musl
 * binary whose every dependency is a decision — or the encoder below. Copied and adapted from
 * qits-ci-runner-daemon's {@code OtlpLogs}, which took the same decision first and whose encoding
 * qits-observability's receiver has answered record for record; lifting the two into a shared
 * library is deliberately not done here. The wire format is stable by construction (protobuf field
 * numbers never move), and the suite decodes what this writes by the schema's field numbers ({@code
 * OtlpDecode}), not by these constants.
 *
 * <p>Nothing here reflects, and nothing is generated: it is safe in the native image as written.
 */
final class Otlp {

  private Otlp() {}

  /** One log record. {@code traceId}/{@code spanId} may be null: then the record names no span. */
  record LogEntry(
      long epochNanos,
      int severityNumber,
      String severityText,
      String body,
      Map<String, Object> attributes,
      byte[] traceId,
      byte[] spanId) {}

  /** One finished span. {@code parentSpanId} is null for the root. */
  record SpanData(
      byte[] traceId,
      byte[] spanId,
      byte[] parentSpanId,
      String name,
      int kind,
      long startEpochNanos,
      long endEpochNanos,
      Map<String, Object> attributes,
      int statusCode,
      String statusMessage) {}

  /** {@code Span.SpanKind.SPAN_KIND_INTERNAL}. */
  static final int KIND_INTERNAL = 1;

  /** {@code Status.StatusCode}: unset, ok, error. */
  static final int STATUS_UNSET = 0;

  static final int STATUS_OK = 1;
  static final int STATUS_ERROR = 2;

  // Field numbers, from opentelemetry-proto (collector/*/v1, logs/v1, trace/v1, common/v1,
  // resource/v1). Wire types: 0 varint, 1 fixed64, 2 length-delimited.
  private static final int REQUEST_RESOURCE_ENTRIES = 1; // resource_logs / resource_spans
  private static final int RESOURCE_ENTRY_RESOURCE = 1;
  private static final int RESOURCE_ENTRY_SCOPE_ENTRIES = 2; // scope_logs / scope_spans
  private static final int RESOURCE_ATTRIBUTES = 1;
  private static final int SCOPE_ENTRY_SCOPE = 1;
  private static final int SCOPE_ENTRY_ITEMS = 2; // log_records / spans
  private static final int SCOPE_NAME = 1;
  private static final int SCOPE_VERSION = 2;

  private static final int RECORD_TIME_UNIX_NANO = 1;
  private static final int RECORD_SEVERITY_NUMBER = 2;
  private static final int RECORD_SEVERITY_TEXT = 3;
  private static final int RECORD_BODY = 5;
  private static final int RECORD_ATTRIBUTES = 6;
  private static final int RECORD_TRACE_ID = 9;
  private static final int RECORD_SPAN_ID = 10;
  private static final int RECORD_OBSERVED_TIME_UNIX_NANO = 11;

  private static final int SPAN_TRACE_ID = 1;
  private static final int SPAN_SPAN_ID = 2;
  private static final int SPAN_PARENT_SPAN_ID = 4;
  private static final int SPAN_NAME = 5;
  private static final int SPAN_KIND = 6;
  private static final int SPAN_START_TIME_UNIX_NANO = 7;
  private static final int SPAN_END_TIME_UNIX_NANO = 8;
  private static final int SPAN_ATTRIBUTES = 9;
  private static final int SPAN_STATUS = 15;
  private static final int STATUS_MESSAGE = 2; // field 1 is reserved
  private static final int STATUS_CODE = 3;

  private static final int KEY_VALUE_KEY = 1;
  private static final int KEY_VALUE_VALUE = 2;
  private static final int ANY_VALUE_STRING = 1;
  private static final int ANY_VALUE_BOOL = 2;
  private static final int ANY_VALUE_INT = 3;

  /** The OTel semantic-convention attribute a record's logger name travels in. */
  static final String LOGGER_ATTRIBUTE = "code.namespace";

  /** One logs request: one resource, one scope, every entry. */
  static byte[] encodeLogs(
      Map<String, String> resource, String scopeName, String scopeVersion, List<LogEntry> entries) {
    Proto scopeLogs = new Proto().message(SCOPE_ENTRY_SCOPE, scope(scopeName, scopeVersion));
    for (LogEntry e : entries) {
      Proto record =
          new Proto()
              .fixed64(RECORD_TIME_UNIX_NANO, e.epochNanos())
              .varint(RECORD_SEVERITY_NUMBER, e.severityNumber())
              .string(RECORD_SEVERITY_TEXT, e.severityText())
              .message(RECORD_BODY, new Proto().string(ANY_VALUE_STRING, e.body()));
      attributes(record, RECORD_ATTRIBUTES, e.attributes());
      if (e.traceId() != null && e.spanId() != null) {
        record.bytes(RECORD_TRACE_ID, e.traceId()).bytes(RECORD_SPAN_ID, e.spanId());
      }
      record.fixed64(RECORD_OBSERVED_TIME_UNIX_NANO, e.epochNanos());
      scopeLogs.message(SCOPE_ENTRY_ITEMS, record);
    }
    return request(resource, scopeLogs);
  }

  /** One traces request: one resource, one scope, every span. */
  static byte[] encodeSpans(
      Map<String, String> resource, String scopeName, String scopeVersion, List<SpanData> spans) {
    Proto scopeSpans = new Proto().message(SCOPE_ENTRY_SCOPE, scope(scopeName, scopeVersion));
    for (SpanData s : spans) {
      Proto span = new Proto().bytes(SPAN_TRACE_ID, s.traceId()).bytes(SPAN_SPAN_ID, s.spanId());
      if (s.parentSpanId() != null) {
        span.bytes(SPAN_PARENT_SPAN_ID, s.parentSpanId());
      }
      span.string(SPAN_NAME, s.name())
          .varint(SPAN_KIND, s.kind())
          .fixed64(SPAN_START_TIME_UNIX_NANO, s.startEpochNanos())
          .fixed64(SPAN_END_TIME_UNIX_NANO, s.endEpochNanos());
      attributes(span, SPAN_ATTRIBUTES, s.attributes());
      if (s.statusCode() != STATUS_UNSET) {
        Proto status = new Proto();
        if (s.statusMessage() != null && !s.statusMessage().isEmpty()) {
          status.string(STATUS_MESSAGE, s.statusMessage());
        }
        span.message(SPAN_STATUS, status.varint(STATUS_CODE, s.statusCode()));
      }
      scopeSpans.message(SCOPE_ENTRY_ITEMS, span);
    }
    return request(resource, scopeSpans);
  }

  /**
   * The OTel severity number for a {@code java.util.logging} level's value — JBoss' levels included
   * (FATAL 1100, ERROR 1000, WARN 900, INFO 800, DEBUG 500, TRACE 400).
   */
  static int severityNumber(int level) {
    if (level >= 1100) {
      return 21; // FATAL
    }
    if (level >= 1000) {
      return 17; // ERROR
    }
    if (level >= 900) {
      return 13; // WARN
    }
    if (level >= 800) {
      return 9; // INFO
    }
    if (level >= 500) {
      return 5; // DEBUG
    }
    return 1; // TRACE
  }

  static String severityText(int severityNumber) {
    return switch (severityNumber) {
      case 21 -> "FATAL";
      case 17 -> "ERROR";
      case 13 -> "WARN";
      case 9 -> "INFO";
      case 5 -> "DEBUG";
      default -> "TRACE";
    };
  }

  private static byte[] request(Map<String, String> resource, Proto scopeEntry) {
    Proto res = new Proto();
    resource.forEach((k, v) -> res.message(RESOURCE_ATTRIBUTES, keyValue(k, v)));
    Proto resourceEntry =
        new Proto()
            .message(RESOURCE_ENTRY_RESOURCE, res)
            .message(RESOURCE_ENTRY_SCOPE_ENTRIES, scopeEntry);
    return new Proto().message(REQUEST_RESOURCE_ENTRIES, resourceEntry).bytes();
  }

  private static Proto scope(String name, String version) {
    return new Proto().string(SCOPE_NAME, name).string(SCOPE_VERSION, version);
  }

  private static void attributes(Proto into, int field, Map<String, Object> attributes) {
    if (attributes != null) {
      attributes.forEach((k, v) -> into.message(field, keyValue(k, v)));
    }
  }

  /** Strings, booleans and integers — the three value kinds anything here carries. */
  private static Proto keyValue(String key, Object value) {
    Proto any =
        switch (value) {
          case Boolean b -> new Proto().varint(ANY_VALUE_BOOL, b ? 1 : 0);
          case Integer i -> new Proto().varint(ANY_VALUE_INT, i.longValue());
          case Long l -> new Proto().varint(ANY_VALUE_INT, l);
          case null -> new Proto().string(ANY_VALUE_STRING, "");
          default -> new Proto().string(ANY_VALUE_STRING, value.toString());
        };
    return new Proto().string(KEY_VALUE_KEY, key).message(KEY_VALUE_VALUE, any);
  }

  /** A message under construction. Empty strings and zeros are written anyway; proto3 allows both. */
  private static final class Proto {
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    /** Two's complement for a negative int64, ten bytes — exactly what protobuf's int64 is. */
    Proto varint(int field, long value) {
      tag(field, 0);
      raw(value);
      return this;
    }

    Proto fixed64(int field, long value) {
      tag(field, 1);
      for (int i = 0; i < 8; i++) {
        out.write((int) (value >>> (8 * i)) & 0xFF);
      }
      return this;
    }

    Proto string(int field, String value) {
      return bytes(field, (value == null ? "" : value).getBytes(StandardCharsets.UTF_8));
    }

    Proto message(int field, Proto message) {
      return bytes(field, message.bytes());
    }

    byte[] bytes() {
      return out.toByteArray();
    }

    Proto bytes(int field, byte[] value) {
      tag(field, 2);
      raw(value.length);
      out.writeBytes(value);
      return this;
    }

    private void tag(int field, int wireType) {
      raw(((long) field << 3) | wireType);
    }

    private void raw(long value) {
      while ((value & ~0x7FL) != 0) {
        out.write((int) ((value & 0x7F) | 0x80));
        value >>>= 7;
      }
      out.write((int) value);
    }
  }
}
