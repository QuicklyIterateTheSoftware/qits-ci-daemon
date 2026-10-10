package eu.wohlben.qits.cidaemon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.cidaemon.protocol.CiDaemonBinary;
import eu.wohlben.qits.cidaemon.protocol.StepChunk;
import eu.wohlben.qits.cidaemon.protocol.Stream;
import io.vertx.core.Vertx;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The export on its own, against a real HTTP collector ({@link FakeCollector}), decoding what
 * arrives by the schema's field numbers ({@link OtlpDecode}). {@code DaemonMainTest} drives the same
 * export through the whole flow; this pins the parts that need no socket.
 */
class TelemetryTest {

  private static final Map<String, String> RESOURCE =
      Telemetry.resource("run-7", "2", "repo-1", "", "  ", "main", "0123abcd", "qits.example.eu");

  private final Vertx vertx = Vertx.vertx();
  private FakeCollector collector;

  @BeforeEach
  void setUp() throws Exception {
    collector = new FakeCollector(vertx);
  }

  @AfterEach
  void tearDown() throws Exception {
    collector.close();
    vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
  }

  private Telemetry telemetry(String runId, Telemetry.Settings settings) {
    return new Telemetry(
        vertx, collector.endpoint(), "run-t0ken", RESOURCE, runId, settings, Telemetry::nowNanos);
  }

  private static String flush(Telemetry telemetry, String rootError) throws Exception {
    return telemetry
        .flush(rootError)
        .toCompletionStage()
        .toCompletableFuture()
        .get(10, TimeUnit.SECONDS);
  }

  private static LogRecord line(Level level, String message) {
    LogRecord record = new LogRecord(level, message);
    record.setLoggerName("eu.wohlben.qits.cidaemon.DaemonMain");
    return record;
  }

  // --- endpoint resolution ------------------------------------------------------------------------

  @Test
  void theEndpointIsDerivedFromTheControlSocketsCiHostWhenNothingOverridesIt() {
    assertEquals(
        "https://observability.qits.example.eu/observability/api/otel",
        Telemetry.endpoint(null, "wss://ci.qits.example.eu/ci/daemon"));
    assertEquals(
        "http://observability.dev.internal:8080/observability/api/otel",
        Telemetry.endpoint(null, "ws://ci.dev.internal:8080/ci/daemon"));
  }

  @Test
  void aControlSocketWithoutACiHostDerivesNoEndpointAtAll() {
    assertNull(Telemetry.endpoint(null, "ws://127.0.0.1:8080/ci/daemon"));
    assertNull(Telemetry.endpoint(null, "wss://dev-qits-ci:8080/ci/daemon"));
    assertNull(Telemetry.endpoint(null, null));
  }

  @Test
  void anExplicitOverrideWinsOverTheDerivationAndLosesItsTrailingSlash() {
    assertEquals(
        "http://collector.test:4318/otel",
        Telemetry.endpoint("http://collector.test:4318/otel/", "wss://ci.qits.example.eu/ci/daemon"));
  }

  @Test
  void anEmptyOverrideSwitchesTheExportOffEvenWhereOneCouldBeDerived() {
    assertNull(Telemetry.endpoint("", "wss://ci.qits.example.eu/ci/daemon"));
    assertNull(Telemetry.endpoint("   ", "wss://ci.qits.example.eu/ci/daemon"));
  }

  @Test
  void anOverrideThatIsNotAnHttpUrlIsRefusedByName() {
    IllegalArgumentException refused =
        assertThrows(
            IllegalArgumentException.class,
            () -> Telemetry.endpoint("collector:4318", "wss://ci.qits.example.eu/ci/daemon"));
    assertTrue(refused.getMessage().contains("QITS_CI_TELEMETRY_URL"), refused::getMessage);
  }

  // --- encoding -----------------------------------------------------------------------------------

  @Test
  void theResourceNamesTheServiceTheRunAndTheCheckoutAndLeavesOutWhatWasNotHandedOver() {
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("service.name", "qits-ci-daemon");
    expected.put("service.version", CiDaemonBinary.VERSION);
    expected.put("qits.ci.run_id", "run-7");
    expected.put("qits.ci.step_index", "2");
    expected.put("qits.ci.repo_id", "repo-1");
    expected.put("vcs.ref.head.name", "main");
    expected.put("vcs.ref.head.revision", "0123abcd");
    expected.put("qits.domain", "qits.example.eu");
    assertEquals(expected, RESOURCE);
  }

  @Test
  void logsAndSpansAreEncodedAsOtlpWithTheResourceTheBearerAndOneTraceId() throws Exception {
    Telemetry telemetry = telemetry("run-7", Telemetry.Settings.defaults(200, 5_000));
    telemetry.start();
    Spans.Span dial = telemetry.span("dial");
    telemetry.publish(line(Level.INFO, "ci-daemon registered as daemon-1"));
    dial.end();
    Spans.Span script = telemetry.span("script");
    telemetry.output(new StepChunk("c1", 0, Stream.OUT, "built\n"));
    script.attribute("qits.ci.exit_code", 3L).attribute("qits.ci.timed_out", false);
    script.end("the script exited 3");

    String outcome = flush(telemetry, "the script exited 3");

    assertEquals("telemetry: submitted 2 logs, 3 spans (0 dropped)", outcome);
    assertEquals(
        List.of("Bearer run-t0ken", "Bearer run-t0ken"),
        collector.requests.stream().map(FakeCollector.Request::authorization).toList());
    String traceId = HexFormat.of().formatHex(sha256Prefix("run-7"));

    OtlpDecode.Logs logs = OtlpDecode.logs(collector.to("/otel/v1/logs").getFirst().body());
    assertEquals(new LinkedHashMap<String, Object>(RESOURCE), logs.resource());
    assertEquals(Telemetry.SCOPE, logs.scopeName());
    assertEquals(CiDaemonBinary.VERSION, logs.scopeVersion());
    OtlpDecode.Line own = logs.lines().getFirst();
    assertEquals("ci-daemon registered as daemon-1", own.body());
    assertEquals(9, own.severityNumber());
    assertEquals(traceId, own.traceId());
    assertEquals(HexFormat.of().formatHex(dial.id()), own.spanId(), "the span it was logged in");
    OtlpDecode.Line output = logs.lines().get(1);
    assertEquals("built", output.body());
    assertEquals(Map.of(Telemetry.STREAM_ATTRIBUTE, "out"), output.attributes());
    assertEquals(HexFormat.of().formatHex(script.id()), output.spanId());

    OtlpDecode.Traces traces = OtlpDecode.traces(collector.to("/otel/v1/traces").getFirst().body());
    assertEquals(new LinkedHashMap<String, Object>(RESOURCE), traces.resource());
    List<OtlpDecode.Span> spans = traces.spans();
    assertEquals(List.of(Spans.ROOT, "dial", "script"), spans.stream().map(OtlpDecode.Span::name).toList());
    OtlpDecode.Span root = spans.getFirst();
    assertEquals("", root.parentSpanId());
    assertEquals(Otlp.STATUS_ERROR, root.statusCode());
    assertEquals("the script exited 3", root.statusMessage());
    for (OtlpDecode.Span span : spans) {
      assertEquals(traceId, span.traceId());
      assertEquals(Otlp.KIND_INTERNAL, span.kind());
      assertTrue(span.endTimeUnixNano() >= span.startTimeUnixNano(), span::toString);
      if (span != root) {
        assertEquals(root.spanId(), span.parentSpanId());
      }
    }
    assertEquals(Otlp.STATUS_OK, spans.get(1).statusCode());
    OtlpDecode.Span scriptSpan = spans.get(2);
    assertEquals(Map.of("qits.ci.exit_code", 3L, "qits.ci.timed_out", false), scriptSpan.attributes());
    assertEquals(Otlp.STATUS_ERROR, scriptSpan.statusCode());
    assertEquals("the script exited 3", scriptSpan.statusMessage());
  }

  @Test
  void twoStepsOfOneRunShareATraceIdAndOneWithoutARunIdGetsItsOwn() throws Exception {
    for (String step : List.of("1", "2")) {
      Telemetry telemetry =
          new Telemetry(
              vertx,
              collector.endpoint(),
              "t",
              Telemetry.resource("run-42", step, null, null, null, "main", "abc", null),
              "run-42",
              Telemetry.Settings.defaults(200, 5_000),
              Telemetry::nowNanos);
      telemetry.start();
      flush(telemetry, null);
    }
    Telemetry anonymous = telemetry(null, Telemetry.Settings.defaults(200, 5_000));
    anonymous.start();
    flush(anonymous, null);

    List<FakeCollector.Request> traces = collector.to(Telemetry.TRACES_PATH);
    assertEquals(3, traces.size());
    List<String> traceIds =
        traces.stream()
            .map(r -> OtlpDecode.traces(r.body()).spans().getFirst().traceId())
            .toList();
    String runTrace = HexFormat.of().formatHex(sha256Prefix("run-42"));
    assertEquals(List.of(runTrace, runTrace), traceIds.subList(0, 2));
    assertFalse(traceIds.get(2).equals(runTrace));
    assertEquals(32, traceIds.get(2).length());
    assertEquals(
        List.of("1", "2"),
        traces.subList(0, 2).stream()
            .map(r -> OtlpDecode.traces(r.body()).resource().get("qits.ci.step_index"))
            .toList());
  }

  // --- the step's output --------------------------------------------------------------------------

  @Test
  void chunksAreSplitIntoLinesAndAPartialLineIsCarriedIntoTheNextChunkOfItsStream() {
    StepOutput output = new StepOutput(200);
    output.accept(Stream.OUT, "one\ntw", 1, null, null);
    output.accept(Stream.ERR, "err-a", 2, null, null);
    output.accept(Stream.OUT, "o\r\nthr", 3, null, null);
    output.accept(Stream.ERR, "\n", 4, null, null);
    output.accept(Stream.OUT, "ee", 5, null, null);

    List<StepOutput.Line> lines = output.drain(6);

    assertEquals(
        List.of("OUT one", "OUT two", "ERR err-a", "OUT three"),
        lines.stream().map(l -> l.stream() + " " + l.text()).toList());
    assertEquals(4, output.total());
    assertEquals(0, output.notExported());
  }

  @Test
  void aLineLongerThanTheBodyCapKeepsItsHead() {
    StepOutput output = new StepOutput(10);
    String huge = "h".repeat(Telemetry.MAX_BODY + 100);
    output.accept(Stream.OUT, huge.substring(0, 10_000), 1, null, null);
    output.accept(Stream.OUT, huge.substring(10_000) + "\nnext\n", 2, null, null);

    List<StepOutput.Line> lines = output.drain(3);

    assertEquals(2, lines.size());
    assertEquals("h".repeat(Telemetry.MAX_BODY) + "…", lines.getFirst().text());
    assertEquals("next", lines.get(1).text());
  }

  @Test
  void theOutputCapKeepsTheLatestLinesAndSaysHowManyItKeptOut() throws Exception {
    Telemetry telemetry = telemetry("run-7", Telemetry.Settings.defaults(200, 5_000));
    telemetry.start();
    telemetry.span("script");
    StringBuilder chunk = new StringBuilder();
    for (int i = 1; i <= 1000; i++) {
      chunk.append("line ").append(i).append('\n');
      if (i % 37 == 0) {
        telemetry.output(new StepChunk("c1", i, Stream.OUT, chunk.toString()));
        chunk.setLength(0);
      }
    }
    telemetry.output(new StepChunk("c1", 1001, Stream.OUT, chunk.toString()));
    telemetry.publish(line(Level.INFO, "the daemon's own"));

    flush(telemetry, null);

    List<String> bodies = collector.lines().stream().map(OtlpDecode.Line::body).toList();
    List<String> expected = new ArrayList<>();
    expected.add("the daemon's own");
    for (int i = 801; i <= 1000; i++) {
      expected.add("line " + i);
    }
    expected.add("step output: 1000 lines, 800 not exported");
    assertEquals(expected, bodies);
  }

  // --- the queue and the flush --------------------------------------------------------------------

  @Test
  void aFullQueueDropsItsOldestLinesAndSaysHowMany() throws Exception {
    Telemetry telemetry = telemetry("run-7", new Telemetry.Settings(3, 50, 60_000, 200, 5_000));
    telemetry.start();
    for (int i = 0; i < 5; i++) {
      telemetry.publish(line(Level.INFO, "line " + i));
    }

    String outcome = flush(telemetry, null);

    assertEquals(
        List.of(
            "ci-daemon telemetry dropped 2 log line(s): the queue was full",
            "line 2",
            "line 3",
            "line 4"),
        collector.lines().stream().map(OtlpDecode.Line::body).toList());
    assertEquals("telemetry: submitted 4 logs, 1 spans (2 dropped)", outcome);
  }

  @Test
  void debugLinesAreNotShipped() throws Exception {
    Telemetry telemetry = telemetry("run-7", Telemetry.Settings.defaults(200, 5_000));
    telemetry.start();
    telemetry.publish(line(Level.FINE, "chatter"));
    telemetry.publish(line(Level.INFO, "kept"));
    flush(telemetry, null);
    assertEquals(List.of("kept"), collector.lines().stream().map(OtlpDecode.Line::body).toList());
  }

  @Test
  void theTimerShipsTheDaemonsOwnLinesWhileTheStepRuns() throws Exception {
    Telemetry telemetry = telemetry("run-7", new Telemetry.Settings(100, 50, 50, 200, 5_000));
    telemetry.start();
    telemetry.publish(line(Level.INFO, "by the timer"));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (collector.to(Telemetry.LOGS_PATH).isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(
        List.of("by the timer"), collector.lines().stream().map(OtlpDecode.Line::body).toList());
    assertTrue(collector.to(Telemetry.TRACES_PATH).isEmpty(), "spans go only at the flush");
    telemetry.shutdown();
  }

  @Test
  void aFlushRunsOnceAndEveryLaterCallGetsTheFirstOutcome() throws Exception {
    Telemetry telemetry = telemetry("run-7", Telemetry.Settings.defaults(200, 5_000));
    telemetry.start();
    String first = flush(telemetry, null);
    telemetry.publish(line(Level.INFO, "after the flush"));
    String second = flush(telemetry, "a later ending");

    assertEquals(first, second);
    assertEquals(first, telemetry.outcome());
    assertEquals(1, collector.to(Telemetry.TRACES_PATH).size());
    assertEquals(
        Otlp.STATUS_OK, collector.spans().getFirst().statusCode(), "the first flush decided it");
  }

  @Test
  void aRefusingCollectorIsReportedAsNotSubmittedRatherThanThrown() throws Exception {
    collector.status = 503;
    Telemetry telemetry = telemetry("run-7", Telemetry.Settings.defaults(200, 5_000));
    telemetry.start();
    telemetry.publish(line(Level.INFO, "refused"));
    assertEquals("telemetry: NOT submitted (the collector answered 503)", flush(telemetry, null));
  }

  @Test
  void aCollectorThatNeverAnswersIsCutOffAtTheFlushBound() throws Exception {
    collector.silent = true;
    Telemetry telemetry = telemetry("run-7", Telemetry.Settings.defaults(200, 300));
    telemetry.start();
    telemetry.publish(line(Level.INFO, "unheard"));

    long startedAt = System.nanoTime();
    String outcome = flush(telemetry, null);
    long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000L;

    assertEquals("telemetry: NOT submitted (timeout after 300 ms)", outcome);
    assertTrue(elapsedMillis >= 250 && elapsedMillis < 3_000, () -> elapsedMillis + "ms");
  }

  @Test
  void anExportThatNeverStartedSendsNothingAndOffSaysSo() throws Exception {
    Telemetry unstarted = telemetry("run-7", Telemetry.Settings.defaults(200, 300));
    unstarted.publish(line(Level.SEVERE, "misconfigured"));
    assertTrue(flush(unstarted, null).startsWith("telemetry: NOT submitted"));

    Telemetry off = Telemetry.off();
    assertFalse(off.enabled());
    off.start();
    off.publish(line(Level.SEVERE, "nowhere"));
    assertEquals("telemetry: off", flush(off, null));
    assertEquals("telemetry: off", off.outcome());
    assertEquals(List.of(), collector.requests);
  }

  @Test
  void aChildStillOpenAtTheFlushEndsAsAnError() throws Exception {
    Telemetry telemetry = telemetry("run-7", Telemetry.Settings.defaults(200, 5_000));
    telemetry.start();
    telemetry.span("script");
    flush(telemetry, "socket lost");

    OtlpDecode.Span script = collector.spans().get(1);
    assertEquals("script", script.name());
    assertEquals(Otlp.STATUS_ERROR, script.statusCode());
    assertEquals(Spans.UNFINISHED, script.statusMessage());
  }

  private static byte[] sha256Prefix(String runId) throws Exception {
    return Arrays.copyOf(
        MessageDigest.getInstance("SHA-256").digest(runId.getBytes(StandardCharsets.UTF_8)), 16);
  }
}
