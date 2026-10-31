package eu.wohlben.qits.cidaemon;

import eu.wohlben.qits.cidaemon.protocol.CiDaemonBinary;
import eu.wohlben.qits.cidaemon.protocol.StepChunk;
import eu.wohlben.qits.cidaemon.protocol.Stream;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jboss.logging.Logger;
import org.jboss.logmanager.ExtLogRecord;

/**
 * The daemon's own log, its step's output and its step's spans, shipped to qits-observability as
 * OTLP/HTTP protobuf — the only way a step's work can be found in {@code qits observe}.
 *
 * <p><b>Through the public edge, with the run's token.</b> A step runs on a runner outside the
 * swarm, so the platform's internal aliases are not reachable from here; the route is {@code
 * observability.<domain>} — derived from the control socket's {@code ci.<domain>} by {@link
 * #endpoint} — and the bearer is {@code $QITS_TOKEN}, which the edge admits like every platform
 * token and which lives exactly as long as the run.
 *
 * <p><b>It never gets in the log's way.</b> {@link #publish} is a bounded queue's {@code offer} and
 * nothing else: no I/O on the logging thread, and a full queue drops its <em>oldest</em> line, whose
 * count is itself shipped as a line. A timer sends a batch at a time, one request in flight, and a
 * batch that fails is dropped rather than retried. The step's output never touches that queue: it is
 * kept apart ({@link StepOutput}, capped at its latest lines) and emitted only at the flush, so a
 * chatty script cannot displace one line of the daemon's own.
 *
 * <p><b>The flush is the point.</b> qits-ci {@code docker rm -f}s the container the moment the
 * terminal frame arrives, so anything sent after it races a SIGKILL — and the end of a step is
 * exactly what such a batch would hold. {@link DaemonMain} therefore calls {@link #flush} <em>before
 * </em> it transmits {@code StepFinished} or {@code InitFailed}, and waits for it. The flush ends the
 * spans, drains both buffers, sends the final requests and completes when they are answered — or
 * when {@link Settings#flushMillis()} runs out, whichever is first. It never fails, never blocks a
 * thread (it is a future, bounded by a Vert.x timer, so it can be awaited from the event loop its
 * own requests need), and runs once: every later call returns the first one's outcome.
 *
 * <p>Copied and adapted from qits-ci-runner-daemon's {@code Telemetry}; see {@link Otlp} for why
 * the encoding is hand-written and the copy is not a shared library.
 */
public final class Telemetry extends Handler {

  private static final Logger LOG = Logger.getLogger(Telemetry.class);

  /** The OTLP/HTTP paths, appended to the endpoint as every OTLP exporter does. */
  static final String LOGS_PATH = "/v1/logs";

  static final String TRACES_PATH = "/v1/traces";

  /** Where qits-observability's OTLP receiver lives below its host — its own root path. */
  static final String TELEMETRY_PATH = "/observability/api/otel";

  /** A line longer than this is cut, keeping its head: a stack trace's first frames name it. */
  static final int MAX_BODY = 16 * 1024;

  /** The one bucket every step's telemetry lands in. */
  public static final String SERVICE_NAME = "qits-ci-daemon";

  /** The OTLP instrumentation scope — this package. */
  static final String SCOPE = "eu.wohlben.qits.cidaemon";

  /** The attribute naming which of the script's streams an output line came from. */
  static final String STREAM_ATTRIBUTE = "qits.ci.stream";

  /** What {@link #outcome()} says when nothing is exported. */
  static final String OFF = "telemetry: off";

  /** A ceiling on any one request; the flush's own bound is usually what ends a stuck one first. */
  static final long HTTP_TIMEOUT_MILLIS = 10_000;

  /** How much is queued, how often and how much of it is sent, and how the end is bounded. */
  public record Settings(
      int capacity, int batch, long intervalMillis, int outputMaxLines, long flushMillis) {
    public static Settings defaults(int outputMaxLines, long flushMillis) {
      return new Settings(4096, 512, 5_000, outputMaxLines, flushMillis);
    }
  }

  private final Vertx vertx;
  private final Http http;
  private final String endpoint;
  /** Swapped whole by {@link #continueAs}; every request reads the current one. */
  private volatile Map<String, String> headers;

  private volatile Map<String, String> resource;
  private final Settings settings;
  private final LongSupplier clock;
  private final Spans spans;
  private final StepOutput output;

  private final ArrayBlockingQueue<Otlp.LogEntry> queue;
  private final AtomicLong dropped = new AtomicLong();
  private final AtomicLong lost = new AtomicLong();
  private final AtomicLong submittedLogs = new AtomicLong();
  private final AtomicBoolean failing = new AtomicBoolean();
  private final SimpleFormatter julFormatter = new SimpleFormatter();

  // Guarded by this.
  private boolean started;
  private boolean inFlight;
  private Future<Void> current = Future.succeededFuture();
  private long timer = -1;

  /** Set once, by the first {@link #flush}; read without the lock by {@link #publish}. */
  private volatile Future<String> flushed;

  /**
   * @param endpoint the OTLP endpoint, the paths appended to it; null exports nothing
   * @param token the bearer every request carries — the run's own {@code QITS_TOKEN}
   * @param runId the run whose steps share one trace ({@link Spans}); may be blank
   * @param clock epoch nanoseconds
   */
  public Telemetry(
      Vertx vertx,
      String endpoint,
      String token,
      Map<String, String> resource,
      String runId,
      Settings settings,
      LongSupplier clock) {
    this.vertx = vertx;
    this.endpoint = endpoint;
    this.http = endpoint == null ? null : new Http(vertx, HTTP_TIMEOUT_MILLIS);
    this.headers =
        Map.of(
            "Content-Type", "application/x-protobuf",
            "Authorization", "Bearer " + (token == null ? "" : token));
    this.resource = Map.copyOf(resource);
    this.settings = settings;
    this.clock = clock;
    this.spans = new Spans(runId, clock);
    this.output = new StepOutput(settings.outputMaxLines());
    this.queue = new ArrayBlockingQueue<>(Math.max(1, settings.capacity()));
    setLevel(Level.INFO);
  }

  /**
   * The lease now serves the next run of its release request (qits-732): every later request
   * carries {@code token} — the first run's is deleted when that run closes, so the final flush
   * would otherwise be refused — and is labelled with the new run's id, branch and sha. The trace
   * stays the lease's: its root span covers both runs and its id was seeded from the first run's.
   * A label of the first run's that is already queued is sent under the new one, which costs a
   * line's attribution and never a line.
   */
  public void continueAs(String token, String runId, String branch, String sha) {
    headers =
        Map.of(
            "Content-Type", "application/x-protobuf",
            "Authorization", "Bearer " + (token == null ? "" : token));
    Map<String, String> next = new LinkedHashMap<>(resource);
    replace(next, "qits.ci.run_id", runId);
    replace(next, "vcs.ref.head.name", branch);
    replace(next, "vcs.ref.head.revision", sha);
    resource = Map.copyOf(next);
  }

  private static void replace(Map<String, String> resource, String key, String value) {
    if (value == null || value.isBlank()) {
      resource.remove(key);
    } else {
      resource.put(key, value.trim());
    }
  }

  /**
   * Ships nothing. Spans are still kept — they cost a few objects — so the flow never branches on
   * whether telemetry is on.
   */
  public static Telemetry off() {
    return new Telemetry(
        null, null, null, Map.of(), null, Settings.defaults(0, 0), Telemetry::nowNanos);
  }

  /** Epoch nanoseconds, at whatever resolution the clock has. */
  public static long nowNanos() {
    Instant now = Instant.now();
    return now.getEpochSecond() * 1_000_000_000L + now.getNano();
  }

  /** A control-socket url whose host is the edge's {@code ci.} application name, and the rest. */
  private static final Pattern CI_HOST =
      Pattern.compile("(?i)(wss?|https?)://ci\\.([^/?#\\s]+)(?:[/?#]\\S*)?");

  /**
   * The OTLP endpoint, or null for none.
   *
   * <p>{@code raw} is {@code QITS_CI_TELEMETRY_URL}. Set, it is the endpoint; set but blank, the
   * export is off — which is why {@link Main} reads that one variable raw, since SmallRye cannot
   * tell empty from unset. Unset ({@code null}), it is derived from the control socket's url the way
   * the edge names every application: {@code wss://ci.<rest>/ci/daemon} → {@code
   * https://observability.<rest>/observability/api/otel}, and {@code ws://} → {@code http://}. A
   * daemon url whose host does not start with {@code ci.} has no such sibling and derives nothing.
   *
   * @throws IllegalArgumentException when {@code raw} is set to something that is not an http(s) url
   */
  public static String endpoint(String raw, String daemonUrl) {
    if (raw != null) {
      if (raw.isBlank()) {
        return null;
      }
      String url = raw.trim().replaceAll("/+$", "");
      if (!url.matches("(?i)https?://[^/\\s]+(/\\S*)?")) {
        throw new IllegalArgumentException(
            "QITS_CI_TELEMETRY_URL is not an http(s) url: '" + url + "'");
      }
      return url;
    }
    Matcher ci = CI_HOST.matcher(daemonUrl == null ? "" : daemonUrl.trim());
    if (!ci.matches()) {
      return null;
    }
    String scheme = ci.group(1).toLowerCase(java.util.Locale.ROOT);
    boolean secure = scheme.equals("wss") || scheme.equals("https");
    return (secure ? "https://" : "http://") + "observability." + ci.group(2) + TELEMETRY_PATH;
  }

  /**
   * Who is speaking, on every request: one bucket, {@link #SERVICE_NAME}, told apart by the run, the
   * step and the repository. Labels only — the daemon reads these values for nothing else — and every
   * one is optional: a blank value is left out, never a reason not to run.
   */
  public static Map<String, String> resource(
      String runId,
      String stepIndex,
      String repoId,
      String repoName,
      String projectId,
      String branch,
      String sha,
      String domain) {
    Map<String, String> resource = new LinkedHashMap<>();
    resource.put("service.name", SERVICE_NAME);
    resource.put("service.version", CiDaemonBinary.VERSION);
    put(resource, "qits.ci.run_id", runId);
    put(resource, "qits.ci.step_index", stepIndex);
    put(resource, "qits.ci.repo_id", repoId);
    put(resource, "qits.ci.repo_name", repoName);
    put(resource, "qits.ci.project_id", projectId);
    put(resource, "vcs.ref.head.name", branch);
    put(resource, "vcs.ref.head.revision", sha);
    put(resource, "qits.domain", domain);
    return resource;
  }

  private static void put(Map<String, String> resource, String key, String value) {
    if (value != null && !value.isBlank()) {
      resource.put(key, value.trim());
    }
  }

  public boolean enabled() {
    return endpoint != null;
  }

  String logsUrl() {
    return endpoint == null ? null : endpoint + LOGS_PATH;
  }

  String tracesUrl() {
    return endpoint == null ? null : endpoint + TRACES_PATH;
  }

  Spans spans() {
    return spans;
  }

  /** Start a child span of the step's root. */
  Spans.Span span(String name) {
    return spans.start(name);
  }

  /**
   * Begin sending the daemon's own lines on the timer. Called once the environment is known to be
   * complete: a misconfigured daemon exports nothing, because the address may be what is missing.
   */
  public synchronized void start() {
    if (!enabled() || started) {
      return;
    }
    started = true;
    timer = vertx.setPeriodic(settings.intervalMillis(), id -> sendQueued());
    LOG.infof("ci-daemon ships its telemetry to %s", endpoint);
  }

  /** A chunk of the step's output, split into lines and kept for the flush. */
  void output(StepChunk chunk) {
    if (!enabled() || flushed != null) {
      return;
    }
    Spans.Span span = spans.current();
    output.accept(
        chunk.stream(), chunk.text(), clock.getAsLong(), spans.traceId(), span.id());
  }

  @Override
  public void publish(LogRecord record) {
    if (!enabled() || flushed != null || record == null || !isLoggable(record)) {
      return;
    }
    int severity = Otlp.severityNumber(record.getLevel().intValue());
    String logger = record.getLoggerName();
    Otlp.LogEntry entry =
        new Otlp.LogEntry(
            record.getInstant().getEpochSecond() * 1_000_000_000L + record.getInstant().getNano(),
            severity,
            Otlp.severityText(severity),
            body(record),
            logger == null || logger.isEmpty() ? Map.of() : Map.of(Otlp.LOGGER_ATTRIBUTE, logger),
            spans.traceId(),
            spans.current().id());
    while (!queue.offer(entry)) {
      if (queue.poll() != null) {
        dropped.incrementAndGet();
        lost.incrementAndGet();
      }
    }
  }

  /** The timer's send: one batch, if none is in flight and the flush has not begun. */
  void sendQueued() {
    List<Otlp.LogEntry> batch;
    Promise<Void> done;
    synchronized (this) {
      if (flushed != null || inFlight) {
        return;
      }
      batch = takeBatch(settings.batch());
      if (batch.isEmpty()) {
        return;
      }
      inFlight = true;
      done = Promise.promise();
      current = done.future();
    }
    postLogs(batch)
        .onComplete(
            answered -> {
              String problem = answered.result();
              if (problem != null) {
                lost.addAndGet(batch.size());
              }
              report(problem, batch.size());
              boolean more;
              synchronized (this) {
                inFlight = false;
                more = problem == null && queue.size() >= settings.batch();
              }
              done.complete();
              if (more) {
                vertx.runOnContext(v -> sendQueued());
              }
            });
  }

  /**
   * End the spans, send everything still held, and complete with the one line that says how that
   * went — at the latest after {@link Settings#flushMillis()}. Runs once; every later call, the
   * shutdown hook's included, returns the first call's future. Never fails.
   *
   * @param rootError why the step went wrong, which makes the root span ERROR; null for OK
   */
  public Future<String> flush(String rootError) {
    Future<Void> previous;
    List<Otlp.SpanData> spanData;
    Promise<String> outcome;
    synchronized (this) {
      if (flushed != null) {
        return flushed;
      }
      if (!enabled()) {
        flushed = Future.succeededFuture(OFF);
        return flushed;
      }
      if (!started) {
        flushed = Future.succeededFuture("telemetry: NOT submitted (the export never started)");
        return flushed;
      }
      if (timer >= 0) {
        vertx.cancelTimer(timer);
        timer = -1;
      }
      spanData = spans.finish(rootError);
      previous = current;
      outcome = Promise.promise();
      flushed = outcome.future();
    }
    long bound = Math.max(1, settings.flushMillis());
    long boundTimer =
        vertx.setTimer(
            bound,
            id ->
                outcome.tryComplete(
                    "telemetry: NOT submitted (timeout after " + settings.flushMillis() + " ms)"));
    // After the batch the timer may have in flight, so the final one never races it.
    previous
        .transform(ignored -> sendFinal(spanData))
        .onComplete(
            sent -> {
              vertx.cancelTimer(boundTimer);
              outcome.tryComplete(
                  sent.succeeded()
                      ? sent.result()
                      : "telemetry: NOT submitted (" + sent.cause().getMessage() + ")");
            });
    return outcome.future();
  }

  /**
   * The line the daemon prints last before it exits: what the flush did, or that there was none.
   */
  public String outcome() {
    Future<String> f = flushed;
    if (f != null && f.succeeded()) {
      return f.result();
    }
    return enabled() ? "telemetry: NOT submitted (no flush ran)" : OFF;
  }

  /**
   * A JVM shutdown hook running the same bounded flush, if none has run: a SIGTERM or SIGINT (a
   * manual {@code docker stop}, a Ctrl-C) still gets the step's telemetry out. It is not the
   * mechanism — the daemon's own endings flush before they exit, and {@code docker rm -f}, which is
   * how a step container is actually removed, is a SIGKILL no hook sees. Best effort in a second
   * sense too: Quarkus closes Vert.x from its own hook, concurrently, and a flush that loses that race
   * reports NOT submitted.
   */
  public Thread shutdownHook() {
    return new Thread(
        () -> {
          boolean due;
          synchronized (this) {
            due = enabled() && started && flushed == null;
          }
          if (!due) {
            return;
          }
          try {
            String line =
                flush("the daemon was stopped by a signal")
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(settings.flushMillis() + 1_000, java.util.concurrent.TimeUnit.MILLISECONDS);
            LOG.info(line);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } catch (Exception e) {
            LOG.infof("telemetry: NOT submitted (%s)", e.toString());
          }
        },
        "ci-daemon-telemetry-shutdown");
  }

  /** Stop the timer and release the client. The process is on its way out. */
  public void shutdown() {
    synchronized (this) {
      if (timer >= 0) {
        vertx.cancelTimer(timer);
        timer = -1;
      }
    }
    if (http != null) {
      http.close();
    }
  }

  private Future<String> sendFinal(List<Otlp.SpanData> spanData) {
    List<Otlp.LogEntry> entries = takeBatch(Integer.MAX_VALUE);
    long now = clock.getAsLong();
    List<StepOutput.Line> lines = output.drain(now);
    for (StepOutput.Line line : lines) {
      int severity = line.stream() == Stream.ERR ? 13 : 9;
      entries.add(
          new Otlp.LogEntry(
              line.epochNanos(),
              severity,
              Otlp.severityText(severity),
              line.text(),
              Map.of(STREAM_ATTRIBUTE, line.stream() == Stream.ERR ? "err" : "out"),
              line.traceId(),
              line.spanId()));
    }
    long notExported = output.notExported();
    if (notExported > 0) {
      StepOutput.Line last = lines.isEmpty() ? null : lines.getLast();
      entries.add(
          new Otlp.LogEntry(
              now,
              9,
              "INFO",
              "step output: " + output.total() + " lines, " + notExported + " not exported",
              Map.of(Otlp.LOGGER_ATTRIBUTE, Telemetry.class.getName()),
              last == null ? spans.traceId() : last.traceId(),
              last == null ? null : last.spanId()));
    }
    Future<String> logs = Future.succeededFuture(null);
    for (int from = 0; from < entries.size(); from += Math.max(1, settings.batch())) {
      List<Otlp.LogEntry> batch =
          entries.subList(from, Math.min(entries.size(), from + Math.max(1, settings.batch())));
      logs = logs.compose(problem -> problem != null ? Future.succeededFuture(problem) : postLogs(batch));
    }
    Future<String> traces =
        post(tracesUrl(), Otlp.encodeSpans(resource, SCOPE, CiDaemonBinary.VERSION, spanData));
    Future<String> logsDone = logs;
    return Future.all(logsDone, traces)
        .map(
            all -> {
              String problem = logsDone.result() != null ? logsDone.result() : traces.result();
              if (problem != null) {
                return "telemetry: NOT submitted (" + problem + ")";
              }
              return "telemetry: submitted "
                  + submittedLogs.get()
                  + " logs, "
                  + spanData.size()
                  + " spans ("
                  + lost.get()
                  + " dropped)";
            });
  }

  /** The dropped-lines line first, if any were, then up to {@code max} queued lines. */
  private List<Otlp.LogEntry> takeBatch(int max) {
    List<Otlp.LogEntry> batch = new ArrayList<>();
    long lostNow = dropped.getAndSet(0);
    if (lostNow > 0) {
      batch.add(
          new Otlp.LogEntry(
              clock.getAsLong(),
              13,
              "WARN",
              "ci-daemon telemetry dropped " + lostNow + " log line(s): the queue was full",
              Map.of(Otlp.LOGGER_ATTRIBUTE, Telemetry.class.getName()),
              spans.traceId(),
              spans.current().id()));
    }
    queue.drainTo(batch, Math.max(0, max - batch.size()));
    return batch;
  }

  /** Completes with null when the collector took it, or with what went wrong. Never fails. */
  private Future<String> postLogs(List<Otlp.LogEntry> batch) {
    return post(logsUrl(), Otlp.encodeLogs(resource, SCOPE, CiDaemonBinary.VERSION, batch))
        .map(
            problem -> {
              if (problem == null) {
                submittedLogs.addAndGet(batch.size());
              }
              return problem;
            });
  }

  private Future<String> post(String url, byte[] body) {
    return http.post(url, headers, Buffer.buffer(body))
        .transform(
            answered ->
                Future.succeededFuture(
                    answered.failed()
                        ? String.valueOf(answered.cause().getMessage())
                        : answered.result().status() / 100 == 2
                            ? null
                            : "the collector answered " + answered.result().status()));
  }

  /** One line when the timer's shipping starts failing and one when it recovers. */
  private void report(String problem, int lines) {
    if (problem != null && failing.compareAndSet(false, true)) {
      LOG.warnf(
          "ci-daemon could not ship %d log line(s) to %s: %s; dropping them until it can",
          lines, logsUrl(), problem);
    } else if (problem == null && failing.compareAndSet(true, false)) {
      LOG.infof("ci-daemon ships its log to %s again", logsUrl());
    }
  }

  private String body(LogRecord record) {
    String message =
        record instanceof ExtLogRecord ext
            ? ext.getFormattedMessage()
            : julFormatter.formatMessage(record);
    StringBuilder body = new StringBuilder(message == null ? "" : message);
    if (record.getThrown() != null) {
      StringWriter trace = new StringWriter();
      record.getThrown().printStackTrace(new PrintWriter(trace));
      body.append('\n').append(trace);
    }
    return body.length() <= MAX_BODY ? body.toString() : body.substring(0, MAX_BODY) + "…";
  }

  /** Nothing to do on the logging thread: the timer sends, and {@link #flush} sends the rest. */
  @Override
  public void flush() {}

  /**
   * A no-op on purpose: a logging framework resetting its handlers at shutdown must not close the
   * client out from under a shutdown hook's flush. {@link #shutdown()} is the real close.
   */
  @Override
  public void close() {}
}
