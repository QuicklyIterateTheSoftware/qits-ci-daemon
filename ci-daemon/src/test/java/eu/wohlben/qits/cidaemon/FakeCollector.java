package eu.wohlben.qits.cidaemon;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * qits-observability's OTLP receiver, as far as the daemon can tell: a real Vert.x HTTP server on an
 * ephemeral port that keeps every request it answered — and, switched to {@link #silent}, one that
 * accepts the connection and the request and never answers at all.
 */
final class FakeCollector implements AutoCloseable {

  /** One answered request: its path, bearer, body, and when its response finished writing. */
  record Request(String path, String authorization, byte[] body, long answeredAtNanos) {}

  private final HttpServer server;
  final List<Request> requests = Collections.synchronizedList(new ArrayList<>());
  volatile boolean silent;
  volatile int status = 200;

  FakeCollector(Vertx vertx) throws Exception {
    server =
        vertx
            .createHttpServer()
            .requestHandler(this::onRequest)
            .listen(0)
            .toCompletionStage()
            .toCompletableFuture()
            .get(10, TimeUnit.SECONDS);
  }

  private void onRequest(HttpServerRequest request) {
    String path = request.path();
    String authorization = request.getHeader("Authorization");
    request
        .body()
        .onSuccess(
            body -> {
              if (silent) {
                return; // read it, hold the connection open, never answer
              }
              request
                  .response()
                  .setStatusCode(status)
                  .end()
                  .onComplete(
                      written ->
                          requests.add(
                              new Request(path, authorization, body.getBytes(), System.nanoTime())));
            });
  }

  /** The endpoint, as {@code QITS_CI_TELEMETRY_URL} would name it. */
  String endpoint() {
    return "http://127.0.0.1:" + server.actualPort() + "/otel";
  }

  List<Request> to(String suffix) {
    synchronized (requests) {
      return requests.stream().filter(r -> r.path().endsWith(suffix)).toList();
    }
  }

  List<OtlpDecode.Line> lines() {
    return to(Telemetry.LOGS_PATH).stream()
        .flatMap(r -> OtlpDecode.logs(r.body()).lines().stream())
        .toList();
  }

  List<OtlpDecode.Span> spans() {
    return to(Telemetry.TRACES_PATH).stream()
        .flatMap(r -> OtlpDecode.traces(r.body()).spans().stream())
        .toList();
  }

  long lastAnsweredAtNanos() {
    synchronized (requests) {
      return requests.stream().mapToLong(Request::answeredAtNanos).max().orElse(Long.MAX_VALUE);
    }
  }

  @Override
  public void close() throws Exception {
    server.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
  }
}
