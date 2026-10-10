package eu.wohlben.qits.cidaemon;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import java.util.Map;

/**
 * The one kind of plain HTTP call this daemon makes — its telemetry's POSTs ({@link Telemetry}) — on
 * the vertx-core client the WebSocket already brings. Not a REST client: two POST paths do not
 * justify one in a static binary. Copied from qits-ci-runner-daemon's {@code Http}, whose comment on
 * the context below was measured, not guessed.
 */
final class Http {

  /** A response, whatever its status. Callers branch on {@link #status()}; nothing throws on 4xx. */
  record Response(int status) {}

  private final Vertx vertx;
  private final HttpClient client;
  private final long timeoutMillis;

  Http(Vertx vertx, long timeoutMillis) {
    this.vertx = vertx;
    this.client = vertx.createHttpClient();
    this.timeoutMillis = timeoutMillis;
  }

  /** POST a binary {@code body} to an absolute url. A transport failure fails the future. */
  Future<Response> post(String url, Map<String, String> headers, Buffer body) {
    RequestOptions options =
        new RequestOptions()
            .setMethod(HttpMethod.POST)
            .setAbsoluteURI(url)
            .setConnectTimeout(timeoutMillis)
            .setIdleTimeout(timeoutMillis);
    headers.forEach(options::addHeader);
    // The whole exchange runs on a Vert.x context, never on the caller's thread: attached from
    // another thread, a continuation can be attached after the response already ended on the event
    // loop, and a body asked for after the end never completes — the runner measured exactly that.
    Promise<Response> answer = Promise.promise();
    vertx
        .getOrCreateContext()
        .runOnContext(
            v ->
                client
                    .request(options)
                    .compose(request -> request.send(body))
                    .compose(
                        response -> response.body().map(ignored -> new Response(response.statusCode())))
                    .onComplete(answer));
    return answer.future();
  }

  void close() {
    client.close();
  }
}
