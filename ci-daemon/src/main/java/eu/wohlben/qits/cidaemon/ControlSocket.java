package eu.wohlben.qits.cidaemon;

import eu.wohlben.qits.cidaemon.protocol.CiDaemonCodec;
import eu.wohlben.qits.cidaemon.protocol.CiDaemonMessage;
import eu.wohlben.qits.cidaemon.protocol.Heartbeat;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketClient;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.JsonObject;
import java.net.URI;
import org.jboss.logging.Logger;

/**
 * The one connection this container ever makes: an outbound WebSocket to qits-ci, dialled from
 * {@code $QITS_CI_DAEMON_URL} <b>verbatim</b>. There is no inbound listener in the container at any
 * stage, and no address of this container's own is ever announced.
 *
 * <p>Mirrors qits-workspace-daemon's {@code ControlSocket} in its mechanics — vert.x {@link
 * WebSocketClient}, capped backoff, writes marshalled onto the connection's context — and inverts
 * its central invariant. The workspace daemon reconnects forever because it is the container's
 * reason to exist. This one dials <b>once</b>: the retry budget covers a host that is still coming
 * up, and a socket that closes after connecting is a terminal condition, because the only thing
 * behind this socket was one step's worth of conversation.
 *
 * <p>Identity travels as handshake headers, not in the path and not in the first frame. The host
 * validates both against its in-memory launch record before it reads anything, and closes 1008 on a
 * mismatch. The workspace control socket identifies its caller by a path parameter, which is its
 * known impersonation bug; this does not reproduce it.
 */
public final class ControlSocket {

  private static final Logger LOG = Logger.getLogger(ControlSocket.class);

  /** The handshake headers qits-ci authenticates the connection with. */
  static final String HEADER_ID = "X-Qits-Ci-Daemon-Id";

  static final String HEADER_SECRET = "X-Qits-Ci-Daemon-Secret";

  /**
   * What the socket tells its owner. Deliberately four events and not a message stream: three of
   * them are terminal conditions for a daemon that always exits, and naming them separately is what
   * keeps {@link DaemonMain} from having to infer an ending from a silence.
   */
  public interface Listener {

    /** The upgrade completed. Time to say {@code Hello}. */
    void onConnected();

    /** A decodable frame arrived. Undecodable ones never reach here. */
    void onMessage(CiDaemonMessage message);

    /** The connection closed. Terminal: this socket does not re-dial once it has connected. */
    void onClosed();

    /** The dial budget ran out without a connection. Terminal. */
    void onDialFailed(String detail);
  }

  /**
   * Dial and liveness knobs. The budget is a <b>total</b> across attempts and is short on purpose: a
   * daemon that cannot reach its host is a container the host reaps on its own register timeout, so
   * retrying past that only delays a decided outcome.
   */
  public record Settings(
      long heartbeatMillis,
      long dialBudgetMillis,
      long dialInitialBackoffMillis,
      long dialMaxBackoffMillis) {}

  private final Vertx vertx;
  private final String url;
  private final String daemonId;
  private final String daemonSecret;
  private final Settings settings;
  private final Listener listener;

  private volatile WebSocketClient client;
  private volatile WebSocket socket;
  private volatile Context socketContext;
  private volatile boolean ended;
  private long deadlineNanos;

  public ControlSocket(
      Vertx vertx,
      String url,
      String daemonId,
      String daemonSecret,
      Settings settings,
      Listener listener) {
    this.vertx = vertx;
    this.url = url;
    this.daemonId = daemonId;
    this.daemonSecret = daemonSecret;
    this.settings = settings;
    this.listener = listener;
  }

  /** Begin dialling. Reports exactly one of {@link Listener#onConnected} or the two endings. */
  public void start() {
    client = vertx.createWebSocketClient();
    deadlineNanos = System.nanoTime() + settings.dialBudgetMillis() * 1_000_000L;
    if (settings.heartbeatMillis() > 0) {
      // Armed from the dial rather than from the connect, so the cadence does not depend on how
      // long the host took to answer. It no-ops until there is a socket to write on.
      vertx.setPeriodic(settings.heartbeatMillis(), id -> heartbeat());
    }
    connect(0);
  }

  private void connect(int attempt) {
    WebSocketConnectOptions options;
    try {
      options = optionsFor(url);
    } catch (RuntimeException e) {
      // An unparseable url will not become parseable on retry, and there is nothing to fall back
      // to: the daemon is told this address and derives nothing.
      end(() -> listener.onDialFailed("malformed QITS_CI_DAEMON_URL '" + url + "'"));
      return;
    }
    options
        .addHeader("X-Qits-User", "qits-ci-daemon")
        .addHeader("X-Qits-Roles", "qits:system")
        .addHeader(HEADER_ID, daemonId)
        .addHeader(HEADER_SECRET, daemonSecret);
    client
        .connect(options)
        .onSuccess(this::onConnected)
        .onFailure(
            t -> {
              long backoff = backoffFor(attempt);
              if (System.nanoTime() + backoff * 1_000_000L >= deadlineNanos) {
                end(
                    () ->
                        listener.onDialFailed(
                            "no connection to "
                                + url
                                + " within "
                                + settings.dialBudgetMillis()
                                + "ms ("
                                + (attempt + 1)
                                + " attempts): "
                                + t.getMessage()));
                return;
              }
              LOG.debugf("ci-daemon dial attempt %d failed: %s", attempt, t.getMessage());
              vertx.setTimer(backoff, id -> connect(attempt + 1));
            });
  }

  private long backoffFor(int attempt) {
    long backoff =
        settings.dialInitialBackoffMillis() * (1L << Math.min(attempt, 20));
    return Math.min(settings.dialMaxBackoffMillis(), Math.max(1, backoff));
  }

  private void onConnected(WebSocket ws) {
    socketContext = vertx.getOrCreateContext();
    ws.textMessageHandler(this::onFrame);
    ws.closeHandler(v -> end(listener::onClosed));
    ws.exceptionHandler(t -> LOG.debugf("ci-daemon control socket error: %s", t.getMessage()));
    socket = ws;
    listener.onConnected();
  }

  private void onFrame(String json) {
    CiDaemonMessage message;
    try {
      message = CiDaemonCodec.decode(new JsonObject(json).getMap());
    } catch (RuntimeException e) {
      // Dropped, not fatal. The codec is strict by design and the daemon is the party that can
      // least afford to die of a frame it did not understand — it would look to the host exactly
      // like a container that went quiet mid-step.
      LOG.debugf("ci-daemon dropped an undecodable frame: %s", e.getMessage());
      return;
    }
    listener.onMessage(message);
  }

  /**
   * Write a frame, marshalling onto the connection's context. The returned future completes when
   * the frame is actually written — which is what lets a terminal frame be followed by a close and
   * an exit without racing the write off the end of the process.
   */
  public Future<Void> send(CiDaemonMessage message) {
    WebSocket ws = socket;
    if (ws == null || ws.isClosed()) {
      return Future.failedFuture("ci-daemon control socket is not open");
    }
    String json = new JsonObject(CiDaemonCodec.encode(message)).encode();
    Context context = socketContext;
    if (context != null && Vertx.currentContext() != context) {
      Promise<Void> promise = Promise.promise();
      context.runOnContext(v -> ws.writeTextMessage(json).onComplete(promise));
      return promise.future();
    }
    return ws.writeTextMessage(json);
  }

  /** Close the connection; completes when it is closed (or immediately if it never opened). */
  public Future<Void> close() {
    WebSocket ws = socket;
    Future<Void> closed = ws == null || ws.isClosed() ? Future.succeededFuture() : ws.close();
    return closed.otherwiseEmpty();
  }

  /** Release the client. Called once the process is on its way out. */
  public void shutdown() {
    WebSocketClient c = client;
    if (c != null) {
      c.close();
    }
  }

  private void heartbeat() {
    WebSocket ws = socket;
    if (ws != null && !ws.isClosed()) {
      send(new Heartbeat());
    }
  }

  /** Report a terminal condition at most once; a close on the way out must not re-report. */
  private void end(Runnable report) {
    if (ended) {
      return;
    }
    ended = true;
    report.run();
  }

  /**
   * Split the url into what vert.x's client wants, and nothing more. The path and query are carried
   * across untouched: this is the address the daemon was handed, and a client that reassembled it
   * from parts it thought it understood is how a daemon ends up dialling somewhere its host is not.
   */
  static WebSocketConnectOptions optionsFor(String url) {
    URI uri = URI.create(url);
    if (uri.getHost() == null) {
      throw new IllegalArgumentException("no host in '" + url + "'");
    }
    String scheme = uri.getScheme() == null ? "ws" : uri.getScheme().toLowerCase();
    boolean ssl = scheme.equals("wss") || scheme.equals("https");
    int port = uri.getPort() != -1 ? uri.getPort() : (ssl ? 443 : 80);
    String path = uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath();
    if (uri.getRawQuery() != null) {
      path = path + "?" + uri.getRawQuery();
    }
    return new WebSocketConnectOptions().setHost(uri.getHost()).setPort(port).setURI(path).setSsl(ssl);
  }
}
