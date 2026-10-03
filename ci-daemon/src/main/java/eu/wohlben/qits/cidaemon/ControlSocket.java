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
 * WebSocketClient}, capped backoff, writes marshalled onto the connection's context — and bounds
 * its central invariant. The workspace daemon reconnects forever because it is the container's
 * reason to exist. This one reconnects for a <b>budget</b>: the dial budget covers a host that is
 * still coming up, and a separate reconnect budget covers a socket that drops after connecting —
 * which is what every step does when qits-edge, the path the socket travels, is redeployed (qits-748).
 * The step behind the socket is still running and the host holds its launch through a grace of its
 * own, so the same token re-dials and the conversation resumes. Only a reconnect budget that runs
 * out is terminal: the host has reaped the launch by then, and retrying past it only delays an
 * outcome that is already decided.
 *
 * <p>An outage lasts from the close until the host <em>speaks</em> on a new socket, not until the
 * upgrade completes: a host that accepts and closes again has not taken the launch back, so the
 * budget and the backoff carry across such cycles rather than restarting with each of them. A close
 * this side asked for — the one after the terminal frame — is never re-dialled.
 *
 * <p>The credential travels as a handshake header and the launch id in the first frame — never in
 * the path. The workspace control socket identifies its caller by a path parameter, which is its
 * known impersonation bug; this does not reproduce it.
 *
 * <p><b>One handshake: {@code Authorization: Bearer $QITS_TOKEN} and <em>nothing else</em>.</b> No
 * asserted identity (the edge strips it from every inbound request, and a daemon that sent it would
 * be presenting a claim the edge exists to refuse) and no id or secret header either, because the
 * edge strips {@code X-Qits-*} headers of every shape and a header that never arrives authenticates
 * nothing. The token already proves this container's run to qits-ci; what is left to say is only
 * <em>which</em> launch of that run this is, and that travels in the first frame instead — the
 * {@code Hello} every capability version already sends carries the launch id, and the host matches
 * its launch record against the token's subject once that frame names it.
 *
 * <p>A {@code wss://} url is dialled over TLS against the default trust store, and that is a
 * deliberate absence of options rather than an oversight. There is no trust option to set because
 * the step image is arbitrary — its {@code /etc/ssl} may be empty, stale or absent — so the
 * roots have to be the binary's own. In the native image they are the build JDK's {@code cacerts},
 * which GraalVM embeds in the image heap at build time; {@code quarkus.ssl.native} in {@code
 * application.properties} is what keeps Quarkus from substituting a disabled {@code SSLContext}.
 */
public final class ControlSocket {

  private static final Logger LOG = Logger.getLogger(ControlSocket.class);

  /** The one handshake header: it carries the run's own token. */
  static final String HEADER_AUTHORIZATION = "Authorization";

  /**
   * What the socket tells its owner. Deliberately events and not a message stream: two of them are
   * terminal conditions for a daemon that always exits, and naming them separately is what keeps
   * {@link DaemonMain} from having to infer an ending from a silence.
   */
  public interface Listener {

    /** The first upgrade completed. Time to say {@code Hello}. */
    void onConnected();

    /**
     * A connection that had dropped is back, on a new socket. Time to say {@code Hello} again: the
     * host re-admits the launch on it, and nothing else may precede it on the new socket.
     */
    void onReconnected();

    /** The connection dropped and is being re-dialled. Not terminal; frames sent now will fail. */
    void onDisconnected();

    /** A decodable frame arrived. Undecodable ones never reach here. */
    void onMessage(CiDaemonMessage message);

    /** A dropped connection could not be re-established within the reconnect budget. Terminal. */
    void onClosed();

    /** The dial budget ran out without a first connection. Terminal. */
    void onDialFailed(String detail);
  }

  /**
   * Dial and liveness knobs. Both budgets are <b>totals</b> across attempts. The dial budget is short
   * on purpose: a daemon that cannot reach its host is a container the host reaps on its own
   * register timeout. The reconnect budget has to cover an edge redeploy and must be at least the
   * host's own reconnect grace ({@code qits.ci.daemon.reconnect-grace-seconds} in qits-ci), or the
   * daemon gives up on a launch the host is still holding for it.
   */
  public record Settings(
      long heartbeatMillis,
      long dialBudgetMillis,
      long dialInitialBackoffMillis,
      long dialMaxBackoffMillis,
      long reconnectBudgetMillis) {}

  private final Vertx vertx;
  private final String url;
  private final String token;
  private final Settings settings;
  private final Listener listener;

  private volatile WebSocketClient client;
  private volatile WebSocket socket;
  private volatile Context socketContext;
  private volatile boolean ended;

  /** Set by {@link #close()}: a close this side asked for is the end, never an outage. */
  private volatile boolean closing;

  private volatile boolean connectedOnce;

  /** The deadline of the dial, or of the outage in progress. */
  private volatile long deadlineNanos;

  /** Nonzero while an outage is open; cleared by the first frame the host sends afterwards. */
  private volatile long outageStartedNanos;

  /** Attempts in the current dial or outage, accept-then-close cycles included; drives backoff. */
  private volatile int attempts;

  public ControlSocket(
      Vertx vertx,
      String url,
      String token,
      Settings settings,
      Listener listener) {
    this.vertx = vertx;
    this.url = url;
    this.token = token;
    this.settings = settings;
    this.listener = listener;
  }

  /**
   * Begin dialling. Reports exactly one of {@link Listener#onConnected} or {@link
   * Listener#onDialFailed}; after a connect, any number of disconnect/reconnect pairs, and at most
   * one {@link Listener#onClosed}.
   */
  public void start() {
    client = vertx.createWebSocketClient();
    deadlineNanos = System.nanoTime() + settings.dialBudgetMillis() * 1_000_000L;
    if (settings.heartbeatMillis() > 0) {
      // Armed from the dial rather than from the connect, so the cadence does not depend on how
      // long the host took to answer. It no-ops until there is a socket to write on.
      vertx.setPeriodic(settings.heartbeatMillis(), id -> heartbeat());
    }
    connect();
  }

  private void connect() {
    if (closing || ended) {
      return;
    }
    WebSocketConnectOptions options;
    try {
      options = optionsFor(url);
    } catch (RuntimeException e) {
      // An unparseable url will not become parseable on retry, and there is nothing to fall back
      // to: the daemon is told this address and derives nothing.
      end(() -> listener.onDialFailed("malformed QITS_CI_DAEMON_URL '" + url + "'"));
      return;
    }
    // No X-Qits-* header of any shape: qits-edge strips them all, so sending one here would only
    // be a header this container mistakenly trusted to arrive. The token is the whole handshake;
    // which launch this is travels in the Hello that follows instead.
    options.addHeader(HEADER_AUTHORIZATION, "Bearer " + token);
    int attempt = attempts++;
    client
        .connect(options)
        .onSuccess(this::onConnected)
        .onFailure(
            t -> {
              LOG.debugf("ci-daemon dial attempt %d failed: %s", attempt, t.getMessage());
              retryOrGiveUp(t.getMessage());
            });
  }

  /** Schedule the next attempt, or report the budget in force as spent. */
  private void retryOrGiveUp(String lastFailure) {
    long backoff = backoffFor(attempts);
    if (System.nanoTime() + backoff * 1_000_000L < deadlineNanos) {
      vertx.setTimer(backoff, id -> connect());
      return;
    }
    if (!connectedOnce) {
      end(
          () ->
              listener.onDialFailed(
                  "no connection to "
                      + url
                      + " within "
                      + settings.dialBudgetMillis()
                      + "ms ("
                      + attempts
                      + " attempts): "
                      + lastFailure));
      return;
    }
    LOG.warnf(
        "ci-daemon could not re-establish the control socket to %s within %dms (%d attempts): %s",
        url, settings.reconnectBudgetMillis(), attempts, lastFailure);
    end(listener::onClosed);
  }

  private long backoffFor(int attempt) {
    long backoff =
        settings.dialInitialBackoffMillis() * (1L << Math.min(attempt, 20));
    return Math.min(settings.dialMaxBackoffMillis(), Math.max(1, backoff));
  }

  private void onConnected(WebSocket ws) {
    if (closing || ended) {
      // The daemon finished while this attempt was in flight; nothing is left to say on it.
      ws.close();
      return;
    }
    socketContext = vertx.getOrCreateContext();
    ws.textMessageHandler(this::onFrame);
    ws.closeHandler(v -> onLost(ws));
    ws.exceptionHandler(t -> LOG.debugf("ci-daemon control socket error: %s", t.getMessage()));
    socket = ws;
    if (connectedOnce) {
      LOG.infof("ci-daemon control socket re-established (attempt %d)", attempts);
      listener.onReconnected();
    } else {
      connectedOnce = true;
      listener.onConnected();
    }
  }

  /**
   * A connected socket closed. Unless this side asked for it, that opens (or continues) an outage
   * and the same token is dialled again within the reconnect budget.
   */
  private void onLost(WebSocket ws) {
    if (closing || ended || socket != ws) {
      return;
    }
    if (outageStartedNanos == 0) {
      outageStartedNanos = System.nanoTime();
      deadlineNanos = outageStartedNanos + settings.reconnectBudgetMillis() * 1_000_000L;
      attempts = 0;
      LOG.warnf(
          "ci-daemon control socket closed; re-dialling for up to %dms",
          settings.reconnectBudgetMillis());
    }
    listener.onDisconnected();
    retryOrGiveUp("the socket closed");
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
    // The host speaking is what ends an outage: an upgrade alone does not mean the launch was taken
    // back, a frame does.
    outageStartedNanos = 0;
    attempts = 0;
    listener.onMessage(message);
  }

  /**
   * Write a frame, marshalling onto the connection's context. The returned future completes when
   * the frame is actually written — which is what lets a terminal frame be followed by a close and
   * an exit without racing the write off the end of the process. While the socket is down it fails
   * at once: holding what must survive an outage is the owner's business, because only the owner
   * knows which frames those are.
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

  /**
   * Close the connection for good; completes when it is closed (or immediately if it never opened).
   * A close asked for here is never re-dialled.
   */
  public Future<Void> close() {
    closing = true;
    WebSocket ws = socket;
    Future<Void> closed = ws == null || ws.isClosed() ? Future.succeededFuture() : ws.close();
    return closed.otherwiseEmpty();
  }

  /** Release the client. Called once the process is on its way out. */
  public void shutdown() {
    closing = true;
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
