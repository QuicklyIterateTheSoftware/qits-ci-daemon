package eu.wohlben.qits.cidaemon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.cidaemon.protocol.Ack;
import eu.wohlben.qits.cidaemon.protocol.AckReceived;
import eu.wohlben.qits.cidaemon.protocol.Cancel;
import eu.wohlben.qits.cidaemon.protocol.CiDaemonCodec;
import eu.wohlben.qits.cidaemon.protocol.CiDaemonMessage;
import eu.wohlben.qits.cidaemon.protocol.CiDaemonProtocol;
import eu.wohlben.qits.cidaemon.protocol.Heartbeat;
import eu.wohlben.qits.cidaemon.protocol.Hello;
import eu.wohlben.qits.cidaemon.protocol.InitFailed;
import eu.wohlben.qits.cidaemon.protocol.Initialized;
import eu.wohlben.qits.cidaemon.protocol.RunStep;
import eu.wohlben.qits.cidaemon.protocol.StepChunk;
import eu.wohlben.qits.cidaemon.protocol.StepFinished;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.PfxOptions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/**
 * The flow, against a real in-JVM Vert.x WebSocket server standing in for qits-ci. Real socket, real
 * frames, real codec — the only thing scripted is the host's side of the conversation, which is the
 * point: the daemon's endings are what these tests are about, and each one is a real close, a real
 * timeout, or a real frame.
 *
 * <p>The step itself is a real {@link StepProcess} driving real {@code bash} wherever a step runs,
 * so the happy path proves the whole chain rather than the flow's opinion of it. Only the checkout
 * is scripted — a git clone would add nothing here that {@code WorkspaceTest} does not already pin.
 */
@EnabledOnOs(OS.LINUX)
class DaemonMainTest {

  private final Vertx vertx = Vertx.vertx();
  private final List<Host> hosts = Collections.synchronizedList(new ArrayList<>());

  @TempDir Path workDir;

  @AfterEach
  void tearDown() throws Exception {
    for (Host host : List.copyOf(hosts)) {
      host.close();
    }
    vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
  }

  @Test
  void theDialPresentsItsIdentityAsHandshakeHeadersAndNotInThePath() throws Exception {
    Host host = host((h, message) -> h.reply(message));

    int code = runDaemon(host.url("/ci/daemon"), ready(), 30).get(30, TimeUnit.SECONDS);

    assertEquals(ExitCode.OK, code);
    // The workspace control socket identifies its caller by a path parameter and that is its known
    // impersonation bug. The path here carries no identity at all; the headers do, and the host
    // validates them before it reads a frame.
    assertEquals("/ci/daemon", host.requestPath);
    assertEquals("daemon-1", host.headers.get(ControlSocket.HEADER_ID));
    assertEquals("s3cret", host.headers.get(ControlSocket.HEADER_SECRET));
    assertEquals("qits-ci-daemon", host.headers.get(ControlSocket.HEADER_USER));
    assertEquals("qits:system", host.headers.get(ControlSocket.HEADER_ROLES));
    // Without a token the INTERNAL handshake is exactly what it always was: no bearer at all, not
    // an empty one.
    assertNull(host.headers.get(ControlSocket.HEADER_AUTHORIZATION));
  }

  @Test
  void aRunTokenIsPresentedAsABearerAndReplacesTheAssertedIdentityButNotTheSecondFactor()
      throws Exception {
    Host host = host((h, message) -> h.reply(message));

    int code = runDaemon(tokenEnv(host.url("/ci/daemon")), 30).get(30, TimeUnit.SECONDS);

    assertEquals(ExitCode.OK, code);
    assertEquals("Bearer run-t0ken", host.headers.get(ControlSocket.HEADER_AUTHORIZATION));
    // The edge strips asserted identity on every inbound request; a daemon that still sent it would
    // be presenting exactly the claim the token exists to replace.
    assertFalse(host.headers.contains(ControlSocket.HEADER_USER));
    assertFalse(host.headers.contains(ControlSocket.HEADER_ROLES));
    // The id and secret bind the connection to this container; the token binds it to the run.
    // Both factors, in both modes.
    assertEquals("daemon-1", host.headers.get(ControlSocket.HEADER_ID));
    assertEquals("s3cret", host.headers.get(ControlSocket.HEADER_SECRET));
    // The subject is for the daemon's own log line and is never announced.
    assertFalse(host.headers.entries().toString().contains("run:42"));
  }

  @Test
  void aBlankTokenIsNoTokenAndKeepsTheInternalHandshake() throws Exception {
    Host host = host((h, message) -> h.reply(message));
    DaemonEnv env =
        new DaemonEnv(
            host.url("/ci/daemon"),
            "daemon-1",
            "s3cret",
            "file:///origin",
            "main",
            "0123456789abcdef",
            "  ",
            "");

    int code = runDaemon(env, 30).get(30, TimeUnit.SECONDS);

    assertEquals(ExitCode.OK, code);
    assertNull(host.headers.get(ControlSocket.HEADER_AUTHORIZATION));
    assertEquals("qits:system", host.headers.get(ControlSocket.HEADER_ROLES));
  }

  @Test
  void aWssUrlIsDialledOverTlsAgainstTheDefaultTrustStore() throws Exception {
    TestTls tls = TestTls.generate(workDir.resolve("tls"));
    Host host = host((h, message) -> h.reply(message), tls);

    int code;
    String previous = System.getProperty("javax.net.ssl.trustStore");
    // The default trust store and not a trust option on the client: that is the path the native
    // binary takes with its embedded cacerts, and pointing the JDK's default at the stub's root is
    // the only way to walk it with a certificate no public root signed.
    System.setProperty("javax.net.ssl.trustStore", tls.trustStore().toString());
    System.setProperty("javax.net.ssl.trustStorePassword", TestTls.PASSWORD);
    System.setProperty("javax.net.ssl.trustStoreType", "PKCS12");
    try {
      code = runDaemon(tokenEnv(host.tlsUrl("/ci/daemon")), 5_000).get(30, TimeUnit.SECONDS);
    } finally {
      if (previous == null) {
        System.clearProperty("javax.net.ssl.trustStore");
      } else {
        System.setProperty("javax.net.ssl.trustStore", previous);
      }
      System.clearProperty("javax.net.ssl.trustStorePassword");
      System.clearProperty("javax.net.ssl.trustStoreType");
    }

    assertEquals(ExitCode.OK, code);
    assertEquals("/ci/daemon", host.requestPath);
    assertEquals("Bearer run-t0ken", host.headers.get(ControlSocket.HEADER_AUTHORIZATION));
    assertNotNull(host.first(Hello.class));
  }

  @Test
  void aWssHostWhoseCertificateIsNotTrustedIsADialFailureRatherThanAnUnverifiedConnection()
      throws Exception {
    TestTls tls = TestTls.generate(workDir.resolve("tls"));
    Host host = host((h, message) -> h.reply(message), tls);

    // No trust store pointed at the stub: the JDK's own roots, which never signed it. A client that
    // quietly trusted everything would register here, and hand its bearer to whoever answered.
    int code = runDaemon(tokenEnv(host.tlsUrl("/ci/daemon")), 400).get(30, TimeUnit.SECONDS);

    assertEquals(ExitCode.DIAL_FAILED, code);
    assertNull(host.headers);
  }

  @Test
  void aUrlWithAQueryIsDialledVerbatimRatherThanReassembled() throws Exception {
    Host host = host((h, message) -> h.reply(message));

    int code = runDaemon(host.url("/ci/daemon?x=1"), ready(), 30).get(30, TimeUnit.SECONDS);

    assertEquals(ExitCode.OK, code);
    assertEquals("/ci/daemon?x=1", host.requestUri);
  }

  @Test
  void aStepRunsAndItsOutputAndResultReachTheHostBeforeTheSocketCloses() throws Exception {
    Host host = host((h, message) -> h.reply(message, "echo hello; echo oops >&2; exit 4"));

    int code = runDaemon(host.url("/ci/daemon"), ready(), 30).get(30, TimeUnit.SECONDS);
    host.awaitDrained();

    assertEquals(ExitCode.OK, code);
    assertEquals(
        new Hello("daemon-1", CiDaemonProtocol.CAPABILITY_VERSION), host.first(Hello.class));
    assertNotNull(host.first(Initialized.class));
    assertNull(host.first(InitFailed.class));
    StringBuilder out = new StringBuilder();
    host.all(StepChunk.class).forEach(chunk -> out.append(chunk.text()));
    assertTrue(out.toString().contains("hello"), out::toString);
    assertTrue(out.toString().contains("oops"), out::toString);
    StepFinished finished = host.first(StepFinished.class);
    assertEquals(4, finished.exitCode());
    assertFalse(finished.timedOut());
    // The terminal frame is the LAST thing on the wire: chunks are drained before it is sent, so a
    // host that stops reading at StepFinished has already seen everything the step printed.
    assertEquals(StepFinished.class, host.received.get(host.received.size() - 1).getClass());
  }

  @Test
  void anAckCarryingACapabilityVersionThisBinaryDoesNotKnowEndsTheProcessNonzero()
      throws Exception {
    Host host =
        host(
            (h, message) -> {
              if (message instanceof Hello) {
                h.send(new Ack(CiDaemonProtocol.CAPABILITY_VERSION + 7));
              }
            });

    int code = runDaemon(host.url("/ci/daemon"), ready(), 30).get(30, TimeUnit.SECONDS);
    host.awaitDrained();

    assertEquals(ExitCode.CAPABILITY_MISMATCH, code);
    // No compat mode, and nothing half-done: the daemon does not clone for a host it cannot talk to.
    assertNull(host.first(Initialized.class));
  }

  @Test
  void aMatchingAckIsConfirmedBeforeTheCloneStarts() throws Exception {
    Host host = host((h, message) -> h.reply(message));

    int code = runDaemon(host.url("/ci/daemon"), ready(), 30).get(30, TimeUnit.SECONDS);
    host.awaitDrained();

    assertEquals(ExitCode.OK, code);
    List<CiDaemonMessage> received = host.received;
    int ackReceivedAt = indexOfFirst(received, AckReceived.class);
    int initializedAt = indexOfFirst(received, Initialized.class);
    assertTrue(ackReceivedAt >= 0, "expected an AckReceived confirming the host's Ack");
    assertTrue(
        ackReceivedAt < initializedAt,
        "AckReceived must reach the host before the clone's own Initialized: " + received);
  }

  @Test
  void aFailedCheckoutIsReportedAsInitFailedAndTheDaemonExitsNonzero() throws Exception {
    Host host = host((h, message) -> h.reply(message));

    int code =
        runDaemon(
                host.url("/ci/daemon"),
                () -> new Workspace.Preparation(InitFailed.Reason.SHA_GONE, "fatal: reference is not a tree"),
                30)
            .get(30, TimeUnit.SECONDS);
    host.awaitDrained();

    // The frame carries the outcome the host branches on; the exit code says the container did not
    // run its step. Those are two different statements and both are true.
    assertEquals(ExitCode.INIT_FAILED_SENT, code);
    InitFailed failed = host.first(InitFailed.class);
    assertEquals(InitFailed.Reason.SHA_GONE, failed.reason());
    assertEquals("fatal: reference is not a tree", failed.detail());
    assertNull(host.first(Initialized.class));
  }

  @Test
  void aSocketClosedBeforeRunStepIsAnExitAndNotARetry() throws Exception {
    Host host =
        host(
            (h, message) -> {
              if (message instanceof Hello) {
                h.send(new Ack(CiDaemonProtocol.CAPABILITY_VERSION));
              } else if (message instanceof Initialized) {
                h.socket.close(); // the host has reaped us; there is nothing to reconnect to
              }
            });

    int code = runDaemon(host.url("/ci/daemon"), ready(), 30).get(30, TimeUnit.SECONDS);

    assertEquals(ExitCode.SOCKET_CLOSED_EARLY, code);
  }

  @Test
  void aHostThatCannotBeReachedEndsTheDialWithinItsBudgetRatherThanRetryingForever()
      throws Exception {
    // A port with nothing on it. The budget is a total across attempts, so this must end promptly:
    // the container is one the host will reap on its own register timeout anyway.
    long startedAt = System.nanoTime();
    int code =
        runDaemon("ws://127.0.0.1:1/ci/daemon", ready(), 400).get(30, TimeUnit.SECONDS);
    long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000L;

    assertEquals(ExitCode.DIAL_FAILED, code);
    assertTrue(elapsedMillis < 10_000, () -> "the dial budget did not bound the retry: " + elapsedMillis + "ms");
  }

  @Test
  void aMalformedDialUrlEndsImmediatelyBecauseItWillNotBecomeParseableOnRetry() throws Exception {
    int code = runDaemon("not a url at all", ready(), 30_000).get(30, TimeUnit.SECONDS);

    assertEquals(ExitCode.DIAL_FAILED, code);
  }

  @Test
  void anEnvironmentMissingItsSecretExitsBeforeAnythingIsDialled() {
    DaemonEnv env =
        new DaemonEnv("ws://127.0.0.1:1/ci/daemon", "daemon-1", "", "file:///origin", "main", "abc123f");

    int code =
        new DaemonMain(
                vertx,
                env,
                new ControlSocket.Settings(10_000, 30_000, 500, 5_000),
                ready(),
                (request, emit) -> step(request, emit))
            .run();

    assertEquals(ExitCode.MISCONFIGURED, code);
  }

  @Test
  void aCancelAfterRunStepEndsTheStepWithAStepFinishedRatherThanASilence() throws Exception {
    Host host =
        host(
            (h, message) -> {
              if (message instanceof Hello) {
                h.send(new Ack(CiDaemonProtocol.CAPABILITY_VERSION));
              } else if (message instanceof Initialized) {
                h.send(new RunStep("c1", "echo started; sleep 60", 300));
              } else if (message instanceof StepChunk) {
                // The step is demonstrably running; cancel it.
                h.send(new Cancel("c1"));
              }
            });

    int code = runDaemon(host.url("/ci/daemon"), ready(), 30).get(60, TimeUnit.SECONDS);
    host.awaitDrained();

    assertEquals(ExitCode.OK, code);
    StepFinished finished = host.first(StepFinished.class);
    assertNotNull(finished, "a cancelled step still finishes — the host must not await a silence");
    assertFalse(finished.timedOut(), "a cancellation is not a timeout");
  }

  @Test
  void aCancelWithNoStepRunningEndsTheProcess() throws Exception {
    Host host =
        host(
            (h, message) -> {
              if (message instanceof Hello) {
                h.send(new Ack(CiDaemonProtocol.CAPABILITY_VERSION));
              } else if (message instanceof Initialized) {
                h.send(new Cancel("c1"));
              }
            });

    int code = runDaemon(host.url("/ci/daemon"), ready(), 30).get(30, TimeUnit.SECONDS);

    assertEquals(ExitCode.CANCELLED_BEFORE_STEP, code);
  }

  @Test
  void heartbeatsRunUnderneathTheConversationFromDialUntilClose() throws Exception {
    AtomicInteger heartbeats = new AtomicInteger();
    Host host =
        host(
            (h, message) -> {
              if (message instanceof Heartbeat) {
                heartbeats.incrementAndGet();
                return;
              }
              // Hold the step open long enough for a few beats: the host's step timeout is a
              // backstop, not a liveness probe, so a step printing nothing must still look alive.
              h.reply(message, "sleep 1");
            });

    int code = runDaemon(host.url("/ci/daemon"), ready(), 30, 100).get(30, TimeUnit.SECONDS);

    assertEquals(ExitCode.OK, code);
    assertTrue(heartbeats.get() >= 2, () -> "expected heartbeats, saw " + heartbeats.get());
  }

  @Test
  void anUndecodableFrameFromTheHostIsDroppedRatherThanEndingTheConnection() throws Exception {
    Host host =
        host(
            (h, message) -> {
              if (message instanceof Hello) {
                h.socket.writeTextMessage("{\"type\":\"nonsense\"}");
                h.socket.writeTextMessage("not json at all");
                h.send(new Ack(CiDaemonProtocol.CAPABILITY_VERSION));
              } else if (message instanceof Initialized) {
                h.send(new RunStep("c1", "echo survived", 30));
              }
            });

    int code = runDaemon(host.url("/ci/daemon"), ready(), 30).get(30, TimeUnit.SECONDS);
    host.awaitDrained();

    // The daemon is the party that can least afford to die of a frame it did not understand: to the
    // host it would be indistinguishable from a container that went quiet mid-step.
    assertEquals(ExitCode.OK, code);
    assertNotNull(host.first(StepFinished.class));
  }

  // --- harness ------------------------------------------------------------------------------------

  private DaemonMain.Initializer ready() {
    return () -> Workspace.Preparation.READY;
  }

  private static int indexOfFirst(List<CiDaemonMessage> messages, Class<?> type) {
    for (int i = 0; i < messages.size(); i++) {
      if (type.isInstance(messages.get(i))) {
        return i;
      }
    }
    return -1;
  }

  private Step step(RunStep request, java.util.function.Consumer<CiDaemonMessage> emit) {
    return new StepProcess(workDir, request, emit, 8192, 100, 2000, "bash");
  }

  private CompletableFuture<Integer> runDaemon(
      String url, DaemonMain.Initializer initializer, long dialBudgetMillis) {
    return runDaemon(url, initializer, dialBudgetMillis, 10_000);
  }

  private CompletableFuture<Integer> runDaemon(
      String url,
      DaemonMain.Initializer initializer,
      long dialBudgetMillis,
      long heartbeatMillis) {
    return runDaemon(
        new DaemonEnv(url, "daemon-1", "s3cret", "file:///origin", "main", "0123456789abcdef"),
        initializer,
        dialBudgetMillis,
        heartbeatMillis);
  }

  private CompletableFuture<Integer> runDaemon(DaemonEnv env, long dialBudgetMillis) {
    return runDaemon(env, ready(), dialBudgetMillis, 10_000);
  }

  private CompletableFuture<Integer> runDaemon(
      DaemonEnv env,
      DaemonMain.Initializer initializer,
      long dialBudgetMillis,
      long heartbeatMillis) {
    DaemonMain daemon =
        new DaemonMain(
            vertx,
            env,
            new ControlSocket.Settings(heartbeatMillis, dialBudgetMillis, 50, 200),
            initializer,
            this::step);
    return CompletableFuture.supplyAsync(daemon::run);
  }

  /** The EDGE plane's environment: the INTERNAL one plus the run's token. */
  private static DaemonEnv tokenEnv(String url) {
    return new DaemonEnv(
        url,
        "daemon-1",
        "s3cret",
        "file:///origin",
        "main",
        "0123456789abcdef",
        "run-t0ken",
        "run:42");
  }

  private Host host(BiConsumer<Host, CiDaemonMessage> script) throws Exception {
    return host(script, null);
  }

  private Host host(BiConsumer<Host, CiDaemonMessage> script, TestTls tls) throws Exception {
    Host host = new Host(script, tls);
    hosts.add(host);
    return host;
  }

  /**
   * A self-signed certificate for 127.0.0.1 and a trust store holding it, minted by the JDK's own
   * {@code keytool} into a temp directory. A real process rather than a committed fixture: nothing
   * key-shaped lives in the tree, nothing expires under the suite, and no certificate library joins
   * the test classpath.
   */
  private record TestTls(Path keyStore, Path trustStore) {

    static final String PASSWORD = "changeit";

    static TestTls generate(Path dir) throws Exception {
      Files.createDirectories(dir);
      Path keyStore = dir.resolve("host.p12");
      Path cert = dir.resolve("host.crt");
      Path trustStore = dir.resolve("trust.p12");
      keytool(
          "-genkeypair", "-alias", "host", "-keyalg", "EC", "-groupname", "secp256r1",
          "-dname", "CN=127.0.0.1", "-ext", "SAN=ip:127.0.0.1,dns:localhost",
          "-validity", "2", "-storetype", "PKCS12", "-keystore", keyStore.toString(),
          "-storepass", PASSWORD, "-keypass", PASSWORD);
      keytool(
          "-exportcert", "-rfc", "-alias", "host", "-keystore", keyStore.toString(),
          "-storepass", PASSWORD, "-file", cert.toString());
      keytool(
          "-importcert", "-noprompt", "-alias", "host", "-file", cert.toString(),
          "-storetype", "PKCS12", "-keystore", trustStore.toString(), "-storepass", PASSWORD);
      return new TestTls(keyStore, trustStore);
    }

    private static void keytool(String... args) throws Exception {
      List<String> command = new ArrayList<>();
      command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
      command.addAll(List.of(args));
      Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
      String output = new String(process.getInputStream().readAllBytes());
      assertTrue(process.waitFor(60, TimeUnit.SECONDS), "keytool hung");
      assertEquals(0, process.exitValue(), () -> "keytool " + args[0] + ": " + output);
    }
  }

  /** qits-ci's side of the socket: a real server, with its half of the conversation scripted. */
  private final class Host implements AutoCloseable {

    private final HttpServer server;
    private final BiConsumer<Host, CiDaemonMessage> script;

    final List<CiDaemonMessage> received = Collections.synchronizedList(new ArrayList<>());
    volatile MultiMap headers;
    volatile String requestPath;
    volatile String requestUri;
    volatile ServerWebSocket socket;

    /** Completes when the daemon's socket closes; see {@link #awaitDrained()}. */
    private final CompletableFuture<Void> closed = new CompletableFuture<>();

    Host(BiConsumer<Host, CiDaemonMessage> script, TestTls tls) throws Exception {
      this.script = script;
      HttpServerOptions options = new HttpServerOptions();
      if (tls != null) {
        options
            .setSsl(true)
            .setKeyCertOptions(
                new PfxOptions().setPath(tls.keyStore().toString()).setPassword(TestTls.PASSWORD));
      }
      this.server =
          vertx
              .createHttpServer(options)
              .webSocketHandler(this::onUpgrade)
              .listen(0)
              .toCompletionStage()
              .toCompletableFuture()
              .get(10, TimeUnit.SECONDS);
    }

    private void onUpgrade(ServerWebSocket ws) {
      headers = ws.headers();
      requestPath = ws.path();
      requestUri = ws.uri();
      socket = ws;
      ws.closeHandler(v -> closed.complete(null));
      ws.textMessageHandler(
          json -> {
            CiDaemonMessage message = CiDaemonCodec.decode(new JsonObject(json).getMap());
            received.add(message);
            script.accept(this, message);
          });
    }

    /**
     * Wait until the wire is drained before reading {@link #received}. The daemon's exit code only
     * proves its last frame was <em>written</em>; this side reads it on another event loop, so an
     * assertion made the instant {@code run()} returns can look at the list before the frame has
     * reached it. Vert.x delivers the close after the frames that preceded it on the same
     * connection, so the close is the signal that everything the daemon sent has been handled here.
     */
    void awaitDrained() throws Exception {
      closed.get(10, TimeUnit.SECONDS);
    }

    /** The default script: Ack the Hello, answer Initialized with a trivial step. */
    void reply(CiDaemonMessage message) {
      reply(message, "true");
    }

    void reply(CiDaemonMessage message, String script) {
      if (message instanceof Hello) {
        send(new Ack(CiDaemonProtocol.CAPABILITY_VERSION));
      } else if (message instanceof Initialized) {
        send(new RunStep("c1", script, 300));
      }
    }

    void send(CiDaemonMessage message) {
      socket.writeTextMessage(new JsonObject(CiDaemonCodec.encode(message)).encode());
    }

    String url(String path) {
      return "ws://127.0.0.1:" + server.actualPort() + path;
    }

    String tlsUrl(String path) {
      return "wss://127.0.0.1:" + server.actualPort() + path;
    }

    <T extends CiDaemonMessage> T first(Class<T> type) {
      synchronized (received) {
        return received.stream().filter(type::isInstance).map(type::cast).findFirst().orElse(null);
      }
    }

    <T extends CiDaemonMessage> List<T> all(Class<T> type) {
      synchronized (received) {
        return received.stream().filter(type::isInstance).map(type::cast).toList();
      }
    }

    @Override
    public void close() throws Exception {
      hosts.remove(this);
      server.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
  }
}
