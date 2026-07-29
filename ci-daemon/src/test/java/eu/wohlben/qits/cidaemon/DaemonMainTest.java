package eu.wohlben.qits.cidaemon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.cidaemon.protocol.Ack;
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
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
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

    assertEquals(ExitCode.CAPABILITY_MISMATCH, code);
    // No compat mode, and nothing half-done: the daemon does not clone for a host it cannot talk to.
    assertNull(host.first(Initialized.class));
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

    // The daemon is the party that can least afford to die of a frame it did not understand: to the
    // host it would be indistinguishable from a container that went quiet mid-step.
    assertEquals(ExitCode.OK, code);
    assertNotNull(host.first(StepFinished.class));
  }

  // --- harness ------------------------------------------------------------------------------------

  private DaemonMain.Initializer ready() {
    return () -> Workspace.Preparation.READY;
  }

  private Step step(RunStep request, java.util.function.Consumer<CiDaemonMessage> emit) {
    return new StepProcess(workDir, request, emit, 8192, 100, 2000);
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
    DaemonEnv env =
        new DaemonEnv(url, "daemon-1", "s3cret", "file:///origin", "main", "0123456789abcdef");
    DaemonMain daemon =
        new DaemonMain(
            vertx,
            env,
            new ControlSocket.Settings(heartbeatMillis, dialBudgetMillis, 50, 200),
            initializer,
            this::step);
    return CompletableFuture.supplyAsync(daemon::run);
  }

  private Host host(BiConsumer<Host, CiDaemonMessage> script) throws Exception {
    Host host = new Host(script);
    hosts.add(host);
    return host;
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

    Host(BiConsumer<Host, CiDaemonMessage> script) throws Exception {
      this.script = script;
      this.server =
          vertx
              .createHttpServer()
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
      ws.textMessageHandler(
          json -> {
            CiDaemonMessage message = CiDaemonCodec.decode(new JsonObject(json).getMap());
            received.add(message);
            script.accept(this, message);
          });
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
