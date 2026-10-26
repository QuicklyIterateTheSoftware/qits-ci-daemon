package eu.wohlben.qits.cidaemon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.cidaemon.MirrorWorktreeTest.Git;
import eu.wohlben.qits.cidaemon.protocol.Ack;
import eu.wohlben.qits.cidaemon.protocol.CiDaemonCodec;
import eu.wohlben.qits.cidaemon.protocol.CiDaemonMessage;
import eu.wohlben.qits.cidaemon.protocol.CiDaemonProtocol;
import eu.wohlben.qits.cidaemon.protocol.Close;
import eu.wohlben.qits.cidaemon.protocol.Hello;
import eu.wohlben.qits.cidaemon.protocol.Initialized;
import eu.wohlben.qits.cidaemon.protocol.RunStep;
import eu.wohlben.qits.cidaemon.protocol.StepChunk;
import eu.wohlben.qits.cidaemon.protocol.StepFinished;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/**
 * One lease, end to end and with nothing scripted but the host: a real socket, a real {@link
 * MirrorWorktree} on a bare repository built in a temp directory, and real {@link StepProcess}es
 * running {@code bash} one after another in the one worktree — then {@code Close}, and the worktree
 * is gone.
 */
@EnabledOnOs(OS.LINUX)
class MultiStepRunTest {

  private final Vertx vertx = Vertx.vertx();
  private HttpServer server;

  @TempDir Path tmp;

  @AfterEach
  void tearDown() throws Exception {
    if (server != null) {
      server.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
    vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
  }

  @Test
  void threeStepsRunInOneWorktreeTheSecondsFailureIsReportedAndCloseRemovesTheWorktree()
      throws Exception {
    Path remote = remote();
    String sha = Git.sha(remote, "HEAD");
    List<RunStep> plan =
        List.of(
            new RunStep("c0", 0, "pwd; cat file.txt; echo made > made-by-step-0", 60, Map.of()),
            new RunStep("c1", 1, "echo failing >&2; exit 7", 60, Map.of()),
            new RunStep(
                "c2",
                2,
                "cat made-by-step-0; echo \"$QITS_TEST_STEP_VAR\"",
                60,
                Map.of("QITS_TEST_STEP_VAR", "only-step-2")));
    List<CiDaemonMessage> received = Collections.synchronizedList(new ArrayList<>());
    CompletableFuture<Void> closed = new CompletableFuture<>();
    server =
        vertx
            .createHttpServer()
            .webSocketHandler(
                ws -> {
                  ws.closeHandler(v -> closed.complete(null));
                  ws.textMessageHandler(
                      json -> {
                        CiDaemonMessage message =
                            CiDaemonCodec.decode(new JsonObject(json).getMap());
                        received.add(message);
                        switch (message) {
                          case Hello h -> send(ws, new Ack(CiDaemonProtocol.CAPABILITY_VERSION));
                          case Initialized i -> send(ws, plan.getFirst());
                          case StepFinished f -> {
                            int next = f.stepIndex() + 1;
                            send(ws, next < plan.size() ? plan.get(next) : new Close());
                          }
                          default -> {}
                        }
                      });
                })
            .listen(0)
            .toCompletionStage()
            .toCompletableFuture()
            .get(10, TimeUnit.SECONDS);

    MirrorWorktree worktree =
        new MirrorWorktree(
            tmp.resolve("cache"),
            tmp.resolve("work"),
            remote.toString(),
            "p1",
            "repo",
            "repo-id",
            "run-7",
            sha,
            CommandRunner.forking(60),
            List.of(Duration.ZERO),
            MirrorWorktree.DEFAULT_STALE_AFTER);
    AtomicLong seq = new AtomicLong();
    DaemonEnv env =
        new DaemonEnv(
            "ws://127.0.0.1:" + server.actualPort() + "/ci/daemon",
            "daemon-1",
            remote.toString(),
            "main",
            sha,
            "run-t0ken",
            "run:7",
            "run-7");
    DaemonMain daemon =
        new DaemonMain(
            vertx,
            env,
            new ControlSocket.Settings(10_000, 5_000, 50, 200, 5_000),
            worktree,
            (request, emit) ->
                new StepProcess(
                    worktree.directory().toPath(),
                    request,
                    emit,
                    seq,
                    8192,
                    50,
                    2000,
                    worktree.shell(),
                    worktree.processGroups()));

    int code = CompletableFuture.supplyAsync(daemon::run).get(120, TimeUnit.SECONDS);
    closed.get(10, TimeUnit.SECONDS);

    assertEquals(ExitCode.OK, code);
    List<StepFinished> finished = all(received, StepFinished.class);
    assertEquals(List.of(0, 1, 2), finished.stream().map(StepFinished::stepIndex).toList());
    assertEquals(List.of(0, 7, 0), finished.stream().map(StepFinished::exitCode).toList());
    assertEquals(
        List.of("c0", "c1", "c2"), finished.stream().map(StepFinished::correlationId).toList());

    Path dir = tmp.resolve("work/run-7");
    assertEquals(dir + "\ncommitted\nmade\n" + "only-step-2\n", outOf(received, "c0", "c2"));
    assertTrue(errOf(received, "c1").contains("failing"));
    // One seq for the lease, strictly increasing across every step's chunks.
    List<Long> seqs = all(received, StepChunk.class).stream().map(StepChunk::seq).toList();
    assertEquals(seqs.stream().sorted().distinct().toList(), seqs);

    assertFalse(Files.exists(dir), "Close removes the worktree");
    String worktrees = Git.run(tmp.resolve("cache/p1/repo.git"), "worktree", "list");
    assertFalse(worktrees.contains("run-7"), worktrees);
    assertNotNull(Files.readString(tmp.resolve("cache/p1/repo.git/HEAD")), "the mirror stays");
  }

  private static void send(ServerWebSocket ws, CiDaemonMessage message) {
    ws.writeTextMessage(new JsonObject(CiDaemonCodec.encode(message)).encode());
  }

  private static <T> List<T> all(List<CiDaemonMessage> received, Class<T> type) {
    synchronized (received) {
      return received.stream().filter(type::isInstance).map(type::cast).toList();
    }
  }

  private static String outOf(List<CiDaemonMessage> received, String... correlationIds) {
    List<String> ids = List.of(correlationIds);
    StringBuilder out = new StringBuilder();
    all(received, StepChunk.class).stream()
        .filter(c -> ids.contains(c.correlationId()))
        .filter(c -> c.stream() == eu.wohlben.qits.cidaemon.protocol.Stream.OUT)
        .forEach(c -> out.append(c.text()));
    return out.toString();
  }

  private static String errOf(List<CiDaemonMessage> received, String correlationId) {
    StringBuilder err = new StringBuilder();
    all(received, StepChunk.class).stream()
        .filter(c -> c.correlationId().equals(correlationId))
        .filter(c -> c.stream() == eu.wohlben.qits.cidaemon.protocol.Stream.ERR)
        .forEach(c -> err.append(c.text()));
    return err.toString();
  }

  private Path remote() throws Exception {
    Path source = tmp.resolve("src");
    Files.createDirectories(source);
    Git.run(source, "init", "-q", "-b", "main");
    Files.writeString(source.resolve("file.txt"), "committed\n");
    Git.run(source, "add", "file.txt");
    Git.run(source, "commit", "-q", "-m", "one");
    Path bare = tmp.resolve("remote/repo.git");
    Files.createDirectories(bare.getParent());
    Git.run(tmp, "clone", "-q", "--bare", source.toString(), bare.toString());
    return bare;
  }
}
