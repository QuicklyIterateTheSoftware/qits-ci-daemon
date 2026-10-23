package eu.wohlben.qits.cidaemon;

import eu.wohlben.qits.cidaemon.protocol.CiDaemonBinary;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.vertx.core.Vertx;
import jakarta.inject.Inject;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.jboss.logging.Logger;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Entry point, and <b>the one CDI shell</b>: it resolves configuration, news up the plain classes
 * that do the work, and hands its exit code back to the runtime. Everything below it — {@link
 * DaemonMain}, {@link ControlSocket}, {@link MirrorWorktree}, {@link StepProcess} — is a plain
 * class with a plain constructor, which is what makes the suite able to drive the whole flow
 * without a container.
 *
 * <p>That single-reader property is the point of the shape, not a style preference: two components
 * resolving the same key independently is how they come to disagree about it silently. Every setting
 * a class needs arrives as a constructor argument from here.
 *
 * <p>Unlike the {@code workspace-daemon} this repo mirrors, {@code run} does <b>not</b> call {@link
 * Quarkus#waitForExit()}. This process has one run's steps to run and then it is done; {@link
 * DaemonMain#run()} blocks until a terminal condition and returns the code the container exits with.
 *
 * <p>{@code --version} is the one argument: it prints {@link CiDaemonBinary#VERSION} and exits 0
 * before Quarkus starts, so the release can run the built binary as a smoke test with no
 * environment, no container and no docker.
 */
@QuarkusMain
public class Main {

  private static final Logger LOG = Logger.getLogger(Main.class);

  /**
   * Read raw rather than through config, because its empty value means something: set and empty
   * switches the telemetry export off, where unset derives it (see {@link Telemetry#endpoint}) —
   * and SmallRye reads an empty value as no value at all. The config key {@code
   * qits.ci.telemetry-url} is the fallback when the variable is absent.
   */
  static final String TELEMETRY_URL = "QITS_CI_TELEMETRY_URL";

  /** What {@code --version} prints: the binary's name and version, nothing else. */
  static String versionLine() {
    return CiDaemonBinary.DAEMON_NAME + " " + CiDaemonBinary.VERSION;
  }

  public static void main(String... args) {
    if (args.length == 1 && "--version".equals(args[0])) {
      System.out.println(versionLine());
      return;
    }
    Quarkus.run(DaemonApplication.class, args);
  }

  public static class DaemonApplication implements QuarkusApplication {

    @Inject Vertx vertx;

    // All Optional<String>, all unset by default: an empty `defaultValue` is read by SmallRye as
    // *no value* and then fails to resolve a plain String, killing the binary at startup with a
    // message about config rather than about the missing environment. DaemonEnv.missing() is what
    // turns an absent one into a sentence naming the variable the launcher forgot.
    @ConfigProperty(name = "qits.ci.daemon-url")
    Optional<String> daemonUrl;

    @ConfigProperty(name = "qits.ci.daemon-id")
    Optional<String> daemonId;

    @ConfigProperty(name = "qits.ci.repository-url")
    Optional<String> repositoryUrl;

    @ConfigProperty(name = "qits.ci.branch")
    Optional<String> branch;

    @ConfigProperty(name = "qits.ci.sha")
    Optional<String> sha;

    @ConfigProperty(name = "qits.token")
    Optional<String> token;

    // Optional in the other sense too: DaemonEnv.missing() never names this one. It only names the
    // token in a log line.
    @ConfigProperty(name = "qits.token-subject")
    Optional<String> tokenSubject;

    // The lease's warm mirrors and the run's worktree (qits-1541): /cache/git/<projectId>/<repo>.git
    // and /work/<runId>. Keys so the suite — or QITS_CI_GIT_CACHE_DIR / QITS_CI_WORK_DIR, or a
    // -D system property — can point them at a temp directory.
    @ConfigProperty(name = "qits.ci.git-cache-dir", defaultValue = "/cache/git")
    String gitCacheDir;

    @ConfigProperty(name = "qits.ci.work-dir", defaultValue = "/work")
    String workDir;

    @ConfigProperty(name = "qits.ci.heartbeat-interval-ms", defaultValue = "10000")
    long heartbeatMillis;

    @ConfigProperty(name = "qits.ci.dial-budget-ms", defaultValue = "30000")
    long dialBudgetMillis;

    @ConfigProperty(name = "qits.ci.dial-initial-backoff-ms", defaultValue = "500")
    long dialInitialBackoffMillis;

    @ConfigProperty(name = "qits.ci.dial-max-backoff-ms", defaultValue = "5000")
    long dialMaxBackoffMillis;

    @ConfigProperty(name = "qits.ci.reconnect-budget-ms", defaultValue = "90000")
    long reconnectBudgetMillis;

    @ConfigProperty(name = "qits.ci.git-timeout-seconds", defaultValue = "600")
    long gitTimeoutSeconds;

    // qits-1253: how long to wait before each retry after the git host answers a mirror clone or
    // fetch with a transient failure (a 503, a dropped connection). Comma-separated,
    // ISO-8601-ish with the `PT` prefix implied: "2S,5S" means two attempts after the first, after
    // 2s and then 5s. Not a List<Duration> ConfigProperty: SmallRye's Duration converter demands the
    // full "PT2S" form, and the point of writing it here is that it does not.
    @ConfigProperty(name = "qits.ci.clone-retry-delays", defaultValue = "2S,5S")
    String cloneRetryDelays;

    @ConfigProperty(name = "qits.ci.step-kill-grace-ms", defaultValue = "5000")
    long stepKillGraceMillis;

    /** Flush on a newline, on this many characters, or on the interval below — whichever first. */
    @ConfigProperty(name = "qits.ci.step-chunk-max-chars", defaultValue = "8192")
    int stepChunkMaxChars;

    @ConfigProperty(name = "qits.ci.step-chunk-flush-ms", defaultValue = "100")
    long stepChunkFlushMillis;

    @ConfigProperty(name = "qits.ci.telemetry-url")
    Optional<String> telemetryUrl;

    @ConfigProperty(name = "qits.ci.telemetry-flush-ms", defaultValue = "3000")
    long telemetryFlushMillis;

    @ConfigProperty(name = "qits.ci.telemetry-output-max-lines", defaultValue = "200")
    int telemetryOutputMaxLines;

    // Labels on the telemetry and nothing else — see application.properties. All optional: a value
    // the launcher did not hand over is an attribute left out, never a reason not to run.
    @ConfigProperty(name = "qits.ci.run-id")
    Optional<String> runId;

    @ConfigProperty(name = "qits.ci.step-index")
    Optional<String> stepIndex;

    @ConfigProperty(name = "qits.ci.repo-id")
    Optional<String> repoId;

    @ConfigProperty(name = "qits.ci.repo-name")
    Optional<String> repoName;

    @ConfigProperty(name = "qits.ci.project-id")
    Optional<String> projectId;

    @ConfigProperty(name = "qits.domain")
    Optional<String> domain;

    @Override
    public int run(String... args) {
      DaemonEnv env =
          new DaemonEnv(
              daemonUrl.orElse(""),
              daemonId.orElse(""),
              repositoryUrl.orElse(""),
              branch.orElse(""),
              sha.orElse(""),
              token.orElse(""),
              tokenSubject.orElse(""),
              runId.orElse(""));
      Telemetry telemetry = telemetry(env);
      // On the root logger, so every line this process writes is shipped — queued from here, sent
      // once the environment is known to be complete.
      java.util.logging.Logger.getLogger("").addHandler(telemetry);
      if (telemetry.enabled()) {
        Runtime.getRuntime().addShutdownHook(telemetry.shutdownHook());
      }
      MirrorWorktree worktree =
          new MirrorWorktree(
              Path.of(gitCacheDir),
              Path.of(workDir),
              env.repositoryUrl(),
              projectId.orElse(""),
              repoName.orElse(""),
              repoId.orElse(""),
              env.runId(),
              env.sha(),
              CommandRunner.forking(gitTimeoutSeconds),
              parseCloneRetryDelays(cloneRetryDelays));
      // One counter for every step of the lease; see StepProcess on seq.
      AtomicLong seq = new AtomicLong();
      return new DaemonMain(
              vertx,
              env,
              new ControlSocket.Settings(
                  heartbeatMillis,
                  dialBudgetMillis,
                  dialInitialBackoffMillis,
                  dialMaxBackoffMillis,
                  reconnectBudgetMillis),
              worktree,
              (request, emit) ->
                  new StepProcess(
                      worktree.directory().toPath(),
                      request,
                      emit,
                      seq,
                      stepChunkMaxChars,
                      stepChunkFlushMillis,
                      stepKillGraceMillis,
                      // Read here rather than captured above: initialization decides them, and it
                      // has always completed by the time a step runs (the lane orders them).
                      worktree.shell(),
                      worktree.processGroups()),
              telemetry)
          .run();
    }

    /**
     * qits-1253: {@code "2S,5S"} to {@code [PT2S, PT5S]}. The {@code PT} prefix is implied rather
     * than written in config because every other duration-shaped setting here is a bare number of
     * seconds or millis — spelling one entry {@code PT2S} and the rest {@code 2000} would be the
     * inconsistency, not the fix.
     */
    private static List<Duration> parseCloneRetryDelays(String raw) {
      List<Duration> delays = new ArrayList<>();
      for (String token : raw.split(",")) {
        String trimmed = token.trim();
        if (!trimmed.isEmpty()) {
          delays.add(Duration.parse("PT" + trimmed));
        }
      }
      return delays;
    }

    /**
     * The export, or {@link Telemetry#off()}. A malformed override switches it off with one line
     * rather than failing the step: telemetry is never allowed to change a step's outcome.
     */
    private Telemetry telemetry(DaemonEnv env) {
      String raw = System.getenv(TELEMETRY_URL);
      String endpoint;
      try {
        endpoint = Telemetry.endpoint(raw != null ? raw : telemetryUrl.orElse(null), env.daemonUrl());
      } catch (IllegalArgumentException malformed) {
        LOG.warnf("ci-daemon exports no telemetry: %s", malformed.getMessage());
        return Telemetry.off();
      }
      if (endpoint == null) {
        return Telemetry.off();
      }
      return new Telemetry(
          vertx,
          endpoint,
          env.token(),
          Telemetry.resource(
              runId.orElse(null),
              stepIndex.orElse(null),
              repoId.orElse(null),
              repoName.orElse(null),
              projectId.orElse(null),
              env.branch(),
              env.sha(),
              domain.orElse(null)),
          runId.orElse(null),
          Telemetry.Settings.defaults(telemetryOutputMaxLines, telemetryFlushMillis),
          Telemetry::nowNanos);
    }
  }
}
