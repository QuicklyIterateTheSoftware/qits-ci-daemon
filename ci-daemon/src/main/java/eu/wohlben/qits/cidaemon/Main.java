package eu.wohlben.qits.cidaemon;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.vertx.core.Vertx;
import jakarta.inject.Inject;
import java.nio.file.Path;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Entry point, and <b>the one CDI shell</b>: it resolves configuration, news up the plain classes
 * that do the work, and hands its exit code back to the runtime. Everything below it — {@link
 * DaemonMain}, {@link ControlSocket}, {@link Workspace}, {@link StepProcess} — is a plain class with
 * a plain constructor, which is what makes the suite able to drive the whole flow without a
 * container.
 *
 * <p>That single-reader property is the point of the shape, not a style preference: two components
 * resolving the same key independently is how they come to disagree about it silently. Every setting
 * a class needs arrives as a constructor argument from here.
 *
 * <p>Unlike the {@code workspace-daemon} this repo mirrors, {@code run} does <b>not</b> call {@link
 * Quarkus#waitForExit()}. This process has one step to run and then it is done; {@link
 * DaemonMain#run()} blocks until a terminal condition and returns the code the container exits with.
 */
@QuarkusMain
public class Main {

  public static void main(String... args) {
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

    @ConfigProperty(name = "qits.ci.daemon-secret")
    Optional<String> daemonSecret;

    @ConfigProperty(name = "qits.ci.repository-url")
    Optional<String> repositoryUrl;

    @ConfigProperty(name = "qits.ci.branch")
    Optional<String> branch;

    @ConfigProperty(name = "qits.ci.sha")
    Optional<String> sha;

    @ConfigProperty(name = "qits.ci.workspace-dir", defaultValue = "/workspace")
    String workspaceDir;

    @ConfigProperty(name = "qits.ci.heartbeat-interval-ms", defaultValue = "10000")
    long heartbeatMillis;

    @ConfigProperty(name = "qits.ci.dial-budget-ms", defaultValue = "30000")
    long dialBudgetMillis;

    @ConfigProperty(name = "qits.ci.dial-initial-backoff-ms", defaultValue = "500")
    long dialInitialBackoffMillis;

    @ConfigProperty(name = "qits.ci.dial-max-backoff-ms", defaultValue = "5000")
    long dialMaxBackoffMillis;

    @ConfigProperty(name = "qits.ci.git-timeout-seconds", defaultValue = "600")
    long gitTimeoutSeconds;

    @ConfigProperty(name = "qits.ci.step-kill-grace-ms", defaultValue = "5000")
    long stepKillGraceMillis;

    /** Flush on a newline, on this many characters, or on the interval below — whichever first. */
    @ConfigProperty(name = "qits.ci.step-chunk-max-chars", defaultValue = "8192")
    int stepChunkMaxChars;

    @ConfigProperty(name = "qits.ci.step-chunk-flush-ms", defaultValue = "100")
    long stepChunkFlushMillis;

    @Override
    public int run(String... args) {
      DaemonEnv env =
          new DaemonEnv(
              daemonUrl.orElse(""),
              daemonId.orElse(""),
              daemonSecret.orElse(""),
              repositoryUrl.orElse(""),
              branch.orElse(""),
              sha.orElse(""));
      Path dir = Path.of(workspaceDir);
      Workspace workspace =
          new Workspace(
              dir,
              env.repositoryUrl(),
              env.branch(),
              env.sha(),
              CommandRunner.forking(gitTimeoutSeconds));
      return new DaemonMain(
              vertx,
              env,
              new ControlSocket.Settings(
                  heartbeatMillis,
                  dialBudgetMillis,
                  dialInitialBackoffMillis,
                  dialMaxBackoffMillis),
              workspace::prepare,
              (request, emit) ->
                  new StepProcess(
                      dir,
                      request,
                      emit,
                      stepChunkMaxChars,
                      stepChunkFlushMillis,
                      stepKillGraceMillis))
          .run();
    }
  }
}
