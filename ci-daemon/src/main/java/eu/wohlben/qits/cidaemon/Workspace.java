package eu.wohlben.qits.cidaemon;

import eu.wohlben.qits.cidaemon.protocol.InitFailed;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.jboss.logging.Logger;

/**
 * The checkout half of initialization: a shallow clone of the pushed branch, then a checkout of the
 * pushed sha, shelling the step image's own {@code git} (the image contract — this binary carries no
 * git library, and one that could be linked statically against musl would cost more image than the
 * whole daemon).
 *
 * <p><b>The failure mapping is the interesting part of this class</b>, because it drives host
 * behaviour rather than just a log line:
 *
 * <ul>
 *   <li>no usable {@code git} ⇒ {@link InitFailed.Reason#TOOLING_MISSING} — the image does not
 *       satisfy the contract, which is a pipeline-config mistake and not an infrastructure failure.
 *       {@code bash} is probed here too, before the clone rather than at step time so that a broken
 *       image is reported as a broken image — but it is <b>not</b> required: an image without it
 *       runs the step under {@code sh}. See {@link #probeTooling()} for what demanding it cost.
 *   <li>the clone did not produce a checkout ⇒ {@link InitFailed.Reason#CLONE_FAILED}.
 *   <li><b>the clone succeeded but the sha is not there ⇒ {@link InitFailed.Reason#SHA_GONE}</b>,
 *       which is the force-push backstop. The host verified the sha was an ancestor of the branch
 *       before it launched anything; a push landing between that check and this clone is the race it
 *       cannot close, and this is where it surfaces. qits-ci re-reads its config source to confirm
 *       and then discards the whole run rather than recording a failure against a commit that no
 *       longer exists.
 * </ul>
 *
 * <p><b>Depth 50, not 1.</b> The clone is by branch and the checkout is by sha, so a run whose sha
 * is a few commits behind the branch tip — an entirely normal thing when pushes queue up behind a
 * serialized runner — must still find its commit. Depth 1 would report {@code SHA_GONE} for a commit
 * that is very much still there, and the host would discard a run it should have executed.
 */
public final class Workspace {

  private static final Logger LOG = Logger.getLogger(Workspace.class);

  /**
   * Deep enough that a sha a few pushes behind the tip is present, shallow enough that the clone
   * stays cheap. See the class javadoc for why 1 is wrong.
   */
  static final int CLONE_DEPTH = 50;

  /**
   * qits-1253: two delays, so three attempts total — a git host blip (a 503, a dropped connection)
   * otherwise fails the run for good, even though the same clone would have succeeded moments
   * later. See {@link #cloneWithRetry()} for why the retry lives here rather than in qits-ci.
   */
  static final List<Duration> DEFAULT_CLONE_RETRY_DELAYS =
      List.of(Duration.ofSeconds(2), Duration.ofSeconds(5));

  /**
   * What makes a clone failure worth retrying: a transient git-host condition rather than something
   * a retry cannot fix (a bad branch name, a 4xx, a malformed url). Matched case-insensitively
   * against the clone's combined output — see {@link #cloneWithRetry()}.
   */
  private static final List<Pattern> TRANSIENT_CLONE_FAILURES =
      List.of(
          Pattern.compile("The requested URL returned error: 5\\d\\d", Pattern.CASE_INSENSITIVE),
          Pattern.compile("Could not resolve host", Pattern.CASE_INSENSITIVE),
          Pattern.compile("Connection refused", Pattern.CASE_INSENSITIVE),
          Pattern.compile("Connection reset", Pattern.CASE_INSENSITIVE),
          Pattern.compile("Connection timed out", Pattern.CASE_INSENSITIVE),
          Pattern.compile("Operation timed out", Pattern.CASE_INSENSITIVE),
          Pattern.compile("early EOF", Pattern.CASE_INSENSITIVE),
          Pattern.compile("RPC failed", Pattern.CASE_INSENSITIVE),
          Pattern.compile("unexpected disconnect", Pattern.CASE_INSENSITIVE));

  /**
   * The {@code detail} budget, matching a {@link eu.wohlben.qits.cidaemon.protocol.StepChunk}'s. Git
   * can be verbose; the host records this and shows it to a human, so it is the <em>tail</em> that
   * is kept — the last thing git said before it gave up is the diagnosis, not the first.
   */
  static final int DETAIL_MAX_CHARS = 8192;

  /**
   * What a commit id can look like. Validated because it crosses from the environment into an argv
   * — the boundary rule — and because a value that cannot possibly name a commit must not reach
   * {@code git checkout} as something it might read as an option.
   */
  private static final Pattern COMMIT_ID = Pattern.compile("[0-9a-fA-F]{7,64}");

  private final Path dir;
  private final String repositoryUrl;
  private final String branch;
  private final String sha;
  private final CommandRunner runner;
  private final List<Duration> cloneRetryDelays;

  /**
   * The shell the step's script will be run under, decided by {@link #probeTooling()}.
   *
   * <p>{@code sh} until proven otherwise, so a {@link #shell()} read before {@link #prepare()} names
   * the shell every image has rather than one that may be absent. In the daemon's real order that
   * cannot happen — initialization completes before a {@code RunStep} is accepted — and defaulting
   * to the safe answer is cheaper than a state machine that says so.
   *
   * <p>Volatile because it is written on the worker that initializes and read on the worker that
   * runs the step; the daemon's own ordering makes them the same thread today, and this does not
   * depend on that staying true.
   */
  private volatile String shell = "sh";

  public Workspace(
      Path dir, String repositoryUrl, String branch, String sha, CommandRunner runner) {
    this(dir, repositoryUrl, branch, sha, runner, DEFAULT_CLONE_RETRY_DELAYS);
  }

  /**
   * @param cloneRetryDelays how long to wait before each re-clone after a transient failure; {@code
   *     n} delays give {@code n + 1} clone attempts total. See {@link #cloneWithRetry()}.
   */
  public Workspace(
      Path dir,
      String repositoryUrl,
      String branch,
      String sha,
      CommandRunner runner,
      List<Duration> cloneRetryDelays) {
    this.dir = dir;
    this.repositoryUrl = repositoryUrl;
    this.branch = branch;
    this.sha = sha;
    this.runner = runner;
    this.cloneRetryDelays = List.copyOf(cloneRetryDelays);
  }

  /**
   * The outcome of {@link #prepare()}: ready, or a reason the host branches on plus bounded human
   * detail. Not a boolean-plus-message because {@code reason} is a contract and {@code detail} is
   * prose — the whole point of {@link InitFailed}.
   */
  public record Preparation(InitFailed.Reason failure, String detail) {

    public static final Preparation READY = new Preparation(null, null);

    public boolean ready() {
      return failure == null;
    }
  }

  /** Clone and check out, or report why not. Never throws. */
  public Preparation prepare() {
    Preparation tooling = probeTooling();
    if (tooling != null) {
      return tooling;
    }
    if (!COMMIT_ID.matcher(sha).matches()) {
      // Reported as SHA_GONE rather than as a new reason: a sha that cannot name a commit is, from
      // the host's side, indistinguishable from one that no longer exists, and the host's response
      // to SHA_GONE is to re-read its config source and CONFIRM before discarding — so a malformed
      // value lands in the branch that checks rather than the one that assumes.
      return new Preparation(
          InitFailed.Reason.SHA_GONE, "QITS_CI_SHA is not a commit id: " + tail(sha));
    }
    CommandRunner.Result clone = cloneWithRetry();
    if (!clone.ok()) {
      return new Preparation(InitFailed.Reason.CLONE_FAILED, tail(clone.output()));
    }
    // --detach because the sha is a commit and not a branch: without it git checks out a detached
    // HEAD anyway but warns at length about it, and the warning would be the `detail` a human reads
    // on the next failure.
    CommandRunner.Result checkout = runner.run(dir.toFile(), "git", "checkout", "--detach", sha);
    if (!checkout.ok()) {
      return new Preparation(InitFailed.Reason.SHA_GONE, tail(checkout.output()));
    }
    return Preparation.READY;
  }

  /**
   * qits-1253: a git host that answers a clone with a transient condition — a 503, a dropped
   * connection — otherwise fails the run for good, even though the identical clone would likely
   * succeed moments later. qits-ci deliberately does <b>not</b> auto-retry a {@code CLONE_FAILED}
   * run itself, because that same reason also drives runner quarantine (a runner whose clones keep
   * failing is pulled from rotation) — so retrying a run-level failure there would blur "the host
   * blipped" with "this runner is sick". Retrying here, below that classifier, lets a blip stay a
   * blip instead of either failing the run or muddying quarantine.
   *
   * <p>Only a failure whose output matches {@link #TRANSIENT_CLONE_FAILURES} is retried; anything
   * else — a bad branch name, a 4xx — fails after one attempt, exactly as before this existed.
   */
  private CommandRunner.Result cloneWithRetry() {
    CommandRunner.Result result = runClone();
    int totalAttempts = cloneRetryDelays.size() + 1;
    for (int attempt = 1; attempt <= cloneRetryDelays.size(); attempt++) {
      String reason = transientReason(result.output());
      if (reason == null) {
        return result;
      }
      Duration delay = cloneRetryDelays.get(attempt - 1);
      LOG.warnf(
          "ci-daemon clone attempt %d/%d failed with a transient error (%s); retrying in %s",
          attempt, totalAttempts, reason, delay);
      if (!sleep(delay)) {
        // Interrupted: stop retrying and report what the last attempt said, same as any other exit.
        return result;
      }
      try {
        clearDirectoryContents(dir);
      } catch (IOException e) {
        // Best effort: a directory git refuses to clone into reports its own git error, which is
        // still a usable CLONE_FAILED detail, so there is nothing better to do here than give up on
        // the retry and let that error surface on the next attempt below.
        LOG.warnf(
            "ci-daemon could not clear %s before retrying the clone: %s", dir, e.getMessage());
      }
      result = runClone();
    }
    return result;
  }

  private CommandRunner.Result runClone() {
    return runner.run(
        null,
        "git",
        "clone",
        "--depth",
        String.valueOf(CLONE_DEPTH),
        "--branch",
        branch,
        // Everything after `--` is a positional, so a repository url or path that begins with a
        // dash cannot be read as an option. The branch is safe without it: --branch consumes
        // the next argument whatever it looks like.
        "--",
        repositoryUrl,
        dir.toString());
  }

  /** The matched text naming why {@code output} is worth retrying, or {@code null} if it is not. */
  private static String transientReason(String output) {
    if (output == null) {
      return null;
    }
    for (Pattern pattern : TRANSIENT_CLONE_FAILURES) {
      Matcher matcher = pattern.matcher(output);
      if (matcher.find()) {
        return matcher.group();
      }
    }
    return null;
  }

  /**
   * {@code false} if interrupted, with the interrupt flag restored, so a shutdown mid-retry stops
   * retrying rather than blocking on a sleep nobody asked for.
   */
  private static boolean sleep(Duration delay) {
    try {
      if (!delay.isZero() && !delay.isNegative()) {
        Thread.sleep(delay.toMillis());
      }
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /**
   * Empties {@code dir} without removing {@code dir} itself — it may be a mount point the container
   * runtime set up, not a directory this process is free to recreate. A dir that does not exist
   * (the common case: nothing has cloned into it yet) is left alone rather than treated as an error.
   */
  private static void clearDirectoryContents(Path dir) throws IOException {
    if (!Files.isDirectory(dir)) {
      return;
    }
    try (Stream<Path> entries = Files.list(dir)) {
      for (Path entry : entries.toList()) {
        deleteRecursively(entry);
      }
    }
  }

  private static void deleteRecursively(Path path) throws IOException {
    if (Files.isDirectory(path) && !Files.isSymbolicLink(path)) {
      try (Stream<Path> entries = Files.list(path)) {
        for (Path entry : entries.toList()) {
          deleteRecursively(entry);
        }
      }
    }
    Files.deleteIfExists(path);
  }

  /**
   * What the image promises, probed with {@code --version} so a missing tool is a failed spawn
   * rather than a mystery halfway through the clone. Returns {@code null} when the image is fine.
   *
   * <p><b>{@code git} is required and {@code bash} is not.</b> The clone needs git and there is no
   * substitute for it here; a shell is different, because every image that can run a container at
   * all has {@code /bin/sh}. Demanding bash cost the platform an entire class of image for no
   * capability: {@code docker:28-dind} carries docker, git and wget but no bash, so the one upstream
   * image that could have built the platform's own step images was refused
   * {@code TOOLING_MISSING} — and those step images were consequently buildable only by a machine
   * that already had them, which on 2026-08-20 meant a pruned host could not rebuild its own CI.
   *
   * <p>{@code bash} is still PREFERRED, and that is what keeps this change invisible to every
   * existing pipeline: an image that has it runs its script under it exactly as before, so no
   * script's bashisms are at risk. Only an image without it falls back, and such a script could
   * never have run there anyway.
   */
  private Preparation probeTooling() {
    CommandRunner.Result git = runner.run(null, "git", "--version");
    if (!git.ok()) {
      return new Preparation(
          InitFailed.Reason.TOOLING_MISSING,
          "the step image has no usable `git` (a ci step image must provide git): "
              + tail(git.output()));
    }
    shell = runner.run(null, "bash", "--version").ok() ? "bash" : "sh";
    return null;
  }

  /** The last {@link #DETAIL_MAX_CHARS} of {@code text}; null-safe. */
  private static String tail(String text) {
    if (text == null) {
      return null;
    }
    return text.length() <= DETAIL_MAX_CHARS
        ? text
        : text.substring(text.length() - DETAIL_MAX_CHARS);
  }

  /** Where the checkout lives, for the step process that runs in it. */
  public File directory() {
    return dir.toFile();
  }

  /**
   * Which shell the step's script runs under — {@code bash} when the image has it, {@code sh}
   * otherwise. Read after {@link #prepare()}; see {@link #shell} for what it answers before that.
   */
  public String shell() {
    return shell;
  }
}
