package eu.wohlben.qits.cidaemon;

import eu.wohlben.qits.cidaemon.protocol.InitFailed;
import java.io.File;
import java.nio.file.Path;
import java.util.regex.Pattern;

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
 *   <li>no usable {@code git} or {@code bash} ⇒ {@link InitFailed.Reason#TOOLING_MISSING} — the
 *       image does not satisfy the contract, which is a pipeline-config mistake and not an
 *       infrastructure failure. {@code bash} is probed here, before the clone, even though nothing
 *       needs it until the step runs: discovering it at step time would report a broken image as a
 *       failed step.
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

  /**
   * Deep enough that a sha a few pushes behind the tip is present, shallow enough that the clone
   * stays cheap. See the class javadoc for why 1 is wrong.
   */
  static final int CLONE_DEPTH = 50;

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

  public Workspace(
      Path dir, String repositoryUrl, String branch, String sha, CommandRunner runner) {
    this.dir = dir;
    this.repositoryUrl = repositoryUrl;
    this.branch = branch;
    this.sha = sha;
    this.runner = runner;
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
    CommandRunner.Result clone =
        runner.run(
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
   * Both binaries the image contract promises, probed with {@code --version} so a missing one is a
   * failed spawn rather than a mystery halfway through the clone. Returns {@code null} when the
   * image is fine.
   */
  private Preparation probeTooling() {
    for (String tool : new String[] {"git", "bash"}) {
      CommandRunner.Result probe = runner.run(null, tool, "--version");
      if (!probe.ok()) {
        return new Preparation(
            InitFailed.Reason.TOOLING_MISSING,
            "the step image has no usable `"
                + tool
                + "` (a ci step image must provide git and bash): "
                + tail(probe.output()));
      }
    }
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
}
