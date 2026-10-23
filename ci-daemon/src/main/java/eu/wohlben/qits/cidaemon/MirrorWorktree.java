package eu.wohlben.qits.cidaemon;

import eu.wohlben.qits.cidaemon.protocol.InitFailed;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.jboss.logging.Logger;

/**
 * The run's checkout: a worktree of a warm bare mirror on the target, rather than a clone per step.
 *
 * <pre>
 *   under flock &lt;cache&gt;/&lt;projectId&gt;/&lt;repoName&gt;.lock:
 *     git clone --mirror (first use) | git fetch --prune origin (every later one)
 *     git worktree prune
 *     git worktree add --detach &lt;work&gt;/&lt;runId&gt; &lt;sha&gt;
 *     git submodule init + sync           (they write the mirror's shared config)
 *   then, per submodule:
 *     its own mirror, cloned under its own lock if missing
 *     git submodule update --init --depth 50 [--reference &lt;its mirror&gt;] -- &lt;path&gt;
 *   on Close:
 *     git worktree remove --force &lt;work&gt;/&lt;runId&gt;     (under the lock again)
 * </pre>
 *
 * <p><b>Why a mirror.</b> A CI lease runs every step of a run in one container on a target that
 * keeps {@code /cache/git} between leases, so the second run of a repository pays a fetch of the
 * delta instead of a clone, and a step's checkout is a worktree of objects already on disk. A bare
 * {@code --mirror} rather than a normal clone because it carries every ref the git host has, so any
 * sha the host may name — a branch tip, a sha a few pushes behind it, a tag — is there after one
 * fetch, with no depth to guess (the old per-step clone was 50 deep for exactly that reason).
 *
 * <p><b>Why the lock covers the worktree add too.</b> Two leases of the same repository on one
 * target share the mirror. A fetch running while another run's {@code worktree prune} or {@code
 * worktree add} rewrites {@code worktrees/} is the race; one lock around all of it is what makes the
 * mirror single-writer. The submodule clones run outside it — they are the slow part, and they write
 * only into this worktree's own {@code modules/} — and a submodule's own mirror is created under its
 * own lock, never while the parent's is held, so the two locks are never taken in opposite orders.
 *
 * <p><b>Credentials</b> are the bootstrap's, exactly as for the old clone: the bootstrap writes a git
 * credential helper that answers with {@code $QITS_TOKEN}, and every git invocation here inherits
 * it. Nothing in this class sees the token.
 *
 * <p><b>The failure mapping</b> drives host behaviour, not just a log line: no usable {@code git} ⇒
 * {@link InitFailed.Reason#TOOLING_MISSING}; the mirror could not be cloned or fetched, or the
 * worktree or a submodule could not be made ⇒ {@link InitFailed.Reason#CLONE_FAILED}; <b>the
 * mirror is current but the sha is not in it ⇒ {@link InitFailed.Reason#SHA_GONE}</b>, the
 * force-push backstop qits-ci answers by re-reading its config source and discarding the run.
 */
public final class MirrorWorktree implements DaemonMain.Worktree {

  private static final Logger LOG = Logger.getLogger(MirrorWorktree.class);

  /** A submodule's checkout depth, as the dossier names it. */
  static final int SUBMODULE_DEPTH = 50;

  /**
   * qits-1253, kept from the clone this replaced: a git host that answers with a transient
   * condition — a 503, a dropped connection — is tried again, twice, rather than failing the run.
   */
  static final List<Duration> DEFAULT_RETRY_DELAYS =
      List.of(Duration.ofSeconds(2), Duration.ofSeconds(5));

  private static final List<Pattern> TRANSIENT_FAILURES =
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

  /** The {@code detail} budget, matching a step chunk's; the tail is kept. */
  static final int DETAIL_MAX_CHARS = 8192;

  /**
   * What a commit id can look like — validated because it crosses from the environment into an
   * argv, and a value that cannot name a commit must not reach git as something it might read as an
   * option.
   */
  private static final Pattern COMMIT_ID = Pattern.compile("[0-9a-fA-F]{7,64}");

  /**
   * One path segment of a cache or work path: the project id, a repository name, the run id. They
   * arrive from the environment and from a checkout's {@code .gitmodules} (untrusted), and become
   * directories, so a {@code ..}, a slash or a leading dot or dash is refused at this boundary.
   */
  private static final Pattern SEGMENT = Pattern.compile("[A-Za-z0-9_][A-Za-z0-9._-]{0,127}");

  /** The project directory of an id-addressed run, whose (projectId, repoName) pair is empty. */
  static final String ID_ADDRESSED = "_by-id";

  private final Path cacheRoot;
  private final Path workRoot;
  private final String repositoryUrl;
  private final String projectId;
  private final String repoName;
  private final String repoId;
  private final String runId;
  private final String sha;
  private final CommandRunner runner;
  private final List<Duration> retryDelays;

  private volatile String shell = "sh";
  private volatile boolean processGroups;

  /** Set once {@code worktree add} succeeded: what {@link #remove()} has to undo. */
  private volatile Path worktree;

  private volatile Path mirror;

  /**
   * @param cacheRoot {@code /cache/git} in a lease; the bare mirrors and their lock files
   * @param workRoot {@code /work} in a lease; the run's worktree is {@code <workRoot>/<runId>}
   * @param projectId with {@code repoName}, names the mirror; both blank on an id-addressed run,
   *     which falls back to {@code <cacheRoot>/_by-id/<repoId>.git}
   */
  public MirrorWorktree(
      Path cacheRoot,
      Path workRoot,
      String repositoryUrl,
      String projectId,
      String repoName,
      String repoId,
      String runId,
      String sha,
      CommandRunner runner,
      List<Duration> retryDelays) {
    this.cacheRoot = cacheRoot;
    this.workRoot = workRoot;
    this.repositoryUrl = repositoryUrl;
    this.projectId = projectId;
    this.repoName = repoName;
    this.repoId = repoId;
    this.runId = runId;
    this.sha = sha;
    this.runner = runner;
    this.retryDelays = List.copyOf(retryDelays);
  }

  /**
   * The outcome of {@link #prepare()}: ready, or a reason the host branches on plus bounded human
   * detail.
   */
  public record Preparation(InitFailed.Reason failure, String detail) {

    public static final Preparation READY = new Preparation(null, null);

    public boolean ready() {
      return failure == null;
    }
  }

  /** Make the worktree, or report why not. Never throws. */
  @Override
  public Preparation prepare() {
    Preparation tooling = probeTooling();
    if (tooling != null) {
      return tooling;
    }
    if (sha == null || !COMMIT_ID.matcher(sha).matches()) {
      // SHA_GONE rather than a new reason: to the host a sha that cannot name a commit is one that
      // no longer exists, and its answer to SHA_GONE re-reads its config source before discarding.
      return new Preparation(
          InitFailed.Reason.SHA_GONE, "QITS_CI_SHA is not a commit id: " + tail(sha));
    }
    if (!segment(runId)) {
      return new Preparation(
          InitFailed.Reason.CLONE_FAILED, "QITS_CI_RUN_ID cannot name a directory: " + tail(runId));
    }
    String[] coordinates = mirrorCoordinates(projectId, repoName, repoId);
    if (coordinates == null) {
      return new Preparation(
          InitFailed.Reason.CLONE_FAILED,
          "no mirror coordinates: QITS_CI_PROJECT_ID/QITS_CI_REPO_NAME and QITS_CI_REPO_ID are"
              + " blank or not path segments");
    }
    Path projectDir = cacheRoot.resolve(coordinates[0]);
    Path mirrorDir = projectDir.resolve(coordinates[1] + ".git");
    Path target = workRoot.resolve(runId);
    mirror = mirrorDir;

    List<Submodule> submodules;
    try (GitLock lock = GitLock.acquire(projectDir.resolve(coordinates[1] + ".lock"))) {
      CommandRunner.Result synced = syncMirror(mirrorDir, repositoryUrl);
      if (!synced.ok()) {
        return new Preparation(InitFailed.Reason.CLONE_FAILED, tail(synced.output()));
      }
      git(mirrorDir, "worktree", "prune");
      if (Files.exists(target)) {
        // A daemon restarted in the same container, or a run id reused: what is there is not ours
        // to keep, and `worktree add` refuses a non-empty directory.
        git(mirrorDir, "worktree", "remove", "--force", target.toString());
        deleteRecursively(target);
        git(mirrorDir, "worktree", "prune");
      }
      CommandRunner.Result present = git(mirrorDir, "cat-file", "-e", sha + "^{commit}");
      if (!present.ok()) {
        return new Preparation(
            InitFailed.Reason.SHA_GONE,
            tail("the mirror, fetched just now, has no commit " + sha + ". " + present.output()));
      }
      Files.createDirectories(workRoot);
      CommandRunner.Result added =
          git(mirrorDir, "worktree", "add", "--detach", target.toString(), sha);
      if (!added.ok()) {
        return new Preparation(InitFailed.Reason.CLONE_FAILED, tail(added.output()));
      }
      worktree = target;
      submodules = new ArrayList<>();
      if (Files.exists(target.resolve(".gitmodules"))) {
        // init resolves each relative url against the mirror's origin and writes it, with `active`,
        // into the config every worktree of this mirror shares — hence under the lock. sync then
        // corrects a url a previous run wrote from an older .gitmodules.
        CommandRunner.Result init = git(target, "submodule", "init");
        CommandRunner.Result sync = init.ok() ? git(target, "submodule", "sync", "--quiet") : init;
        if (!sync.ok()) {
          return new Preparation(InitFailed.Reason.CLONE_FAILED, tail(sync.output()));
        }
        submodules = submodules(target);
      }
    } catch (IOException e) {
      return new Preparation(
          InitFailed.Reason.CLONE_FAILED, tail("the mirror could not be locked or written: " + e));
    }

    for (Submodule submodule : submodules) {
      Path reference = referenceMirror(projectDir, submodule);
      List<String> argv = new ArrayList<>(List.of("submodule", "update", "--init", "--depth"));
      argv.add(String.valueOf(SUBMODULE_DEPTH));
      if (reference != null) {
        argv.add("--reference");
        argv.add(reference.toString());
      }
      argv.add("--");
      argv.add(submodule.path());
      CommandRunner.Result updated = git(target, argv.toArray(String[]::new));
      if (!updated.ok()) {
        return new Preparation(InitFailed.Reason.CLONE_FAILED, tail(updated.output()));
      }
    }
    return Preparation.READY;
  }

  /**
   * {@code git worktree remove --force}, under the mirror's lock, and a plain delete behind it for
   * whatever git did not take. Idempotent, never throws: it runs on every ending, and an ending is
   * no place to fail.
   */
  @Override
  public void remove() {
    Path target = worktree;
    Path mirrorDir = mirror;
    if (target == null || mirrorDir == null) {
      return;
    }
    worktree = null;
    try (GitLock lock = GitLock.acquire(mirrorDir.resolveSibling(lockName(mirrorDir)))) {
      CommandRunner.Result removed = git(mirrorDir, "worktree", "remove", "--force", target.toString());
      if (!removed.ok() || Files.exists(target)) {
        LOG.warnf(
            "ci-daemon: git worktree remove left %s behind (%s); deleting it",
            target, tail(removed.output()));
        deleteRecursively(target);
        git(mirrorDir, "worktree", "prune");
      }
    } catch (IOException e) {
      LOG.warnf("ci-daemon could not remove the worktree %s: %s", target, e.getMessage());
    }
  }

  private static String lockName(Path mirrorDir) {
    String name = mirrorDir.getFileName().toString();
    return name.substring(0, name.length() - ".git".length()) + ".lock";
  }

  /** {@code [projectDir, repoName]} for the mirror, or null when nothing names one safely. */
  static String[] mirrorCoordinates(String projectId, String repoName, String repoId) {
    if (segment(projectId) && segment(repoName)) {
      return new String[] {projectId, repoName};
    }
    if (blank(projectId) && blank(repoName) && segment(repoId)) {
      return new String[] {ID_ADDRESSED, repoId};
    }
    return null;
  }

  /** Clone the mirror on first use, fetch it on every later one; transient failures retried. */
  private CommandRunner.Result syncMirror(Path mirrorDir, String url) throws IOException {
    boolean exists = Files.isRegularFile(mirrorDir.resolve("HEAD"));
    if (exists) {
      // The url is set every time: it is the run's, and the same repository may be addressed
      // differently from one plane to the next. The mirror's identity is its path, not its url.
      git(mirrorDir, "remote", "set-url", "origin", url);
      return withRetry(() -> git(mirrorDir, "fetch", "--prune", "origin"), null);
    }
    Files.createDirectories(mirrorDir.getParent());
    // `--` so a url beginning with a dash is a positional, never an option.
    return withRetry(
        () -> runner.run(null, "git", "clone", "--mirror", "--", url, mirrorDir.toString()),
        mirrorDir);
  }

  /**
   * A submodule's own mirror for {@code --reference}: used when present, cloned under its own lock
   * when not. Null — no reference, a full fetch from the remote — when its name cannot be a path
   * segment or its clone failed; a missing reference makes the update slower, never wrong.
   */
  private Path referenceMirror(Path projectDir, Submodule submodule) {
    String name = repositoryName(submodule.url());
    if (name == null) {
      return null;
    }
    Path subMirror = projectDir.resolve(name + ".git");
    if (Files.isRegularFile(subMirror.resolve("HEAD"))) {
      return subMirror;
    }
    try (GitLock lock = GitLock.acquire(projectDir.resolve(name + ".lock"))) {
      if (Files.isRegularFile(subMirror.resolve("HEAD"))) {
        return subMirror; // another run made it while this one waited
      }
      CommandRunner.Result cloned = syncMirror(subMirror, submodule.url());
      if (cloned.ok()) {
        return subMirror;
      }
      LOG.warnf(
          "ci-daemon could not create the mirror for submodule %s (%s); updating it without one",
          submodule.path(), tail(cloned.output()));
    } catch (IOException e) {
      LOG.warnf("ci-daemon could not lock the mirror for submodule %s: %s", submodule.path(), e);
    }
    return null;
  }

  /** The last path segment of a repository url, {@code .git} stripped, if it is a safe segment. */
  static String repositoryName(String url) {
    if (url == null) {
      return null;
    }
    String trimmed = url.strip();
    while (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    int slash = Math.max(trimmed.lastIndexOf('/'), trimmed.lastIndexOf(':'));
    String last = slash >= 0 ? trimmed.substring(slash + 1) : trimmed;
    if (last.endsWith(".git")) {
      last = last.substring(0, last.length() - ".git".length());
    }
    return segment(last) ? last : null;
  }

  private record Submodule(String name, String path, String url) {}

  /** The worktree's submodules, as {@code .gitmodules} names them and init resolved their urls. */
  private List<Submodule> submodules(Path target) {
    CommandRunner.Result paths =
        git(target, "config", "-f", ".gitmodules", "--get-regexp", "^submodule\\..*\\.path$");
    List<Submodule> found = new ArrayList<>();
    if (!paths.ok() || paths.output() == null) {
      return found;
    }
    for (String line : paths.output().split("\n")) {
      int space = line.indexOf(' ');
      if (space <= 0 || !line.startsWith("submodule.") || !line.substring(0, space).endsWith(".path")) {
        continue;
      }
      String name = line.substring("submodule.".length(), space - ".path".length());
      String path = line.substring(space + 1).strip();
      CommandRunner.Result url = git(target, "config", "--get", "submodule." + name + ".url");
      found.add(new Submodule(name, path, url.ok() ? url.output().strip() : null));
    }
    return found;
  }

  @FunctionalInterface
  private interface Attempt {
    CommandRunner.Result run();
  }

  /**
   * qits-1253's retry: only a failure whose output matches {@link #TRANSIENT_FAILURES} is tried
   * again; a 4xx or a bad url fails after one attempt. A clone that failed is removed before the
   * next attempt, because git refuses to clone into what it left.
   */
  private CommandRunner.Result withRetry(Attempt attempt, Path partialClone) {
    CommandRunner.Result result = attempt.run();
    for (int i = 0; i < retryDelays.size(); i++) {
      String reason = result.ok() ? null : transientReason(result.output());
      if (reason == null) {
        return result;
      }
      Duration delay = retryDelays.get(i);
      LOG.warnf(
          "ci-daemon git attempt %d/%d failed with a transient error (%s); retrying in %s",
          i + 1, retryDelays.size() + 1, reason, delay);
      if (!sleep(delay)) {
        return result;
      }
      if (partialClone != null) {
        try {
          deleteRecursively(partialClone);
        } catch (IOException e) {
          LOG.warnf("ci-daemon could not clear %s before retrying: %s", partialClone, e.getMessage());
        }
      }
      result = attempt.run();
    }
    return result;
  }

  private CommandRunner.Result git(Path dir, String... args) {
    String[] argv = new String[args.length + 3];
    argv[0] = "git";
    argv[1] = "-C";
    argv[2] = dir.toString();
    System.arraycopy(args, 0, argv, 3, args.length);
    return runner.run(null, argv);
  }

  private static String transientReason(String output) {
    if (output == null) {
      return null;
    }
    for (Pattern pattern : TRANSIENT_FAILURES) {
      Matcher matcher = pattern.matcher(output);
      if (matcher.find()) {
        return matcher.group();
      }
    }
    return null;
  }

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

  static void deleteRecursively(Path path) throws IOException {
    if (!Files.exists(path, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
      return;
    }
    if (Files.isDirectory(path, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
      try (Stream<Path> entries = Files.list(path)) {
        for (Path entry : entries.toList()) {
          deleteRecursively(entry);
        }
      }
    }
    Files.deleteIfExists(path);
  }

  /**
   * {@code git} is required; {@code bash} is preferred and {@code sh} the fallback (see the README
   * on what demanding bash once cost); {@code setsid} decides whether a step gets a process group of
   * its own, which is what lets {@code Cancel} and a timeout take its background children with it.
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
    processGroups = runner.run(null, "setsid", "--version").ok();
    return null;
  }

  private static boolean segment(String value) {
    return value != null && SEGMENT.matcher(value).matches() && !value.contains("..");
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private static String tail(String text) {
    if (text == null) {
      return null;
    }
    return text.length() <= DETAIL_MAX_CHARS
        ? text
        : text.substring(text.length() - DETAIL_MAX_CHARS);
  }

  /** {@code <workRoot>/<runId>}: every step's working directory. */
  public File directory() {
    return workRoot.resolve(runId == null ? "" : runId).toFile();
  }

  /** {@code bash} when the image has it, {@code sh} otherwise. Read after {@link #prepare()}. */
  public String shell() {
    return shell;
  }

  /** Whether {@code setsid} is there to give each step its own process group. */
  public boolean processGroups() {
    return processGroups;
  }
}
