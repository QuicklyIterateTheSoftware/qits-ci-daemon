package eu.wohlben.qits.cidaemon;

import eu.wohlben.qits.cidaemon.protocol.ContinueFailed;
import eu.wohlben.qits.cidaemon.protocol.InitFailed;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
 *     expire locked qits-ci entries older than the bound; git worktree prune --expire &lt;bound&gt;
 *     git worktree add --lock --reason "qits-ci run &lt;runId&gt; &lt;ISO instant&gt;" --detach
 *         &lt;work&gt;/&lt;runId&gt; &lt;sha&gt;
 *     git submodule init + sync, each url set to the sibling beside the run's origin
 *                                         (they write the mirror's shared config)
 *   then, per submodule:
 *     its own mirror, cloned under its own lock if missing
 *     git -c submodule.&lt;name&gt;.url=&lt;sibling&gt; submodule update --init --depth 50
 *         [--reference &lt;its mirror&gt;] -- &lt;path&gt;
 *   on Continue (qits-732), the lease's next run in the same worktree:
 *     under the lock: git fetch origin +refs/tags/&lt;branch&gt;:refs/tags/&lt;branch&gt;
 *                     | git fetch origin &lt;sha&gt;          (skipped when the mirror has it)
 *     git checkout --force --detach &lt;sha&gt;              (no clean: target/ stays warm)
 *   on Close (under the lock again), this run's entry and nothing else:
 *     git worktree unlock; git worktree remove --force   | rm the dir and worktrees/&lt;id&gt;
 * </pre>
 *
 * <p><b>Why every worktree is locked (qits-1541).</b> Every ci-slot container on a target has a
 * {@code /work} of its own and shares {@code /cache/git}, so from inside one container another
 * run's worktree directory does not exist — and to {@code git worktree prune} a worktree whose
 * directory is missing is a dead one. An unconditional prune before the add therefore deleted every
 * other live run's {@code worktrees/<id>}, and with it the submodule gitdirs under its {@code
 * modules/}: six runs started together on one target all died at the submodule clone. The flock
 * never helped, because it serialises the commands, not the judgement. A locked worktree is one
 * prune never touches, so the lock is taken by the {@code add} itself — there is no add-then-lock
 * window for another container's prune to land in — and given up only by this run's own Close.
 *
 * <p><b>Why an expiry, and why prune stays.</b> A container killed before its Close leaves a locked
 * entry nothing will ever unlock. The lock's reason carries the moment it was taken, so the next
 * run on the mirror takes away a qits-ci entry older than {@code staleAfter} — a bound set far past
 * the longest lease, because removing a live entry breaks that run outright, while leaking a dead
 * one costs a few kilobytes for a day. A lock without a qits-ci reason has no age to judge and is
 * never touched. {@code git worktree prune --expire <bound>} stays beside it for the unlocked
 * entries a daemon from before this change leaves behind: they are live for a while too, and the
 * same bound keeps prune from judging one before it could possibly be dead.
 *
 * <p><b>Submodule urls.</b> The estate's {@code .gitmodules} name the public GitHub address, and a
 * lease must fetch from the platform's git host it was handed, as the java-service release step's
 * own {@code git -c submodule.$name.url=$origin_parent/$name submodule update} did before this
 * class recursed. So each submodule's url is the repository of the same last path segment beside
 * {@link #repositoryUrl}, written into the shared config under the lock (what a step's own {@code
 * git submodule} later reads) and passed again with {@code -c} on the update, which runs outside
 * the lock where another run's {@code sync} could rewrite the shared value in between.
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
 * only into this worktree's own {@code worktrees/<id>/modules/}, which the worktree's lock protects
 * like the rest of the entry; what they share is a read of the submodule's mirror through {@code
 * objects/info/alternates}, which nothing here ever deletes — and a submodule's own mirror is
 * created under its own lock, never while the parent's is held, so the two locks are never taken in
 * opposite orders.
 *
 * <p><b>Credentials</b> are the bootstrap's, exactly as for the old clone: the bootstrap writes a git
 * credential helper that answers with {@code $QITS_TOKEN}, and every git invocation here inherits
 * it. Nothing in this class reads the token: after a {@code Continue} the new run's {@code
 * QITS_TOKEN} reaches the helper as an environment entry laid over git's child, never as an argv.
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

  /**
   * What a {@code Continue}'s branch — a version tag — may look like before it is spliced into a
   * refspec: it arrives over the socket and becomes an argv element, so a leading dash, a colon, a
   * {@code ..} or anything else git's ref grammar or option parser would read is refused. A name
   * that does not match is not an error; the sha is fetched directly instead.
   */
  private static final Pattern TAG_NAME = Pattern.compile("[A-Za-z0-9_][A-Za-z0-9._/-]{0,254}");

  /**
   * How old a qits-ci lock may grow before the next run on the mirror judges its run dead: a day,
   * against a lease that is a handful of steps of at most an hour each.
   */
  static final Duration DEFAULT_STALE_AFTER = Duration.ofHours(24);

  /** The lock reason's prefix; what marks an entry as this daemon's to expire. */
  static final String LOCK_REASON_PREFIX = "qits-ci run ";

  private static final Pattern LOCK_REASON =
      Pattern.compile("qits-ci run (\\S+) (\\S+)\\s*");

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
  private final Duration staleAfter;

  private volatile String shell = "sh";
  private volatile boolean processGroups;

  /** Set once {@code worktree add} succeeded: what {@link #remove()} has to undo. */
  private volatile Path worktree;

  /** The run's own {@code <mirror>/worktrees/<id>}, read back from the worktree's {@code .git}. */
  private volatile Path adminDir;

  private volatile Path mirror;

  /**
   * @param cacheRoot {@code /cache/git} in a lease; the bare mirrors and their lock files
   * @param workRoot {@code /work} in a lease; the run's worktree is {@code <workRoot>/<runId>}
   * @param projectId with {@code repoName}, names the mirror; both blank on an id-addressed run,
   *     which falls back to {@code <cacheRoot>/_by-id/<repoId>.git}
   * @param staleAfter how old another run's locked entry on the mirror must be before this run
   *     takes it away as the leftover of a container that died without a Close
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
      List<Duration> retryDelays,
      Duration staleAfter) {
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
    this.staleAfter = staleAfter;
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

  /** The outcome of {@link #continueAt}: done, or a reason the host branches on plus detail. */
  public record Continuation(ContinueFailed.Reason failure, String detail) {

    public static final Continuation DONE = new Continuation(null, null);

    public boolean done() {
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
      Instant now = Instant.now();
      expireStaleEntries(mirrorDir, now);
      // Never a bare prune: see the class comment. Locked entries are skipped by prune whatever the
      // expiry; the expiry is for the unlocked ones a daemon from before qits-1541 leaves.
      git(mirrorDir, "worktree", "prune", "--expire", staleAfter.toSeconds() + ".seconds.ago");
      Path ownAdmin = mirrorDir.resolve("worktrees").resolve(runId);
      if (Files.exists(target) || Files.exists(ownAdmin)) {
        // A daemon restarted in the same container, or this run id's entry from an earlier attempt:
        // what is there is not ours to keep, and `worktree add` refuses a non-empty directory. The
        // run id is unique to this run, so the entry it names is this run's and nobody else's.
        removeOwnEntry(mirrorDir, target, adminDirOf(mirrorDir, target));
      }
      CommandRunner.Result present = git(mirrorDir, "cat-file", "-e", sha + "^{commit}");
      if (!present.ok()) {
        return new Preparation(
            InitFailed.Reason.SHA_GONE,
            tail("the mirror, fetched just now, has no commit " + sha + ". " + present.output()));
      }
      Files.createDirectories(workRoot);
      String reason = LOCK_REASON_PREFIX + runId + " " + now;
      CommandRunner.Result added =
          git(
              mirrorDir,
              "worktree",
              "add",
              "--lock",
              "--reason",
              reason,
              "--detach",
              target.toString(),
              sha);
      boolean reasonRecorded = added.ok();
      if (!added.ok() && rejectsReason(added.output())) {
        // git before 2.31 knows `add --lock` but not `--reason`. The lock is still taken by the add
        // itself; the reason, which is what lets a later run judge its age, is written after.
        added = git(mirrorDir, "worktree", "add", "--lock", "--detach", target.toString(), sha);
      }
      if (!added.ok()) {
        return new Preparation(InitFailed.Reason.CLONE_FAILED, tail(added.output()));
      }
      worktree = target;
      adminDir = adminDirOf(mirrorDir, target);
      if (!reasonRecorded) {
        Files.writeString(adminDir.resolve("locked"), reason + "\n");
      }
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
        submodules = new ArrayList<>();
        for (Submodule found : submodules(target)) {
          String sibling =
              configKeyName(found.name()) ? siblingUrl(repositoryUrl, found.url()) : null;
          if (sibling == null) {
            LOG.warnf(
                "ci-daemon: submodule %s has no sibling beside the run's origin; using %s",
                found.path(), found.url());
            submodules.add(found);
            continue;
          }
          CommandRunner.Result set =
              git(target, "config", "submodule." + found.name() + ".url", sibling);
          if (!set.ok()) {
            return new Preparation(InitFailed.Reason.CLONE_FAILED, tail(set.output()));
          }
          submodules.add(new Submodule(found.name(), found.path(), sibling));
        }
      }
    } catch (IOException e) {
      return new Preparation(
          InitFailed.Reason.CLONE_FAILED, tail("the mirror could not be locked or written: " + e));
    }

    for (Submodule submodule : submodules) {
      Path reference = referenceMirror(projectDir, submodule);
      List<String> argv = new ArrayList<>();
      if (submodule.url() != null && configKeyName(submodule.name())) {
        argv.add("-c");
        argv.add("submodule." + submodule.name() + ".url=" + submodule.url());
      }
      argv.addAll(List.of("submodule", "update", "--init", "--depth"));
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
   * Move the existing worktree to the lease's next run (qits-732): fetch {@code refs/tags/<branch>}
   * into the mirror — or the sha itself when the tag cannot be fetched — under the mirror's lock,
   * then {@code git checkout --force --detach <sha>} in the same directory. Never throws.
   *
   * <p><b>No {@code git clean}, on purpose.</b> Untracked and ignored files — {@code target/},
   * {@code node_modules/}, whatever the first run built — survive the move; that warm tree is what
   * sharing the lease between a release request's two runs is for, and the second run's commit is
   * the first's plus a version bump. {@code --force} only discards changes to tracked files, which a
   * build that edits its own sources leaves behind and the new commit must not inherit.
   *
   * <p>{@code env} is laid over every git child here: it carries the new run's {@code QITS_TOKEN},
   * which the bootstrap's credential helper answers with. The directory does not move — it is still
   * {@code <workRoot>/<first runId>}, locked under the first run's reason, and removed by the Close
   * as before. Submodules are not updated: the tag is the fold it was cut from plus a version
   * commit, which moves no gitlink.
   */
  @Override
  public Continuation continueAt(String branch, String sha, Map<String, String> env) {
    Path target = worktree;
    Path mirrorDir = mirror;
    if (target == null || mirrorDir == null) {
      return new Continuation(
          ContinueFailed.Reason.CHECKOUT_FAILED, "there is no worktree to move: it was never made");
    }
    if (sha == null || !COMMIT_ID.matcher(sha).matches()) {
      // Crosses from the socket into an argv; see COMMIT_ID.
      return new Continuation(
          ContinueFailed.Reason.FETCH_FAILED, "the Continue's sha is not a commit id: " + tail(sha));
    }
    Map<String, String> gitEnv = env == null ? Map.of() : env;
    try (GitLock lock = GitLock.acquire(mirrorDir.resolveSibling(lockName(mirrorDir)))) {
      if (!hasCommit(mirrorDir, sha)) {
        StringBuilder said = new StringBuilder();
        if (branch != null && TAG_NAME.matcher(branch).matches() && !branch.contains("..")
            && !branch.endsWith("/") && !branch.endsWith(".lock")) {
          String ref = "refs/tags/" + branch;
          CommandRunner.Result byTag =
              withRetry(() -> git(gitEnv, mirrorDir, "fetch", "origin", "+" + ref + ":" + ref), null);
          if (!byTag.ok()) {
            said.append(byTag.output());
          }
        }
        if (!hasCommit(mirrorDir, sha)) {
          CommandRunner.Result bySha =
              withRetry(() -> git(gitEnv, mirrorDir, "fetch", "origin", sha), null);
          if (!bySha.ok()) {
            said.append(bySha.output());
          }
        }
        if (!hasCommit(mirrorDir, sha)) {
          return new Continuation(
              ContinueFailed.Reason.FETCH_FAILED,
              tail("the mirror has no commit " + sha + " after fetching refs/tags/" + branch
                  + " and the sha itself. " + said));
        }
      }
    } catch (IOException e) {
      return new Continuation(
          ContinueFailed.Reason.FETCH_FAILED, tail("the mirror could not be locked: " + e));
    }
    CommandRunner.Result checkout = git(gitEnv, target, "checkout", "--force", "--detach", sha);
    if (!checkout.ok()) {
      return new Continuation(ContinueFailed.Reason.CHECKOUT_FAILED, tail(checkout.output()));
    }
    CommandRunner.Result head = git(target, "rev-parse", "HEAD");
    CommandRunner.Result wanted = git(target, "rev-parse", "--verify", sha + "^{commit}");
    if (!head.ok()
        || !wanted.ok()
        || !head.output().strip().equals(wanted.output().strip())) {
      return new Continuation(
          ContinueFailed.Reason.CHECKOUT_FAILED,
          tail("the worktree is at " + head.output().strip() + " after checking out " + sha));
    }
    return Continuation.DONE;
  }

  private boolean hasCommit(Path mirrorDir, String sha) {
    return git(mirrorDir, "cat-file", "-e", sha + "^{commit}").ok();
  }

  /**
   * This run's entry and nothing else, under the mirror's lock: unlock, {@code git worktree remove
   * --force}, and a plain delete of the directory and of {@code worktrees/<id>} behind it for
   * whatever git did not take — git refuses to remove a worktree holding submodules. Never a {@code
   * prune}: from this container every other run's worktree looks dead. Idempotent, never throws: it
   * runs on every ending, and an ending is no place to fail.
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
      removeOwnEntry(mirrorDir, target, adminDir);
    } catch (IOException e) {
      LOG.warnf("ci-daemon could not remove the worktree %s: %s", target, e.getMessage());
    }
  }

  /** Called under the mirror's lock. {@code admin} is this run's own {@code worktrees/<id>}. */
  private void removeOwnEntry(Path mirrorDir, Path target, Path admin) throws IOException {
    git(mirrorDir, "worktree", "unlock", target.toString());
    CommandRunner.Result removed =
        git(mirrorDir, "worktree", "remove", "--force", target.toString());
    if (!removed.ok() || Files.exists(target) || Files.exists(admin)) {
      LOG.infof(
          "ci-daemon: git worktree remove left %s behind (%s); deleting it and %s",
          target, tail(removed.output()), admin);
      deleteRecursively(target);
      deleteRecursively(admin);
    }
  }

  /**
   * The worktree's own administrative directory, as its {@code .git} file names it — git suffixes
   * the name when it collides — and {@code worktrees/<runId>} when that file is gone or names
   * anything outside this mirror's {@code worktrees/}, which is never this run's to delete.
   */
  private Path adminDirOf(Path mirrorDir, Path target) {
    Path worktrees = mirrorDir.resolve("worktrees");
    Path fallback = worktrees.resolve(runId);
    try {
      String dotGit = Files.readString(target.resolve(".git")).strip();
      if (dotGit.startsWith("gitdir:")) {
        Path named = Path.of(dotGit.substring("gitdir:".length()).strip()).normalize();
        if (named.getParent() != null
            && named.getParent().equals(worktrees.toAbsolutePath().normalize())) {
          return named;
        }
      }
    } catch (IOException | RuntimeException e) {
      // no readable .git: the fallback is the name git gives an uncontested add
    }
    return fallback;
  }

  /**
   * Under the mirror's lock: every entry locked by a qits-ci run whose lock is older than {@link
   * #staleAfter} is the leftover of a container that died before its Close; it is unlocked and its
   * metadata removed. An entry without a qits-ci reason, or younger than the bound, is left alone.
   * Its worktree directory is in that dead container's {@code /work}, out of reach and gone with
   * it.
   */
  private void expireStaleEntries(Path mirrorDir, Instant now) {
    Path worktrees = mirrorDir.resolve("worktrees");
    if (!Files.isDirectory(worktrees)) {
      return;
    }
    Instant bound = now.minus(staleAfter);
    try (Stream<Path> entries = Files.list(worktrees)) {
      for (Path entry : entries.toList()) {
        Path locked = entry.resolve("locked");
        if (!Files.isRegularFile(locked, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
          continue;
        }
        String reason = Files.readString(locked);
        Instant at = lockedAt(reason);
        if (at == null || at.isAfter(bound)) {
          continue;
        }
        LOG.warnf(
            "ci-daemon: expiring %s, locked at %s by a run that never closed (%s)",
            entry.getFileName(), at, reason.strip());
        Files.deleteIfExists(locked);
        deleteRecursively(entry);
      }
    } catch (IOException e) {
      // Leaking a dead entry for one more run costs kilobytes; failing this run for it costs a run.
      LOG.warnf("ci-daemon could not expire stale worktrees of %s: %s", mirrorDir, e.getMessage());
    }
  }

  /** The instant a qits-ci lock reason records, or null when it is not one of ours. */
  static Instant lockedAt(String reason) {
    if (reason == null) {
      return null;
    }
    Matcher matcher = LOCK_REASON.matcher(reason.strip());
    if (!matcher.matches()) {
      return null;
    }
    try {
      return Instant.parse(matcher.group(2));
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  private static boolean rejectsReason(String output) {
    return output != null && output.contains("unknown option") && output.contains("reason");
  }

  /**
   * The repository beside {@code repositoryUrl} with {@code submoduleUrl}'s last path segment — the
   * same {@code ${QITS_CI_REPOSITORY_URL%/*}/<name>} the release step used — or null when either
   * has no such segment. The segment is kept as written, {@code .git} and all.
   */
  static String siblingUrl(String repositoryUrl, String submoduleUrl) {
    if (repositoryUrl == null || repositoryName(submoduleUrl) == null) {
      return null;
    }
    String origin = stripTrailingSlashes(repositoryUrl.strip());
    int slash = origin.lastIndexOf('/');
    if (slash <= 0) {
      return null;
    }
    String sub = stripTrailingSlashes(submoduleUrl.strip());
    String last = sub.substring(Math.max(sub.lastIndexOf('/'), sub.lastIndexOf(':')) + 1);
    return origin.substring(0, slash) + "/" + last;
  }

  private static String stripTrailingSlashes(String value) {
    String trimmed = value;
    while (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed;
  }

  /**
   * Whether a submodule name from the untrusted {@code .gitmodules} can sit in a {@code -c
   * key=value} argument: an {@code =} would end the key early, a control character would not
   * survive the round trip.
   */
  private static boolean configKeyName(String name) {
    return name != null && !name.isEmpty() && name.chars().noneMatch(c -> c == '=' || c < 0x20);
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
    return git(Map.of(), dir, args);
  }

  private CommandRunner.Result git(Map<String, String> env, Path dir, String... args) {
    String[] argv = new String[args.length + 3];
    argv[0] = "git";
    argv[1] = "-C";
    argv[2] = dir.toString();
    System.arraycopy(args, 0, argv, 3, args.length);
    return env.isEmpty() ? runner.run(null, argv) : runner.run(null, env, argv);
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
