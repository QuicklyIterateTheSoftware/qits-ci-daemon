package eu.wohlben.qits.cidaemon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.cidaemon.protocol.InitFailed;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/**
 * Drives a real {@code git} against bare repositories the test builds in a temp directory, because
 * the thing under test <em>is</em> the integration with git: the mirror, the worktree on it, the
 * submodule reference and the lock around them. The runner is the production one with one setting
 * added — {@code protocol.file.allow=always}, which git (2.38.1 on) needs before it clones a
 * submodule from a local path, and which a lease never needs because its remotes are https.
 */
@EnabledOnOs(OS.LINUX)
class MirrorWorktreeTest {

  @TempDir Path tmp;

  /** Every git argv the runner saw, for asserting which of clone and fetch happened. */
  private final List<List<String>> commands = Collections.synchronizedList(new ArrayList<>());

  @Test
  void theFirstRunClonesABareMirrorAndChecksTheShaOutInAWorktreeOfIt() throws Exception {
    Path remote = remote("repo", "first\n", "second\n", "third\n");
    String wanted = Git.sha(remote, "HEAD~1");

    MirrorWorktree worktree = worktree(remote, "r1", wanted);
    MirrorWorktree.Preparation preparation = worktree.prepare();

    assertTrue(preparation.ready(), () -> "expected a ready worktree, got " + preparation);
    Path mirror = tmp.resolve("cache/p1/repo.git");
    assertEquals("true", Git.run(mirror, "rev-parse", "--is-bare-repository").strip());
    assertEquals(tmp.resolve("work/r1").toFile(), worktree.directory());
    assertEquals(wanted, Git.sha(tmp.resolve("work/r1"), "HEAD"));
    assertEquals("second\n", Files.readString(tmp.resolve("work/r1/file.txt")));
    assertEquals(1, count("clone", "--mirror"));
    assertEquals(0, count("fetch", "--prune", "origin"));
    assertTrue(Files.exists(tmp.resolve("cache/p1/repo.lock")), "the lock file sits beside it");
  }

  @Test
  void theSecondRunFetchesTheMirrorRatherThanCloningItAndSeesTheNewCommit() throws Exception {
    Path remote = remote("repo", "first\n");
    assertTrue(worktree(remote, "r1", Git.sha(remote, "HEAD")).prepare().ready());

    String pushed = Git.push(tmp, remote, "pushed later\n");
    MirrorWorktree.Preparation second = worktree(remote, "r2", pushed).prepare();

    assertTrue(second.ready(), second::toString);
    assertEquals(pushed, Git.sha(tmp.resolve("work/r2"), "HEAD"));
    assertEquals(1, count("clone", "--mirror"), "one clone, ever");
    assertEquals(1, count("fetch", "--prune", "origin"));
    // Both runs' worktrees hang off the one mirror.
    String list = Git.run(tmp.resolve("cache/p1/repo.git"), "worktree", "list");
    assertTrue(list.contains(tmp.resolve("work/r1").toString()), list);
    assertTrue(list.contains(tmp.resolve("work/r2").toString()), list);
  }

  @Test
  void twoConcurrentRunsSerialiseOnTheMirrorsLock() throws Exception {
    Path remote = remote("repo", "first\n");
    String sha = Git.sha(remote, "HEAD");
    AtomicInteger inside = new AtomicInteger();
    AtomicInteger mostInside = new AtomicInteger();
    CommandRunner slow =
        (dir, argv) -> {
          boolean writesTheMirror =
              List.of(argv).contains("clone") || List.of(argv).contains("fetch");
          if (!writesTheMirror) {
            return runner().run(dir, argv);
          }
          mostInside.accumulateAndGet(inside.incrementAndGet(), Math::max);
          try {
            Thread.sleep(300); // wide enough that two unserialised runs would overlap
            return runner().run(dir, argv);
          } catch (InterruptedException e) {
            throw new IllegalStateException(e);
          } finally {
            inside.decrementAndGet();
          }
        };

    CompletableFuture<MirrorWorktree.Preparation> one =
        CompletableFuture.supplyAsync(() -> worktree(remote, "r1", sha, slow).prepare());
    CompletableFuture<MirrorWorktree.Preparation> two =
        CompletableFuture.supplyAsync(() -> worktree(remote, "r2", sha, slow).prepare());

    assertTrue(one.get(60, TimeUnit.SECONDS).ready());
    assertTrue(two.get(60, TimeUnit.SECONDS).ready());
    assertEquals(1, mostInside.get(), "the mirror had two writers at once");
    // Whichever came second found the first one's mirror and fetched it.
    assertEquals(1, count("clone", "--mirror"));
    assertEquals(1, count("fetch", "--prune", "origin"));
  }

  @Test
  void aRunWaitsForTheMirrorLockSomeoneElseHolds() throws Exception {
    Path remote = remote("repo", "first\n");
    String sha = Git.sha(remote, "HEAD");

    CompletableFuture<MirrorWorktree.Preparation> waiting;
    try (GitLock held = GitLock.acquire(tmp.resolve("cache/p1/repo.lock"))) {
      waiting = CompletableFuture.supplyAsync(() -> worktree(remote, "r1", sha).prepare());
      Thread.sleep(500);
      assertFalse(waiting.isDone(), "the run went ahead under somebody else's lock");
      assertEquals(0, count("clone", "--mirror"));
    }

    assertTrue(waiting.get(60, TimeUnit.SECONDS).ready());
  }

  @Test
  void aSubmoduleIsCheckedOutWithItsOwnMirrorAsTheReference() throws Exception {
    Path sub = remote("sub", "sub content\n");
    Path remote = remoteWithSubmodule("repo", sub, "libs/sub");
    String sha = Git.sha(remote, "HEAD");

    MirrorWorktree.Preparation preparation = worktree(remote, "r1", sha).prepare();

    assertTrue(preparation.ready(), preparation::toString);
    Path checkout = tmp.resolve("work/r1/libs/sub");
    assertEquals("sub content\n", Files.readString(checkout.resolve("file.txt")));
    // The missing mirror was made first, under its own lock, beside the superproject's.
    Path subMirror = tmp.resolve("cache/p1/sub.git");
    assertEquals("true", Git.run(subMirror, "rev-parse", "--is-bare-repository").strip());
    assertTrue(Files.exists(tmp.resolve("cache/p1/sub.lock")));
    assertEquals(1, count("--reference", subMirror.toString()));
    assertEquals(1, count("--depth", "50"));
    // And the submodule's objects are borrowed from it rather than fetched again.
    Path gitDir = Path.of(Git.run(checkout, "rev-parse", "--absolute-git-dir").strip());
    String alternates = Files.readString(gitDir.resolve("objects/info/alternates"));
    assertTrue(alternates.contains(subMirror.resolve("objects").toString()), alternates);
  }

  @Test
  void anExistingSubmoduleMirrorIsReusedRatherThanClonedAgain() throws Exception {
    Path sub = remote("sub", "sub content\n");
    Path remote = remoteWithSubmodule("repo", sub, "libs/sub");
    String sha = Git.sha(remote, "HEAD");
    assertTrue(worktree(remote, "r1", sha).prepare().ready());

    assertTrue(worktree(remote, "r2", sha).prepare().ready());

    // Two clones in all: the superproject's mirror and the submodule's, each once.
    assertEquals(2, count("clone", "--mirror"));
    assertEquals(2, count("--reference", tmp.resolve("cache/p1/sub.git").toString()));
  }

  @Test
  void removeTakesTheWorktreeAndItsRegistrationAwayAndLeavesTheMirror() throws Exception {
    Path sub = remote("sub", "sub content\n");
    Path remote = remoteWithSubmodule("repo", sub, "libs/sub");
    MirrorWorktree worktree = worktree(remote, "r1", Git.sha(remote, "HEAD"));
    assertTrue(worktree.prepare().ready());

    worktree.remove();
    worktree.remove(); // idempotent: every ending calls it

    assertFalse(Files.exists(tmp.resolve("work/r1")));
    Path mirror = tmp.resolve("cache/p1/repo.git");
    assertFalse(Git.run(mirror, "worktree", "list").contains("work/r1"));
    assertTrue(Files.exists(mirror.resolve("HEAD")), "the mirror is the cache; it stays");
  }

  @Test
  void aShaTheFreshMirrorDoesNotHoldIsShaGoneAndNotAFailedClone() throws Exception {
    Path remote = remote("repo", "first\n");

    MirrorWorktree.Preparation preparation =
        worktree(remote, "r1", "0123456789abcdef0123456789abcdef01234567").prepare();

    assertEquals(InitFailed.Reason.SHA_GONE, preparation.failure());
    assertFalse(Files.exists(tmp.resolve("work/r1")));
  }

  @Test
  void aRepositoryThatCannotBeMirroredIsCloneFailed() {
    MirrorWorktree.Preparation preparation =
        worktree(tmp.resolve("no-such-repository.git"), "r1", "0123456789abcdef").prepare();

    assertEquals(InitFailed.Reason.CLONE_FAILED, preparation.failure());
  }

  @Test
  void aTransientFailureFromTheGitHostIsRetriedAndThePartialMirrorClearedFirst() throws Exception {
    Path remote = remote("repo", "first\n");
    AtomicInteger clones = new AtomicInteger();
    CommandRunner flaky =
        (dir, argv) -> {
          if (List.of(argv).contains("clone") && clones.incrementAndGet() == 1) {
            runner().run(dir, argv); // leaves a mirror behind, as a clone cut off midway does
            return new CommandRunner.Result(128, "fatal: early EOF");
          }
          return runner().run(dir, argv);
        };

    MirrorWorktree.Preparation preparation =
        worktree(remote, "r1", Git.sha(remote, "HEAD"), flaky).prepare();

    assertTrue(preparation.ready(), preparation::toString);
    assertEquals(2, clones.get());
  }

  @Test
  void aRunIdThatIsNotOnePathSegmentIsRefusedBeforeAnythingIsWritten() throws Exception {
    Path remote = remote("repo", "first\n");

    MirrorWorktree.Preparation preparation =
        worktree(remote, "../escape", Git.sha(remote, "HEAD")).prepare();

    assertEquals(InitFailed.Reason.CLONE_FAILED, preparation.failure());
    assertFalse(Files.exists(tmp.resolve("cache")));
  }

  @Test
  void anIdAddressedRunFallsBackToItsRepositoryIdForTheMirror() {
    assertEquals(
        List.of(MirrorWorktree.ID_ADDRESSED, "repo-uuid"),
        List.of(MirrorWorktree.mirrorCoordinates("", "", "repo-uuid")));
    assertEquals(
        List.of("p1", "repo"), List.of(MirrorWorktree.mirrorCoordinates("p1", "repo", "x")));
    assertNull(MirrorWorktree.mirrorCoordinates("p1", "..", "x"));
    assertNull(MirrorWorktree.mirrorCoordinates("", "", ""));
  }

  @Test
  void aSubmodulesMirrorIsNamedByTheLastSegmentOfItsUrl() {
    assertEquals("sub", MirrorWorktree.repositoryName("https://git.example/p/sub.git"));
    assertEquals("sub", MirrorWorktree.repositoryName("git@example:p/sub.git/"));
    assertEquals("sub", MirrorWorktree.repositoryName("/srv/git/sub"));
    assertNull(MirrorWorktree.repositoryName("https://git.example/p/.."));
    assertNull(MirrorWorktree.repositoryName(null));
  }

  @Test
  void anImageWithoutGitIsToolingMissing() {
    CommandRunner noGit = (dir, argv) -> new CommandRunner.Result(-1, "No such file or directory");

    MirrorWorktree.Preparation preparation =
        worktree(tmp.resolve("x.git"), "r1", "0123456789abcdef", noGit).prepare();

    assertEquals(InitFailed.Reason.TOOLING_MISSING, preparation.failure());
  }

  @Test
  void bashAndSetsidAreProbedSoAStepKnowsItsShellAndWhetherItGetsAProcessGroup() {
    CommandRunner gitOnly =
        (dir, argv) ->
            argv[0].equals("git")
                ? runner().run(dir, argv)
                : new CommandRunner.Result(-1, "not here");
    MirrorWorktree worktree = worktree(tmp.resolve("x.git"), "r1", "0123456789abcdef", gitOnly);

    worktree.prepare(); // fails later, at the clone; the probe has run by then

    assertEquals("sh", worktree.shell());
    assertFalse(worktree.processGroups());
  }

  // --- harness ------------------------------------------------------------------------------------

  private MirrorWorktree worktree(Path remote, String runId, String sha) {
    return worktree(remote, runId, sha, runner());
  }

  private MirrorWorktree worktree(Path remote, String runId, String sha, CommandRunner runner) {
    return new MirrorWorktree(
        tmp.resolve("cache"),
        tmp.resolve("work"),
        remote.toString(),
        "p1",
        repoName(remote),
        "repo-id",
        runId,
        sha,
        runner,
        List.of(Duration.ZERO, Duration.ZERO));
  }

  private static String repoName(Path remote) {
    String name = remote.getFileName().toString();
    return name.endsWith(".git") ? name.substring(0, name.length() - 4) : name;
  }

  /** The production runner, recording, with local-path submodule clones allowed. */
  private CommandRunner runner() {
    CommandRunner forking = CommandRunner.forking(60);
    return (dir, argv) -> {
      commands.add(List.of(argv));
      if (argv.length > 0 && argv[0].equals("git") && !(argv.length == 2 && argv[1].equals("--version"))) {
        String[] allowed = new String[argv.length + 2];
        allowed[0] = "git";
        allowed[1] = "-c";
        allowed[2] = "protocol.file.allow=always";
        System.arraycopy(argv, 1, allowed, 3, argv.length - 1);
        return forking.run(dir, allowed);
      }
      return forking.run(dir, argv);
    };
  }

  /** How many recorded commands contain {@code words} as consecutive arguments. */
  private long count(String... words) {
    List<String> wanted = List.of(words);
    synchronized (commands) {
      return commands.stream().filter(argv -> Collections.indexOfSubList(argv, wanted) >= 0).count();
    }
  }

  /** A bare repository {@code <tmp>/remote/<name>.git} with one commit of file.txt per content. */
  private Path remote(String name, String... contents) throws Exception {
    Path source = tmp.resolve("src/" + name);
    Files.createDirectories(source);
    Git.run(source, "init", "-q", "-b", "main");
    for (String content : contents) {
      Files.writeString(source.resolve("file.txt"), content);
      Git.run(source, "add", "file.txt");
      Git.run(source, "commit", "-q", "-m", content.strip());
    }
    Path bare = tmp.resolve("remote/" + name + ".git");
    Files.createDirectories(bare.getParent());
    Git.run(tmp, "clone", "-q", "--bare", source.toString(), bare.toString());
    return bare;
  }

  /**
   * A bare superproject whose {@code .gitmodules} names {@code sub} by the relative url {@code
   * ../sub.git} — the estate's own convention — so init resolves it against the mirror's origin.
   */
  private Path remoteWithSubmodule(String name, Path sub, String path) throws Exception {
    Path source = tmp.resolve("src/" + name);
    Files.createDirectories(source);
    Git.run(source, "init", "-q", "-b", "main");
    Files.writeString(source.resolve("README"), "super\n");
    Git.run(source, "add", "README");
    Git.run(source, "-c", "protocol.file.allow=always", "submodule", "add", "-q", sub.toString(), path);
    Git.run(source, "config", "-f", ".gitmodules", "submodule." + path + ".url", "../sub.git");
    Git.run(source, "add", ".gitmodules");
    Git.run(source, "commit", "-q", "-m", "with a submodule");
    Path bare = tmp.resolve("remote/" + name + ".git");
    Files.createDirectories(bare.getParent());
    Git.run(tmp, "clone", "-q", "--bare", source.toString(), bare.toString());
    return bare;
  }

  /** The test's own git, with an identity and nothing from the user's config. */
  static final class Git {

    private Git() {}

    static String run(Path dir, String... args) throws Exception {
      List<String> argv = new ArrayList<>(List.of("git", "-C", dir.toString()));
      argv.addAll(List.of(args));
      ProcessBuilder builder = new ProcessBuilder(argv).redirectErrorStream(true);
      Map<String, String> env = builder.environment();
      env.put("GIT_AUTHOR_NAME", "t");
      env.put("GIT_AUTHOR_EMAIL", "t@example.invalid");
      env.put("GIT_COMMITTER_NAME", "t");
      env.put("GIT_COMMITTER_EMAIL", "t@example.invalid");
      Process process = builder.start();
      String output = new String(process.getInputStream().readAllBytes());
      assertTrue(process.waitFor(60, TimeUnit.SECONDS), "git hung");
      assertEquals(0, process.exitValue(), () -> "git " + args[0] + ": " + output);
      return output;
    }

    static String sha(Path repo, String rev) throws Exception {
      return run(repo, "rev-parse", rev).strip();
    }

    /** Commit {@code content} on top of {@code remote}'s main and push it; returns the new sha. */
    static String push(Path tmp, Path remote, String content) throws Exception {
      Path clone = tmp.resolve("pusher-" + System.nanoTime());
      run(tmp, "clone", "-q", remote.toString(), clone.toString());
      Files.writeString(clone.resolve("file.txt"), content);
      run(clone, "commit", "-q", "-am", content.strip());
      run(clone, "push", "-q", "origin", "HEAD:main");
      return sha(clone, "HEAD");
    }
  }
}
