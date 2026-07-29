package eu.wohlben.qits.cidaemon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.cidaemon.protocol.InitFailed;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/**
 * Drives a real {@code git} against a real origin repository built by the test, because the thing
 * under test <em>is</em> the integration with git — a canned runner would only assert that the
 * mapping matches itself. The two cases a real git cannot produce on demand (an image with no git,
 * an image with no bash) use a scripted {@link CommandRunner} instead, which is also the only way to
 * test them on a machine that has both.
 *
 * <p>The mapping these tests pin is not cosmetic. {@code SHA_GONE} is what makes qits-ci re-read its
 * config source and discard a run whose commit was force-pushed away; if a checkout failure ever
 * started reporting {@code CLONE_FAILED} instead, that run would be recorded as a failure against a
 * commit nobody can look at, and nothing else in the system would notice.
 */
@EnabledOnOs(OS.LINUX)
class WorkspaceTest {

  @TempDir Path tmp;

  @Test
  void aCloneAndCheckoutOfThePushedShaLeavesThatCommitInTheWorkingTree() throws Exception {
    Path origin = originWithCommits("first\n", "second\n", "third\n");
    String wanted = shaOf(origin, "HEAD~1");

    Path checkout = tmp.resolve("workspace");
    Workspace.Preparation preparation = workspace(checkout, origin, "main", wanted).prepare();

    assertTrue(preparation.ready(), () -> "expected a ready checkout, got " + preparation);
    assertEquals(wanted, shaOf(checkout, "HEAD"));
    assertEquals("second\n", Files.readString(checkout.resolve("file.txt")));
  }

  @Test
  void aRecentButNotTipShaIsStillPresentBecauseTheCloneIsFiftyDeep() throws Exception {
    // Depth 1 would report SHA_GONE for every one of these, and the host would discard runs it
    // should have executed — pushes queueing up behind a serialized runner is the normal case.
    String[] contents = new String[20];
    Arrays.setAll(contents, i -> "commit-" + i + "\n");
    Path origin = originWithCommits(contents);
    String wanted = shaOf(origin, "HEAD~15");

    Path checkout = tmp.resolve("workspace");
    assertTrue(workspace(checkout, origin, "main", wanted).prepare().ready());
    assertEquals(wanted, shaOf(checkout, "HEAD"));
  }

  @Test
  void aShaTheCloneDoesNotCarryIsReportedAsShaGoneAndNotAsAFailedClone() throws Exception {
    Path origin = originWithCommits("first\n");
    // A well-formed commit id that is simply not in this repository — what a force-push between the
    // host's ancestor check and this clone leaves behind.
    String vanished = "0123456789abcdef0123456789abcdef01234567";

    Workspace.Preparation preparation =
        workspace(tmp.resolve("workspace"), origin, "main", vanished).prepare();

    assertFalse(preparation.ready());
    assertEquals(InitFailed.Reason.SHA_GONE, preparation.failure());
    assertNotNull(preparation.detail());
  }

  @Test
  void aRepositoryThatCannotBeClonedIsReportedAsCloneFailed() {
    Workspace.Preparation preparation =
        new Workspace(
                tmp.resolve("workspace"),
                "file://" + tmp.resolve("no-such-repository"),
                "main",
                "0123456789abcdef0123456789abcdef01234567",
                CommandRunner.forking(60))
            .prepare();

    assertFalse(preparation.ready());
    assertEquals(InitFailed.Reason.CLONE_FAILED, preparation.failure());
  }

  @Test
  void aBranchThatDoesNotExistIsACloneFailureRatherThanAMissingCommit() throws Exception {
    Path origin = originWithCommits("first\n");

    Workspace.Preparation preparation =
        workspace(tmp.resolve("workspace"), origin, "no-such-branch", shaOf(origin, "HEAD"))
            .prepare();

    assertFalse(preparation.ready());
    assertEquals(InitFailed.Reason.CLONE_FAILED, preparation.failure());
  }

  @Test
  void anImageWithoutGitIsReportedAsToolingMissingRatherThanAsAFailedClone() {
    CommandRunner noGit =
        (dir, argv) ->
            argv[0].equals("git")
                ? new CommandRunner.Result(-1, "Cannot run program \"git\"")
                : new CommandRunner.Result(0, "");

    Workspace.Preparation preparation =
        new Workspace(tmp.resolve("workspace"), "file:///origin", "main", "abcdef1", noGit)
            .prepare();

    assertEquals(InitFailed.Reason.TOOLING_MISSING, preparation.failure());
    assertTrue(preparation.detail().contains("git"), preparation.detail());
  }

  @Test
  void anImageWithoutBashIsReportedAsToolingMissingBeforeAnythingIsCloned() {
    List<String> ran = new ArrayList<>();
    CommandRunner noBash =
        (dir, argv) -> {
          ran.add(String.join(" ", argv));
          return argv[0].equals("bash")
              ? new CommandRunner.Result(-1, "Cannot run program \"bash\"")
              : new CommandRunner.Result(0, "");
        };

    Workspace.Preparation preparation =
        new Workspace(tmp.resolve("workspace"), "file:///origin", "main", "abcdef1", noBash)
            .prepare();

    assertEquals(InitFailed.Reason.TOOLING_MISSING, preparation.failure());
    // Probed up front: discovering it when the step starts would report a broken image as a failed
    // step, which is a different thing entirely to whoever reads the run.
    assertFalse(
        ran.stream().anyMatch(command -> command.startsWith("git clone")),
        () -> "nothing should have been cloned, but ran: " + ran);
  }

  @Test
  void aShaThatCannotNameACommitIsRejectedBeforeItReachesAnArgv() {
    List<String> ran = new ArrayList<>();
    CommandRunner recording =
        (dir, argv) -> {
          ran.add(String.join(" ", argv));
          return new CommandRunner.Result(0, "");
        };

    Workspace.Preparation preparation =
        new Workspace(
                tmp.resolve("workspace"), "file:///origin", "main", "--upload-pack=evil", recording)
            .prepare();

    assertEquals(InitFailed.Reason.SHA_GONE, preparation.failure());
    assertFalse(
        ran.stream().anyMatch(command -> command.contains("--upload-pack")),
        () -> "the value must not reach git at all, but ran: " + ran);
  }

  @Test
  void theCloneArgvAsksForFiftyCommitsOfTheNamedBranchAndSeparatesItsPositionals() {
    List<String[]> ran = new ArrayList<>();
    CommandRunner recording =
        (dir, argv) -> {
          ran.add(argv);
          return new CommandRunner.Result(0, "");
        };

    new Workspace(
            Path.of("/workspace"),
            "http://qits-artifacts:8080/git/repo-1",
            "main",
            "0123456789abcdef0123456789abcdef01234567",
            recording)
        .prepare();

    List<String> clone =
        ran.stream()
            .map(Arrays::asList)
            .filter(argv -> argv.size() > 1 && argv.get(1).equals("clone"))
            .findFirst()
            .orElseThrow();
    assertEquals(
        List.of(
            "git",
            "clone",
            "--depth",
            "50",
            "--branch",
            "main",
            "--",
            "http://qits-artifacts:8080/git/repo-1",
            "/workspace"),
        clone);
  }

  @Test
  void aVerboseGitFailureIsBoundedToOneChunksBudgetAndKeepsItsTail() {
    String noise = "x".repeat(Workspace.DETAIL_MAX_CHARS * 3) + "fatal: the last thing git said";
    CommandRunner loud =
        (dir, argv) ->
            argv[1].equals("clone")
                ? new CommandRunner.Result(128, noise)
                : new CommandRunner.Result(0, "");

    Workspace.Preparation preparation =
        new Workspace(
                Path.of("/workspace"),
                "file:///origin",
                "main",
                "0123456789abcdef0123456789abcdef01234567",
                loud)
            .prepare();

    assertEquals(Workspace.DETAIL_MAX_CHARS, preparation.detail().length());
    // The tail, not the head: the last thing git said before it gave up is the diagnosis.
    assertTrue(preparation.detail().endsWith("fatal: the last thing git said"));
  }

  // --- helpers ------------------------------------------------------------------------------------

  private Workspace workspace(Path checkout, Path origin, String branch, String sha) {
    return new Workspace(
        checkout, "file://" + origin.toAbsolutePath(), branch, sha, CommandRunner.forking(120));
  }

  /** A real repository on {@code main} with one commit per given file content. */
  private Path originWithCommits(String... contents) throws Exception {
    Path origin = Files.createDirectories(tmp.resolve("origin"));
    git(origin, "init", "--initial-branch=main");
    for (String content : contents) {
      Files.writeString(origin.resolve("file.txt"), content, StandardCharsets.UTF_8);
      git(origin, "add", "file.txt");
      git(origin, "commit", "-m", "commit");
    }
    return origin;
  }

  private String shaOf(Path repository, String rev) throws Exception {
    return git(repository, "rev-parse", rev).trim();
  }

  /** Runs git with an identity of its own, so the suite does not depend on the machine's. */
  private String git(Path dir, String... args) throws Exception {
    List<String> argv = new ArrayList<>(List.of("git", "-c", "user.email=ci@qits.invalid", "-c", "user.name=qits-ci"));
    argv.addAll(List.of(args));
    Process process =
        new ProcessBuilder(argv).directory(new File(dir.toString())).redirectErrorStream(true).start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertEquals(0, process.waitFor(), () -> "git " + String.join(" ", args) + " failed: " + output);
    return output;
  }
}
