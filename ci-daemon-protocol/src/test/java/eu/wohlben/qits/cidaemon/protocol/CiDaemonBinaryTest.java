package eu.wohlben.qits.cidaemon.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * The pin resolves, and it resolves to a CalVer.
 *
 * <p>This is the guard on the one thing about {@link CiDaemonBinary} that can silently break: the
 * resource-filtering block in this module's pom. Without filtering the class reads the literal
 * {@code ${project.version}}, and the consumer that pins the daemon by this constant would compose
 * a download URL nothing serves — a failure that surfaces a repository away, inside a throwaway
 * step container that never gets as far as running its step.
 *
 * <p>It deliberately asserts the <em>shape</em> and not a value. The version is whatever release
 * stamped this tree, so an expected literal would be a line every release has to edit, and the
 * first missed edit would make this test fail for a correct build.
 */
class CiDaemonBinaryTest {

  @Test
  void theVersionIsFilteredInAndIsACalVer() {
    String version = CiDaemonBinary.VERSION;
    assertFalse(version.isBlank(), "the version is blank");
    assertFalse(
        version.startsWith("$"),
        () -> "an unfiltered ${project.version} reached the jar: " + version);
    assertTrue(
        version.matches("[0-9][0-9.]*[0-9]"),
        () -> "not a CalVer — is resource filtering still on? got: " + version);
  }

  @Test
  void theDaemonNameIsTheOneThePipelinePublishesUnder() {
    // A literal string, because it is a cross-repository contract: `qits-ci-daemon` is what
    // `.config/qits/ci-event-release.yml` declares in `artifacts:`, the path segment it PUTs the
    // bytes to under the `daemons` store, and the segment a step container's bootstrap downloads
    // from. A rename that moved only one side is what this assertion exists to catch.
    assertEquals("qits-ci-daemon", CiDaemonBinary.DAEMON_NAME);
  }
}
