package eu.wohlben.qits.cidaemon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/** The bootstrap's baked credentials, against real files in a temp directory. */
@EnabledOnOs(OS.LINUX)
class CredentialRefreshTest {

  @TempDir Path tmp;

  @Test
  void filesTheBootstrapNeverWroteAreSkippedRatherThanCreated() {
    CredentialRefresh refresh =
        new CredentialRefresh(
            Map.of(), tmp.resolve("docker"), tmp.resolve("settings.xml"), tmp.resolve(".npmrc"));

    // No registry credential in the Continue: config.json is left alone too.
    assertNull(refresh.refresh("old-t0ken", "new-t0ken", Map.of()));

    assertFalse(Files.exists(tmp.resolve("docker")));
    assertFalse(Files.exists(tmp.resolve("settings.xml")));
    assertFalse(Files.exists(tmp.resolve(".npmrc")));
  }

  @Test
  void aBlankOldTokenReplacesNothing() throws Exception {
    Path npmrc = tmp.resolve(".npmrc");
    Files.writeString(npmrc, "//r/:_authToken=old-t0ken\n");
    CredentialRefresh refresh = new CredentialRefresh(Map.of("X", ""), null, null, npmrc);

    assertNull(refresh.refresh("", "new-t0ken", Map.of()));
    assertNull(refresh.refresh(null, "new-t0ken", Map.of()));

    assertEquals("//r/:_authToken=old-t0ken\n", Files.readString(npmrc));
    assertEquals(Map.of(), refresh.inheritedOverlay("", "new-t0ken", Map.of()));
  }

  @Test
  void onlyLeaseVariablesHoldingTheOldTokenThatTheContinueDoesNotSetAreCarriedOver() {
    CredentialRefresh refresh =
        new CredentialRefresh(
            Map.of(
                "QITS_PUBLISH_TOKEN", "old-t0ken",
                "QITS_TOKEN", "old-t0ken",
                "CI", "true"),
            null,
            null,
            null);

    assertEquals(
        Map.of("QITS_PUBLISH_TOKEN", "new-t0ken"),
        refresh.inheritedOverlay("old-t0ken", "new-t0ken", Map.of("QITS_TOKEN", "new-t0ken")));
  }
}
