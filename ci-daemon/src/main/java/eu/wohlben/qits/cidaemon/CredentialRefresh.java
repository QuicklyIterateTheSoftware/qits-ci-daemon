package eu.wohlben.qits.cidaemon;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.jboss.logging.Logger;

/**
 * Re-points the credentials the ci-slot bootstrap baked into the container at the next run's token
 * when a {@code Continue} arrives (qits-732).
 *
 * <p>The bootstrap ({@code CiSlotSettings.BOOTSTRAP} in qits-ci-service) runs once, before this
 * daemon is exec'd, and writes the first run's token into files and exported variables. The host
 * revokes that token once the lease answers {@code Continued}, so a phase-2 step reading any of them
 * is refused — measured live as an image publish failing with {@code the identity provider refused
 * this token}. What the bootstrap leaves, and what happens to each here:
 *
 * <ul>
 *   <li>{@code $DOCKER_CONFIG/config.json}, printed from {@code $QITS_CI_REGISTRY_AUTH_CONFIG}: its
 *       auth is base64 of {@code user:token}, so a textual replace cannot find the token. It is
 *       rewritten whole from the new run's {@code QITS_CI_REGISTRY_AUTH_CONFIG}.
 *   <li>{@code /tmp/qits-deploy-settings.xml} (maven's {@code -gs}) and {@code $HOME/.npmrc}: the
 *       token in clear text, so every occurrence of the old one becomes the new one.
 *   <li>{@code QITS_PUBLISH_TOKEN}, and any other exported variable holding the old token: the
 *       daemon cannot rewrite its own environment, so {@link #inheritedOverlay} names each one for the
 *       step env layer instead.
 *   <li>The git credential helper and {@code /tmp/qits-publish-token} read {@code $QITS_TOKEN} when
 *       they run, which the step env layer already covers.
 * </ul>
 *
 * <p>Every rewrite is a temp file in the same directory moved over the original, so a step never
 * reads half a file, and keeps the owner-only permissions the bootstrap's {@code umask 077} gave.
 * Nothing here logs a token, only paths. A plain class with the paths as constructor arguments,
 * like everything {@link Main} builds, so the suite can point it at a temp directory.
 */
public final class CredentialRefresh {

  private static final Logger LOG = Logger.getLogger(CredentialRefresh.class);

  /** Where the bootstrap writes maven's global settings, with the bearer in three headers. */
  static final Path DEPLOY_SETTINGS = Path.of("/tmp/qits-deploy-settings.xml");

  static final String REGISTRY_AUTH_CONFIG = "QITS_CI_REGISTRY_AUTH_CONFIG";

  private static final Set<PosixFilePermission> OWNER_ONLY =
      PosixFilePermissions.fromString("rw-------");

  private final Map<String, String> leaseEnv;
  private final Path dockerConfigDir;
  private final Path deploySettings;
  private final Path npmrc;

  /**
   * @param leaseEnv the environment every step child inherits — the daemon's own process
   *     environment, which is where the bootstrap's exports live
   * @param dockerConfigDir {@code $DOCKER_CONFIG}; null when the lease has none
   * @param deploySettings maven's global settings file; null for none
   * @param npmrc {@code $HOME/.npmrc}; null when the lease has no home
   */
  public CredentialRefresh(
      Map<String, String> leaseEnv, Path dockerConfigDir, Path deploySettings, Path npmrc) {
    this.leaseEnv = leaseEnv == null ? Map.of() : Map.copyOf(leaseEnv);
    this.dockerConfigDir = dockerConfigDir;
    this.deploySettings = deploySettings;
    this.npmrc = npmrc;
  }

  /** The production paths, read from the process environment the bootstrap exported. */
  public static CredentialRefresh fromEnvironment(Map<String, String> processEnv) {
    String docker = processEnv.get("DOCKER_CONFIG");
    String home = processEnv.get("HOME");
    return new CredentialRefresh(
        processEnv,
        blank(docker) ? null : Path.of(docker),
        DEPLOY_SETTINGS,
        blank(home) ? null : Path.of(home).resolve(".npmrc"));
  }

  /** Touches nothing and adds nothing: a daemon built without a bootstrap behind it. */
  public static CredentialRefresh none() {
    return new CredentialRefresh(Map.of(), null, null, null);
  }

  /**
   * Every lease variable still holding the old token that the Continue does not set itself, mapped
   * to the new token — {@code QITS_PUBLISH_TOKEN} above all. Empty when the old token is blank.
   */
  public Map<String, String> inheritedOverlay(
      String oldToken, String newToken, Map<String, String> continueEnv) {
    Map<String, String> overlay = new LinkedHashMap<>();
    if (blank(oldToken) || blank(newToken)) {
      return overlay;
    }
    leaseEnv.forEach(
        (key, value) -> {
          if (oldToken.equals(value) && !continueEnv.containsKey(key)) {
            overlay.put(key, newToken);
          }
        });
    return overlay;
  }

  /**
   * Rewrite the baked credential files for the new run, or say which could not be written. Never
   * throws; null means done.
   */
  public String refresh(String oldToken, String newToken, Map<String, String> continueEnv) {
    Path current = null;
    try {
      String auth = continueEnv.get(REGISTRY_AUTH_CONFIG);
      if (dockerConfigDir != null && !blank(auth)) {
        current = dockerConfigDir.resolve("config.json");
        Files.createDirectories(dockerConfigDir);
        writeAtomically(current, auth, OWNER_ONLY);
        LOG.infof("ci-daemon rewrote %s for the next run", current);
      }
      if (!blank(oldToken) && !blank(newToken) && !oldToken.equals(newToken)) {
        for (Path file : new Path[] {deploySettings, npmrc}) {
          current = file;
          if (file != null && replaceToken(file, oldToken, newToken)) {
            LOG.infof("ci-daemon re-pointed %s at the next run's token", file);
          }
        }
      }
      return null;
    } catch (IOException | RuntimeException e) {
      // The message names the path and the failure, never the content.
      return "could not rewrite " + current + ": " + e.getClass().getSimpleName() + ": "
          + e.getMessage();
    }
  }

  /** True when the file held the old token and now holds the new one; a missing file is false. */
  private static boolean replaceToken(Path file, String oldToken, String newToken)
      throws IOException {
    if (!Files.isRegularFile(file)) {
      return false;
    }
    String text = Files.readString(file, StandardCharsets.UTF_8);
    if (!text.contains(oldToken)) {
      return false;
    }
    Set<PosixFilePermission> perms;
    try {
      perms = Files.getPosixFilePermissions(file);
    } catch (UnsupportedOperationException e) {
      perms = null;
    }
    writeAtomically(file, text.replace(oldToken, newToken), perms);
    return true;
  }

  private static void writeAtomically(Path target, String content, Set<PosixFilePermission> perms)
      throws IOException {
    Path dir = target.toAbsolutePath().getParent();
    Path temp;
    try {
      FileAttribute<?> attr = PosixFilePermissions.asFileAttribute(OWNER_ONLY);
      temp = Files.createTempFile(dir, "." + target.getFileName() + ".", ".tmp", attr);
    } catch (UnsupportedOperationException e) {
      temp = Files.createTempFile(dir, "." + target.getFileName() + ".", ".tmp");
    }
    try {
      Files.writeString(temp, content, StandardCharsets.UTF_8);
      if (perms != null) {
        try {
          Files.setPosixFilePermissions(temp, perms);
        } catch (UnsupportedOperationException ignored) {
          // not a POSIX file system: whatever the temp file got
        }
      }
      try {
        Files.move(
            temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException | FileAlreadyExistsException e) {
        Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      Files.deleteIfExists(temp);
    }
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
