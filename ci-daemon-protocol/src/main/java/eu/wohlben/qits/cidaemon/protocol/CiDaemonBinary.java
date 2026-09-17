package eu.wohlben.qits.cidaemon.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * <b>What this jar was released with</b>: the {@code qits-ci-daemon} binary version of the release
 * that published this artifact.
 *
 * <p>It exists so a consumer's <em>pom</em> decides which daemon binary its step containers
 * download, instead of a configuration entry rewritten underneath it on every daemon release.
 * qits-ci used to read {@code QITS_CI_DAEMON_VERSION} — moved by a deployment act, with an adoption
 * probe on top that launched whatever had just been published and kept it if the container dialled
 * — so a new daemon reached a real step without the pair ever having been built, let alone tested,
 * together, and the only gate on a protocol break was a probe the released service ran on itself in
 * production. Now the version travels as the version of the artifact that also carries the wire
 * contract both sides speak: bumping one is bumping the other, the maintenance train moves it like
 * any internal library, and the consumer's own release request — which runs this binary at this
 * version against its own host before the merge — is where a daemon that broke the wire fails.
 *
 * <p><b>The value is this module's {@code ${project.version}}, resolved at build time</b> into
 * {@code ci-daemon-binary.properties} beside this class rather than written down as a literal. The
 * release flow stamps the pom with the version it is about to tag, and the same release publishes
 * the binary under that version — so "the version of this jar" and "the version of the binary" are
 * one string by construction and cannot drift apart by an edit somebody forgot. A build from a
 * working tree therefore names the <em>previous</em> release, which is honest: nothing has been
 * published for the tree in hand.
 *
 * <p><b>The contract this names is a URL.</b> {@code .config/qits/ci-event-release.yml} PUTs the
 * binary to {@code …/artifacts/daemons/qits-ci-daemon/<version>} and a step container's bootstrap
 * downloads it from exactly that path, so {@link #DAEMON_NAME} and {@link #VERSION} are the two
 * path segments a consumer composes. That is why a blank or unfiltered value is refused loudly
 * here rather than defaulted: a fallback would compose a path that 404s inside a container nobody
 * is watching, one repository away from the mistake.
 *
 * <p>Framework-free like the rest of this module — a {@code Properties} load off the classpath, no
 * config system, no injection — because the native binary and the consuming service have no
 * framework in common, and because a capability module here is not allowed to read configuration
 * at all.
 */
public final class CiDaemonBinary {

  /** The resource the build filters {@code ${project.version}} into, beside this class. */
  private static final String RESOURCE = "ci-daemon-binary.properties";

  /**
   * The daemon's artifact name in qits-artifacts' {@code daemons} store — the coordinate the
   * release pipeline PUTs the bytes under, the {@code packageName} on the {@code SoftwareRelease}
   * it announces, and the path segment a step container's bootstrap downloads from. Named here
   * rather than at the call site because it is the same string the pipeline writes, and a rename
   * that moved only one side would be a 404 in a throwaway container.
   */
  public static final String DAEMON_NAME = "qits-ci-daemon";

  /** The released version: the {@code qits-ci-daemon} binary version of the release that published this jar. */
  public static final String VERSION = readVersion();

  private static String readVersion() {
    Properties p = new Properties();
    try (InputStream in = CiDaemonBinary.class.getResourceAsStream(RESOURCE)) {
      if (in == null) {
        // Not a recoverable state and not worth a fallback: a jar of this module with the resource
        // missing is a broken build, and a consumer that silently downloaded "" or "latest" is the
        // exact failure this class exists to remove.
        throw new IllegalStateException(
            RESOURCE + " is not on the classpath beside " + CiDaemonBinary.class.getName());
      }
      p.load(in);
    } catch (IOException e) {
      throw new IllegalStateException("cannot read " + RESOURCE, e);
    }
    String version = p.getProperty("version", "");
    if (version.isBlank() || version.startsWith("$")) {
      // `$` catches the one mistake that would otherwise ship: resource filtering switched off,
      // leaving the literal `${project.version}` to be used as a download path segment.
      throw new IllegalStateException(RESOURCE + " carries no resolved version: '" + version + "'");
    }
    return version;
  }

  private CiDaemonBinary() {}
}
