package eu.wohlben.qits.cidaemon;

/**
 * Everything the daemon is handed before the socket exists. Told, never derived: the url is dialled
 * verbatim and nothing is parsed out of it, and no value here is announced back to the host, which
 * already knows all of them.
 *
 * <p>It is a record rather than six {@code @ConfigProperty} fields on the flow class because the
 * flow is a plain class with a plain constructor — {@link Main} is the one place that resolves
 * configuration, and this is the shape it hands over.
 */
public record DaemonEnv(
    String daemonUrl,
    String daemonId,
    String daemonSecret,
    String repositoryUrl,
    String branch,
    String sha) {

  /**
   * The first missing value, or {@code null} when the contract is satisfied. Checked before the
   * dial: a container launched without its secret cannot register, and failing at startup with the
   * name of the absent variable is the only diagnosis anyone gets — the host's own view of it is "a
   * container that never registered".
   *
   * <p>Names the environment variable, not the config key, because that is what the launcher sets
   * and what a human reading {@code docker logs} can act on. The secret is reported as
   * <em>missing</em> and never echoed.
   */
  public String missing() {
    if (blank(daemonUrl)) {
      return "QITS_CI_DAEMON_URL";
    }
    if (blank(daemonId)) {
      return "QITS_CI_DAEMON_ID";
    }
    if (blank(daemonSecret)) {
      return "QITS_CI_DAEMON_SECRET";
    }
    if (blank(repositoryUrl)) {
      return "QITS_CI_REPOSITORY_URL";
    }
    if (blank(branch)) {
      return "QITS_CI_BRANCH";
    }
    if (blank(sha)) {
      return "QITS_CI_SHA";
    }
    return null;
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
