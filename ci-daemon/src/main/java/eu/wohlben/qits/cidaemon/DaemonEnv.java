package eu.wohlben.qits.cidaemon;

/**
 * Everything the daemon is handed before the socket exists. Told, never derived: the url is dialled
 * verbatim and nothing is parsed out of it, and no value here is announced back to the host, which
 * already knows all of them.
 *
 * <p>It is a record rather than eight {@code @ConfigProperty} fields on the flow class because the
 * flow is a plain class with a plain constructor — {@link Main} is the one place that resolves
 * configuration, and this is the shape it hands over.
 *
 * <p><b>{@code token} is required.</b> Every run is handed {@code $QITS_TOKEN}, the run's own {@code
 * ci-run} token, and the socket presents it as a bearer — it is the only credential this daemon has.
 * {@code tokenSubject} names the token for a log line and is never sent anywhere: a value the host
 * minted is not a value to announce back to it.
 */
public record DaemonEnv(
    String daemonUrl,
    String daemonId,
    String repositoryUrl,
    String branch,
    String sha,
    String token,
    String tokenSubject) {

  /**
   * The first missing value, or {@code null} when the contract is satisfied. Checked before the
   * dial: a container launched without what it needs cannot register, and failing at startup
   * with the name of the absent variable is the only diagnosis anyone gets — the host's own view of
   * it is "a container that never registered".
   *
   * <p>Names the environment variable, not the config key, because that is what the launcher sets
   * and what a human reading {@code docker logs} can act on. The token is reported as
   * <em>missing</em> and never echoed.
   *
   * <p>A blank {@code QITS_TOKEN} counts as absent, so a launcher that injects an empty one is told
   * so here rather than dialling with {@code Bearer } and nothing after it.
   */
  public String missing() {
    if (blank(daemonUrl)) {
      return "QITS_CI_DAEMON_URL";
    }
    if (blank(daemonId)) {
      return "QITS_CI_DAEMON_ID";
    }
    if (blank(token)) {
      return "QITS_TOKEN";
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

  /**
   * Redacted: a record's generated {@code toString} would print the bearer into any log line that
   * ever formats this value, and the bearer opens everything the run may do.
   */
  @Override
  public String toString() {
    return "DaemonEnv[daemonUrl="
        + daemonUrl
        + ", daemonId="
        + daemonId
        + ", repositoryUrl="
        + repositoryUrl
        + ", branch="
        + branch
        + ", sha="
        + sha
        + ", token="
        + (blank(token) ? "<absent>" : "<set>")
        + ", tokenSubject="
        + tokenSubject
        + "]";
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
