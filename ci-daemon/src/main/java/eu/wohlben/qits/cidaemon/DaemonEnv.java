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
 * <p><b>{@code token} is optional, and its absence is the INTERNAL plane exactly as it always
 * was.</b> A run taken by a runner on the EDGE plane is handed {@code $QITS_TOKEN}, the run's own
 * {@code ci-run} token, and the socket then presents it as a bearer instead of asserting a user and
 * roles — the edge strips asserted identity on every inbound request, so on that plane the old
 * handshake cannot pass at all. {@code tokenSubject} names the token for a log line and is never
 * sent anywhere: a value the host minted is not a value to announce back to it.
 */
public record DaemonEnv(
    String daemonUrl,
    String daemonId,
    String daemonSecret,
    String repositoryUrl,
    String branch,
    String sha,
    String token,
    String tokenSubject) {

  /** The INTERNAL plane's environment: no token, so the handshake asserts its identity. */
  public DaemonEnv(
      String daemonUrl,
      String daemonId,
      String daemonSecret,
      String repositoryUrl,
      String branch,
      String sha) {
    this(daemonUrl, daemonId, daemonSecret, repositoryUrl, branch, sha, "", "");
  }

  /**
   * Whether the run handed this container a token. Blank counts as absent, so a launcher that
   * injects an empty {@code QITS_TOKEN} gets today's handshake rather than {@code Bearer } with
   * nothing after it.
   */
  public boolean hasToken() {
    return !blank(token);
  }

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

  /**
   * Redacted: a record's generated {@code toString} would print the secret and the bearer into any
   * log line that ever formats this value, and the bearer opens everything the run may do.
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
        + (hasToken() ? "<set>" : "<absent>")
        + ", tokenSubject="
        + tokenSubject
        + "]";
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
