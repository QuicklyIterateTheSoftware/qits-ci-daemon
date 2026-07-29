package eu.wohlben.qits.cidaemon.protocol;

/**
 * Which of the step process's two output streams a {@link StepChunk} carries. They are pumped
 * separately and stay distinguishable all the way to the persisted tail — a step whose stderr is
 * its real output should not be indistinguishable from one that failed silently.
 *
 * <p>Crosses the wire as {@code name()} ({@code "OUT"} / {@code "ERR"}), like the workspace
 * protocol's own stream enum.
 */
public enum Stream {
  OUT,
  ERR
}
