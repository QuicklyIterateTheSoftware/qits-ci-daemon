package eu.wohlben.qits.cidaemon;

import eu.wohlben.qits.cidaemon.protocol.StepFinished;

/**
 * One step's execution, as {@link DaemonMain} needs to see it: run it to a terminal frame, or end it
 * early. {@link StepProcess} is the implementation; the seam exists so the flow can be tested
 * against a scripted step without a real {@code bash}, and so a test can assert the flow's ordering
 * guarantees without racing a process.
 *
 * <p>{@link #run()} returns the terminal {@link StepFinished} rather than emitting it. That is the
 * ordering guarantee stated as a type: chunks reach the consumer while the step runs, and by the
 * time this method returns there are none left to come — so the caller cannot send the terminal
 * frame ahead of output that was still in flight.
 */
public interface Step {

  /** Run to completion, emitting chunks as they are produced. Never throws. */
  StepFinished run();

  /**
   * End the step early. Called from another thread than {@link #run()} — a {@code Cancel} arrives on
   * the socket's event loop while the step blocks a worker — and answered by {@link #run()}
   * returning normally, because a cancelled step still finishes; it just finishes because the daemon
   * killed it.
   */
  void cancel();
}
