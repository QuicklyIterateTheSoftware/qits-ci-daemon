package eu.wohlben.qits.cidaemon.protocol;

/**
 * One slice of a running step's output, tagged with its {@link Stream} and correlated back to the
 * {@link RunStep} that started it. Emitted zero-or-more times before the terminal {@link
 * StepFinished}.
 *
 * <p>{@code correlationId} is here from the first version even though a container runs exactly one
 * step and could not be ambiguous: output may later move to a second outbound-dialled socket
 * (finish-ci-feature.md §4 decision 7) and this shape must not change when it does.
 *
 * <p>{@code seq} is a per-correlation monotonic counter starting at 0. The wire preserves order on
 * one socket, so it is not needed to reassemble — it is there so the host can *assert* order and
 * see a gap, which is the difference between "the step printed nothing" and "we lost frames".
 */
public record StepChunk(String correlationId, long seq, Stream stream, String text)
    implements CiDaemonMessage {}
