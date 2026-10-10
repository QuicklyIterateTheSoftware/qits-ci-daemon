package eu.wohlben.qits.cidaemon;

import eu.wohlben.qits.cidaemon.protocol.Stream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * The step's output, as lines, for telemetry — the latest {@code maxLines} of them and a count of
 * the rest.
 *
 * <p><b>Chunks are not lines.</b> {@link StepProcess} flushes on a newline, on a size cap and on a
 * timer, so one chunk may hold a hundred lines or half of one. This splits on {@code \n} and carries
 * each stream's unfinished tail into the next chunk of that stream; a tail still open when the step
 * ends is a line of its own. A line longer than {@link Telemetry#MAX_BODY} keeps its head and drops
 * the rest — memory stays bounded however long a line a script prints without a newline.
 *
 * <p><b>The latest lines, because the failure is at the end.</b> Every step on the estate shares one
 * {@code qits-ci-daemon} bucket in qits-observability, a ring of a few thousand records; one chatty
 * build exporting all of its output would evict every other step's telemetry. So the cap is
 * load-bearing, and it is a tail rather than a head. The full output still reaches qits-ci as
 * chunks; this is only what qits-observability gets.
 */
final class StepOutput {

  /** One completed line, with the span it was printed under. */
  record Line(long epochNanos, Stream stream, String text, byte[] traceId, byte[] spanId) {}

  private final int maxLines;
  private final ArrayDeque<Line> tail = new ArrayDeque<>();
  private final Map<Stream, StringBuilder> partial = new EnumMap<>(Stream.class);
  private final Map<Stream, Boolean> truncated = new EnumMap<>(Stream.class);
  private long total;
  private byte[] lastTraceId;
  private byte[] lastSpanId;

  StepOutput(int maxLines) {
    this.maxLines = Math.max(0, maxLines);
  }

  /** One chunk of one stream. Lines it completes are timestamped {@code now}. */
  synchronized void accept(Stream stream, String text, long now, byte[] traceId, byte[] spanId) {
    if (text == null || text.isEmpty()) {
      return;
    }
    lastTraceId = traceId;
    lastSpanId = spanId;
    StringBuilder line = partial.computeIfAbsent(stream, s -> new StringBuilder());
    int from = 0;
    while (from < text.length()) {
      int newline = text.indexOf('\n', from);
      int to = newline < 0 ? text.length() : newline;
      append(stream, line, text, from, to);
      if (newline < 0) {
        return;
      }
      complete(stream, line, now, traceId, spanId);
      from = newline + 1;
    }
  }

  /**
   * Close every stream's open tail as a line and hand over what is kept, oldest first. Empties the
   * buffer; {@link #total()} keeps counting what was ever seen. A tail closed here carries the span
   * of the chunk that opened it.
   */
  synchronized List<Line> drain(long now) {
    for (Stream stream : Stream.values()) {
      StringBuilder line = partial.get(stream);
      if (line != null && (line.length() > 0 || Boolean.TRUE.equals(truncated.get(stream)))) {
        complete(stream, line, now, lastTraceId, lastSpanId);
      }
    }
    List<Line> lines = new ArrayList<>(tail);
    tail.clear();
    return lines;
  }

  /** How many lines the step printed, exported or not. */
  synchronized long total() {
    return total;
  }

  /** How many of {@link #total()} the cap kept out. */
  synchronized long notExported() {
    return Math.max(0, total - Math.min(total, maxLines));
  }

  private void append(Stream stream, StringBuilder line, String text, int from, int to) {
    int room = Telemetry.MAX_BODY - line.length();
    if (to - from > room) {
      truncated.put(stream, true);
      to = from + Math.max(0, room);
    }
    line.append(text, from, to);
  }

  private void complete(
      Stream stream, StringBuilder line, long now, byte[] traceId, byte[] spanId) {
    int length = line.length();
    if (length > 0 && line.charAt(length - 1) == '\r') {
      line.setLength(length - 1);
    }
    String text = Boolean.TRUE.equals(truncated.remove(stream)) ? line + "…" : line.toString();
    line.setLength(0);
    total++;
    if (maxLines == 0) {
      return;
    }
    if (tail.size() == maxLines) {
      tail.pollFirst();
    }
    tail.addLast(new Line(now, stream, text, traceId, spanId));
  }
}
