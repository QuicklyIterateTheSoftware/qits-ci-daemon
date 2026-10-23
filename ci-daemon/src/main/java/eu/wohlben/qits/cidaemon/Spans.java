package eu.wohlben.qits.cidaemon;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * The step's trace: one root span, {@value #ROOT}, from the daemon's start to its telemetry flush,
 * and a flat set of children under it — {@code dial}, {@code checkout}, {@code script}.
 *
 * <p><b>The trace id is derived, not minted.</b> It is the first 16 bytes of SHA-256 over the run
 * id, so every step of one run — each its own container, its own process, with nothing shared
 * between them — lands in the same trace without a protocol change and without the host handing a
 * trace context down. A daemon started without a run id gets a random one.
 *
 * <p>Not a tracing library: there is one flow and it is sequential, so "the current span" is simply
 * the most recently started child still open, or the root. A log line takes its ids from there —
 * which needs no thread-local context, and so survives the flow hopping between the event loop and
 * the worker pool. All state is guarded by this object's monitor; nothing here logs or blocks while
 * holding it.
 */
final class Spans {

  /** The root span's name. */
  static final String ROOT = "ci-daemon lease";

  /** What an open child is ended with when the daemon flushes before it ended on its own. */
  static final String UNFINISHED = "the daemon ended before this did";

  /** One span; mutated only through {@link Spans}' monitor. */
  final class Span {
    private final String name;
    private final byte[] id;
    private final byte[] parent;
    private final long start;
    private final Map<String, Object> attributes = new LinkedHashMap<>();
    private long end;
    private int status = Otlp.STATUS_UNSET;
    private String message;

    private Span(String name, byte[] parent, long start) {
      this.name = name;
      this.id = randomId(8);
      this.parent = parent;
      this.start = start;
    }

    /** Set an attribute. Ignored once the span has ended. */
    Span attribute(String key, Object value) {
      synchronized (Spans.this) {
        if (end == 0) {
          attributes.put(key, value);
        }
      }
      return this;
    }

    /** End it, OK. A second end is ignored: the first one is what happened. */
    void end() {
      end(null);
    }

    /** End it; a non-null {@code error} makes it ERROR with that message, null makes it OK. */
    void end(String error) {
      synchronized (Spans.this) {
        if (end != 0) {
          return;
        }
        end = Math.max(start, clock.getAsLong());
        status = error == null ? Otlp.STATUS_OK : Otlp.STATUS_ERROR;
        message = error;
      }
    }

    byte[] id() {
      return id.clone();
    }

    private Otlp.SpanData data() {
      return new Otlp.SpanData(
          traceId.clone(),
          id.clone(),
          parent == null ? null : parent.clone(),
          name,
          Otlp.KIND_INTERNAL,
          start,
          end,
          Map.copyOf(attributes),
          status,
          message);
    }
  }

  private final SecureRandom random = new SecureRandom();
  private final LongSupplier clock;
  private final byte[] traceId;
  private final Span root;
  private final List<Span> children = new ArrayList<>();

  /**
   * @param runId the run every step of which shares one trace; blank for a random trace id
   * @param clock epoch nanoseconds
   */
  Spans(String runId, LongSupplier clock) {
    this.clock = clock;
    this.traceId = runId == null || runId.isBlank() ? randomId(16) : traceIdFor(runId);
    this.root = new Span(ROOT, null, clock.getAsLong());
  }

  /** The first 16 bytes of SHA-256 over the run id's UTF-8. */
  static byte[] traceIdFor(String runId) {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(runId.getBytes(StandardCharsets.UTF_8));
      return Arrays.copyOf(digest, 16);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("every JDK and native image carries SHA-256", e);
    }
  }

  byte[] traceId() {
    return traceId.clone();
  }

  /** Start a child of the root. */
  synchronized Span start(String name) {
    Span span = new Span(name, root.id, clock.getAsLong());
    children.add(span);
    return span;
  }

  /** The child that is still open and started last, or the root. */
  synchronized Span current() {
    for (int i = children.size() - 1; i >= 0; i--) {
      if (children.get(i).end == 0) {
        return children.get(i);
      }
    }
    return root;
  }

  /**
   * End the root — a non-null {@code rootError} makes it ERROR — and any child still open, and
   * return every span. Children still open are ERROR with {@link #UNFINISHED}: an exit 6 leaves a
   * script running, and a span that pretended it ended cleanly would say the opposite of what
   * happened.
   */
  synchronized List<Otlp.SpanData> finish(String rootError) {
    for (Span child : children) {
      child.end(UNFINISHED);
    }
    root.end(rootError);
    List<Otlp.SpanData> all = new ArrayList<>();
    all.add(root.data());
    children.forEach(child -> all.add(child.data()));
    return all;
  }

  /** Never all zeroes: OTLP reads an all-zero id as no id at all. */
  private byte[] randomId(int length) {
    byte[] id = new byte[length];
    do {
      random.nextBytes(id);
    } while (Arrays.equals(id, new byte[length]));
    return id;
  }
}
