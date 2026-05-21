package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

/**
 * RULE: STREAMS_FOREACH_PEEK_PRINTS_STDOUT.
 *
 * The rule fires on a {@code KStream.foreach(...)} or {@code KStream.peek(...)}
 * call site when the supplied lambda's body writes to {@code System.out} or
 * {@code System.err}.
 *
 * In bytecode each anti-pattern site below produces:
 *   INVOKEDYNAMIC apply()Lorg/apache/kafka/streams/kstream/ForeachAction;
 *     [bsmArgs include the impl Handle pointing at a synthetic lambda$ method]
 *   INVOKEINTERFACE org/apache/kafka/streams/kstream/KStream.foreach|peek(...)
 *
 * The synthetic lambda body contains a {@code GETSTATIC java/lang/System.out}
 * (or .err) and an INVOKEVIRTUAL on PrintStream. The rule walks the impl
 * Handle, finds the synthetic method, and inspects its instructions for the
 * System.out/err GETSTATIC fingerprint.
 *
 * The control method at the bottom uses {@code peek()} with a plain side-
 * effect (incrementing a field) — no System.out, must NOT fire.
 */
public final class BadForeachPeekPrintsStdout {

    private long peekCount;

    private KStream<String, String> source(StreamsBuilder b) {
        return b.stream("orders", Consumed.with(Serdes.String(), Serdes.String()));
    }

    /** Anti-pattern: foreach with inline lambda writing to System.out. */
    public void debugWithForeachStdout(StreamsBuilder b) {
        source(b).foreach((k, v) -> System.out.println("k=" + k + " v=" + v)); // FIRES
    }

    /** Anti-pattern: peek with inline lambda writing to System.err. */
    public KStream<String, String> debugWithPeekStderr(StreamsBuilder b) {
        return source(b).peek((k, v) -> System.err.println("inspect " + v)); // FIRES
    }

    /** Anti-pattern: foreach with method reference to System.out::println. */
    public void debugWithForeachMethodRef(StreamsBuilder b) {
        // Single-arg overload of println, so the lambda's SAM signature must
        // ignore one of the two ForeachAction params. The compiler synthesises
        // an adapter that calls System.out.println(value) — still flagged.
        source(b).foreach((k, v) -> System.out.println(v)); // FIRES
    }

    /** Anti-pattern: peek with a lambda calling System.out.printf. */
    public KStream<String, String> debugWithPeekPrintf(StreamsBuilder b) {
        return source(b).peek((k, v) -> System.out.printf("key=%s value=%s%n", k, v)); // FIRES
    }

    /** Control: peek with a benign side-effect (counter). Must NOT fire. */
    public KStream<String, String> inspectWithCounter(StreamsBuilder b) {
        return source(b).peek((k, v) -> peekCount++); // OK
    }
}
