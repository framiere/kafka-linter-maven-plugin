package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

/**
 * RULE: STREAMS_KSTREAM_PRINT.
 *
 * Each {@code stream.print(...)} call below inserts a
 * {@code PrintForeachAction} processor into the topology. Every record
 * passing through is serialised through one synchronous {@code Writer}
 * (default: {@code System.out}). On {@code System.out} that's a
 * line-synchronized mutex shared by every StreamThread, every record,
 * forever; on a file writer it's still a per-record flush sitting in the
 * middle of the processing graph.
 *
 * The first three sites are anti-patterns the rule must flag. The fourth
 * method shows the correct shape — {@code peek()} for inspection without
 * inserting blocking I/O — and must NOT fire.
 */
public final class BadKStreamPrint {

    /** Anti-pattern: print to stdout — the classic 'forgot to remove' debug call. */
    public static void debugToStdout(StreamsBuilder builder) {
        KStream<String, String> stream = builder.stream("orders");
        stream.print(Printed.toSysOut()); // FIRES
    }

    /** Anti-pattern: print to stdout with a label — still a synchronous System.out hop. */
    public static void debugToStdoutLabeled(StreamsBuilder builder) {
        KStream<String, String> stream = builder.stream("payments");
        stream.print(Printed.<String, String>toSysOut().withLabel("payments")); // FIRES
    }

    /** Anti-pattern: print to a file — synchronous per-record write, same shape. */
    public static void debugToFile(StreamsBuilder builder) {
        KStream<String, String> stream = builder.stream("audit");
        stream.print(Printed.toFile("/tmp/audit.log")); // FIRES
    }

    /** Control: peek() for inspection — does not insert blocking I/O, must NOT fire. */
    public static void inspectWithPeek(StreamsBuilder builder) {
        KStream<String, String> stream = builder.stream("metrics");
        stream.peek((k, v) -> {
            // a metrics counter or structured log would go here
        }).to("metrics-out");
    }
}
