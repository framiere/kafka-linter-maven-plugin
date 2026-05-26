package sample;

import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_AGGREGATE_NO_MATERIALIZED — must fire EXACTLY
 * 8 times across this class (one per method below).
 *
 * <p>Exercises the unsafe overload descriptors of {@code
 * aggregate} on each of the four grouped-stream-like owner
 * interfaces (KGroupedStream, KGroupedTable,
 * TimeWindowedKStream, SessionWindowedKStream). Unlike {@code
 * count}, the Kafka API does NOT expose a standalone Named-only
 * half-fix for {@code aggregate} (every Named overload is
 * paired with Materialized) — so every aggregate overload that
 * lacks Materialized is the canonical no-Materialized form.
 */
public final class BadStreamsAggregateNoMaterialized {

    /** 2-arg SAM matching {@code (Initializer, Aggregator)KTable}. */
    @FunctionalInterface
    interface AggFactory<K, V, VR> {
        KTable<K, VR> apply(Initializer<VR> init, Aggregator<? super K, ? super V, VR> agg);
    }

    private static final Initializer<Long> INIT = () -> 0L;
    private static final Aggregator<String, String, Long> AGG = (k, v, acc) -> acc + 1L;
    private static final Aggregator<String, String, Long> SUB = (k, v, acc) -> acc - 1L;
    private static final Merger<String, Long> MERGER = (k, a, b) -> a + b;

    // ===== Direct INVOKEINTERFACE on the unsafe overloads =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KGroupedStream.aggregate(Initializer, Aggregator) — the
     *  canonical no-Materialized form. */
    public KTable<String, Long> aggregateA(KGroupedStream<String, String> g) {
        return g.aggregate(INIT, AGG);
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KGroupedTable.aggregate(Initializer, Aggregator,
     *  Aggregator) — KGroupedTable owner, adder+subtractor
     *  signature, still unnamed state store. */
    public KTable<String, Long> aggregateB(KGroupedTable<String, String> g) {
        return g.aggregate(INIT, AGG, SUB);
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  TimeWindowedKStream.aggregate(Initializer, Aggregator)
     *  — windowed aggregate, still unnamed state store. */
    public KTable<Windowed<String>, Long> aggregateC(TimeWindowedKStream<String, String> w) {
        return w.aggregate(INIT, AGG);
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  SessionWindowedKStream.aggregate(Initializer, Aggregator,
     *  Merger) — session-windowed aggregate, still unnamed
     *  state store. */
    public KTable<Windowed<String>, Long> aggregateD(SessionWindowedKStream<String, String> s) {
        return s.aggregate(INIT, AGG, MERGER);
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code grouped::aggregate} bound to {@link
     *  BiFunction}{@code <Initializer, Aggregator, KTable>}.
     *  Indy implMethod handle descriptor matches
     *  KGroupedStream.aggregate(Initializer, Aggregator)KTable. */
    public BiFunction<Initializer<Long>, Aggregator<? super String, ? super String, Long>, KTable<String, Long>>
            buildBiFunctionFactory(KGroupedStream<String, String> g) {
        return g::aggregate;
    }

    /** MUST FIRE — {@code grouped::aggregate} bound to a
     *  custom 2-arg SAM whose erased implMethod descriptor
     *  matches the unsafe overload. */
    public AggFactory<String, String, Long> buildCustomFactory(KGroupedStream<String, String> g) {
        return g::aggregate;
    }

    /** MUST FIRE — local SAM binding via method reference,
     *  applied inside the same method. */
    public KTable<String, Long> useLocalFactory(KGroupedStream<String, String> g) {
        AggFactory<String, String, Long> factory = g::aggregate;
        return factory.apply(INIT, AGG);
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code g.aggregate(INIT, AGG)}. The synthetic
     *  {@code lambda$aggregateAll$0} carries a direct
     *  INVOKEINTERFACE on KGroupedStream.aggregate(Initializer,
     *  Aggregator). */
    public Stream<KTable<String, Long>> aggregateAll(List<KGroupedStream<String, String>> groups) {
        return groups.stream().map(g -> g.aggregate(INIT, AGG));
    }

    public static void main(String[] args) {
        new BadStreamsAggregateNoMaterialized();
    }
}
