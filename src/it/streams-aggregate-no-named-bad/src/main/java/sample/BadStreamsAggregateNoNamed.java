package sample;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_AGGREGATE_NO_NAMED — must fire EXACTLY 8 times
 * across this class (one per method below).
 *
 * <p>Exercises the unsafe overload descriptors of {@code
 * aggregate} on each of the grouped-stream-like owner
 * interfaces — including KGroupedStream (Init+Agg /
 * Init+Agg+Materialized), KGroupedTable adder-subtractor
 * (Init+Agg+Agg+Materialized), and TimeWindowedKStream
 * (Init+Agg+Materialized). All five owner.aggregate overloads
 * that DO carry Named are safe and must NOT fire.
 */
public final class BadStreamsAggregateNoNamed {

    private static final Initializer<Long> INIT = () -> 0L;
    private static final Aggregator<String, Long, Long> AGG = (k, v, agg) -> agg + v;
    private static final Aggregator<String, Long, Long> SUB = (k, v, agg) -> agg - v;

    /** 2-arg SAM matching {@code (Init, Agg)KTable}. */
    @FunctionalInterface
    interface AggFactory<K, V, VA> {
        KTable<K, VA> apply(Initializer<VA> init, Aggregator<? super K, ? super V, VA> agg);
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overloads =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KGroupedStream.aggregate(Init, Agg) (no Named, no
     *  Materialized). */
    public KTable<String, Long> aggregateA(KGroupedStream<String, Long> g) {
        return g.aggregate(INIT, AGG);
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KGroupedStream.aggregate(Init, Agg, Materialized) — the
     *  SYMMETRIC half-fix variant (still no Named). */
    public KTable<String, Long> aggregateB(KGroupedStream<String, Long> g) {
        return g.aggregate(
                INIT,
                AGG,
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregate-b-store"));
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  TimeWindowedKStream.aggregate(Init, Agg, Materialized) —
     *  windowed aggregate Materialized-only half-fix. */
    public KTable<Windowed<String>, Long> aggregateC(TimeWindowedKStream<String, Long> w) {
        return w.aggregate(
                INIT,
                AGG,
                Materialized.as("aggregate-c-store"));
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KGroupedTable.aggregate(Init, Agg, Agg, Materialized) —
     *  adder-subtractor over an upstream KTable with the
     *  Materialized half-fix; processor-node name is still
     *  graph-index-derived. */
    public KTable<String, Long> aggregateD(KGroupedTable<String, Long> gt) {
        return gt.aggregate(
                INIT,
                AGG,
                SUB,
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregate-d-store"));
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code grouped::aggregate} bound to {@link
     *  BiFunction}{@code <Init, Agg, KTable>}. Indy implMethod
     *  handle descriptor matches KGroupedStream.aggregate(Init,
     *  Agg)KTable — no-Named overload. */
    public BiFunction<Initializer<Long>, Aggregator<? super String, ? super Long, Long>,
            KTable<String, Long>> buildSupplierFactory(KGroupedStream<String, Long> g) {
        return g::aggregate;
    }

    /** MUST FIRE — {@code grouped::aggregate} bound to a
     *  3-arg SAM whose erased descriptor matches
     *  KGroupedStream.aggregate(Init, Agg, Materialized)KTable
     *  — still no Named. */
    @FunctionalInterface
    interface MatAggFactory<K, V, VA> {
        KTable<K, VA> apply(
                Initializer<VA> init,
                Aggregator<? super K, ? super V, VA> agg,
                Materialized<K, VA, KeyValueStore<Bytes, byte[]>> mat);
    }
    public MatAggFactory<String, Long, Long> buildMaterializedFactory(
            KGroupedStream<String, Long> g) {
        return g::aggregate;
    }

    /** MUST FIRE — local SAM binding via method reference,
     *  applied inside the same method. */
    public KTable<String, Long> useLocalFactory(KGroupedStream<String, Long> g) {
        AggFactory<String, Long, Long> factory = g::aggregate;
        return factory.apply(INIT, AGG);
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code g.aggregate(INIT, AGG)}. The synthetic
     *  {@code lambda$aggregateAll$0} carries a direct
     *  INVOKEINTERFACE on KGroupedStream.aggregate(Init, Agg). */
    public Stream<KTable<String, Long>> aggregateAll(List<KGroupedStream<String, Long>> groups) {
        return groups.stream().map(g -> g.aggregate(INIT, AGG));
    }

    public static void main(String[] args) {
        new BadStreamsAggregateNoNamed();
    }
}
