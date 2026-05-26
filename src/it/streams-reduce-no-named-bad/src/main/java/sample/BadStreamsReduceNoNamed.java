package sample;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_REDUCE_NO_NAMED — must fire EXACTLY 8 times
 * across this class (one per method below).
 *
 * <p>Exercises the unsafe overload descriptors of {@code reduce}
 * on each of the grouped-stream-like owner interfaces — including
 * KGroupedStream (Reducer / Reducer+Materialized), KGroupedTable
 * adder-subtractor (Reducer+Reducer+Materialized), and
 * TimeWindowedKStream (Reducer). (The {@code reduce(Reducer,
 * Named, ...)} overloads are safe and must NOT fire.)
 */
public final class BadStreamsReduceNoNamed {

    private static final Reducer<Long> SUM = Long::sum;

    /** 0-arg SAM matching {@code ()KTable}. */
    @FunctionalInterface
    interface ReduceFactory<K, V> {
        KTable<K, V> apply(Reducer<V> r);
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overloads =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KGroupedStream.reduce(Reducer) (no Named, no
     *  Materialized). */
    public KTable<String, Long> reduceA(KGroupedStream<String, Long> g) {
        return g.reduce(SUM);
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KGroupedStream.reduce(Reducer, Materialized) — the
     *  SYMMETRIC half-fix variant (still no Named). */
    public KTable<String, Long> reduceB(KGroupedStream<String, Long> g) {
        return g.reduce(
                SUM,
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("reduce-b-store"));
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  TimeWindowedKStream.reduce(Reducer) — windowed reduce,
     *  still unnamed processor node. */
    public KTable<Windowed<String>, Long> reduceC(TimeWindowedKStream<String, Long> w) {
        return w.reduce(SUM);
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KGroupedTable.reduce(Reducer, Reducer, Materialized) —
     *  adder-subtractor over an upstream KTable with the
     *  Materialized half-fix; processor-node name is still
     *  graph-index-derived. */
    public KTable<String, Long> reduceD(KGroupedTable<String, Long> gt) {
        return gt.reduce(
                SUM,
                (v, agg) -> agg - v,
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("reduce-d-store"));
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code grouped::reduce} bound to {@link
     *  Function}{@code <Reducer, KTable>}. Indy implMethod
     *  handle descriptor matches KGroupedStream.reduce(Reducer)
     *  KTable — no-Named overload. */
    public Function<Reducer<Long>, KTable<String, Long>> buildSupplierFactory(
            KGroupedStream<String, Long> g) {
        return g::reduce;
    }

    /** MUST FIRE — {@code grouped::reduce} bound to {@link
     *  BiFunction}{@code <Reducer, Materialized, KTable>}. Indy
     *  implMethod handle descriptor matches KGroupedStream.reduce(
     *  Reducer, Materialized)KTable — still no Named. */
    public BiFunction<Reducer<Long>, Materialized<String, Long, KeyValueStore<Bytes, byte[]>>,
            KTable<String, Long>> buildMaterializedFactory(KGroupedStream<String, Long> g) {
        return g::reduce;
    }

    /** MUST FIRE — local SAM binding via method reference,
     *  applied inside the same method. */
    public KTable<String, Long> useLocalFactory(KGroupedStream<String, Long> g) {
        ReduceFactory<String, Long> factory = g::reduce;
        return factory.apply(SUM);
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code g.reduce(SUM)}. The synthetic
     *  {@code lambda$reduceAll$0} carries a direct
     *  INVOKEINTERFACE on KGroupedStream.reduce(Reducer). */
    public Stream<KTable<String, Long>> reduceAll(List<KGroupedStream<String, Long>> groups) {
        return groups.stream().map(g -> g.reduce(SUM));
    }

    public static void main(String[] args) {
        new BadStreamsReduceNoNamed();
    }
}
