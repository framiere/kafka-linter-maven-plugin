package sample;

import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_REDUCE_NO_MATERIALIZED — must fire EXACTLY 8
 * times across this class (one per method below).
 *
 * <p>Exercises the unsafe overload descriptors of {@code
 * reduce} on each of the four grouped-stream-like owner
 * interfaces (KGroupedStream, KGroupedTable,
 * TimeWindowedKStream, SessionWindowedKStream). Unlike {@code
 * count}, the Kafka API does NOT expose a standalone Named-only
 * half-fix for {@code reduce} (every Named overload is paired
 * with Materialized) — so every reduce overload that lacks
 * Materialized is the canonical no-Materialized form.
 */
public final class BadStreamsReduceNoMaterialized {

    /** 1-arg SAM matching {@code (Reducer)KTable}. */
    @FunctionalInterface
    interface ReduceFactory<K, V> {
        KTable<K, V> apply(Reducer<V> r);
    }

    private static final Reducer<Long> ADD = (a, b) -> a + b;
    private static final Reducer<Long> SUB = (a, b) -> a - b;

    // ===== Direct INVOKEINTERFACE on the unsafe overloads =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KGroupedStream.reduce(Reducer) — the canonical
     *  no-Materialized form. */
    public KTable<String, Long> reduceA(KGroupedStream<String, Long> g) {
        return g.reduce(ADD);
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KGroupedTable.reduce(Reducer, Reducer) — adder +
     *  subtractor signature, still unnamed state store. */
    public KTable<String, Long> reduceB(KGroupedTable<String, Long> g) {
        return g.reduce(ADD, SUB);
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  TimeWindowedKStream.reduce(Reducer) — windowed reduce,
     *  still unnamed state store. */
    public KTable<Windowed<String>, Long> reduceC(TimeWindowedKStream<String, Long> w) {
        return w.reduce(ADD);
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  SessionWindowedKStream.reduce(Reducer) — session-
     *  windowed reduce, still unnamed state store. */
    public KTable<Windowed<String>, Long> reduceD(SessionWindowedKStream<String, Long> s) {
        return s.reduce(ADD);
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code grouped::reduce} bound to {@link
     *  Function}{@code <Reducer, KTable>}. Indy implMethod
     *  handle descriptor matches KGroupedStream.reduce(Reducer)
     *  KTable. */
    public Function<Reducer<Long>, KTable<String, Long>> buildFunctionFactory(
            KGroupedStream<String, Long> g) {
        return g::reduce;
    }

    /** MUST FIRE — {@code table::reduce} bound to {@link
     *  BiFunction}{@code <Reducer, Reducer, KTable>}. Indy
     *  implMethod handle descriptor matches KGroupedTable.reduce
     *  (Reducer, Reducer)KTable. */
    public BiFunction<Reducer<Long>, Reducer<Long>, KTable<String, Long>> buildBiFunctionFactory(
            KGroupedTable<String, Long> g) {
        return g::reduce;
    }

    /** MUST FIRE — local SAM binding via method reference,
     *  applied inside the same method. */
    public KTable<String, Long> useLocalFactory(KGroupedStream<String, Long> g) {
        ReduceFactory<String, Long> factory = g::reduce;
        return factory.apply(ADD);
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code g.reduce(ADD)}. The synthetic
     *  {@code lambda$reduceAll$0} carries a direct
     *  INVOKEINTERFACE on KGroupedStream.reduce(Reducer). */
    public Stream<KTable<String, Long>> reduceAll(List<KGroupedStream<String, Long>> groups) {
        return groups.stream().map(g -> g.reduce(ADD));
    }

    public static void main(String[] args) {
        new BadStreamsReduceNoMaterialized();
    }
}
