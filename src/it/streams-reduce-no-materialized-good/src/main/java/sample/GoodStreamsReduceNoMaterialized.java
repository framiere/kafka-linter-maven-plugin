package sample;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link Materialized} pinning the state-store name and (by
 * derivation) the changelog topic name.
 */
public final class GoodStreamsReduceNoMaterialized {

    /** 2-arg SAM matching {@code (Reducer, Materialized)KTable}. */
    @FunctionalInterface
    interface ReduceFactory<K, V> {
        KTable<K, V> apply(Reducer<V> r, Materialized<K, V, KeyValueStore<Bytes, byte[]>> m);
    }

    private static final Reducer<Long> ADD = (a, b) -> a + b;
    private static final Reducer<Long> SUB = (a, b) -> a - b;
    private static final Materialized<String, Long, KeyValueStore<Bytes, byte[]>> M =
            Materialized.as("shared-reduce-store");

    public KTable<String, Long> reduceA(KGroupedStream<String, Long> g) {
        return g.reduce(ADD, Materialized.as("reduce-a-store"));
    }

    public KTable<String, Long> reduceB(KGroupedTable<String, Long> g) {
        return g.reduce(ADD, SUB,
                Named.as("reduce-b"), Materialized.as("reduce-b-store"));
    }

    public KTable<Windowed<String>, Long> reduceC(TimeWindowedKStream<String, Long> w) {
        Materialized<String, Long, WindowStore<Bytes, byte[]>> mw =
                Materialized.as("reduce-c-window-store");
        return w.reduce(ADD, mw);
    }

    public KTable<Windowed<String>, Long> reduceD(SessionWindowedKStream<String, Long> s) {
        Materialized<String, Long, SessionStore<Bytes, byte[]>> ms =
                Materialized.as("reduce-d-session-store");
        return s.reduce(ADD, Named.as("reduce-d"), ms);
    }

    public BiFunction<Reducer<Long>, Materialized<String, Long, KeyValueStore<Bytes, byte[]>>, KTable<String, Long>>
            buildFunctionFactory(KGroupedStream<String, Long> g) {
        return g::reduce;
    }

    public ReduceFactory<String, Long> buildCustomFactory(KGroupedStream<String, Long> g) {
        return g::reduce;
    }

    public KTable<String, Long> useLocalFactory(KGroupedStream<String, Long> g) {
        ReduceFactory<String, Long> factory = g::reduce;
        return factory.apply(ADD, M);
    }

    public Stream<KTable<String, Long>> reduceAll(List<KGroupedStream<String, Long>> groups) {
        return groups.stream().map(g -> g.reduce(ADD, M));
    }

    public static void main(String[] args) {
        new GoodStreamsReduceNoMaterialized();
    }
}
