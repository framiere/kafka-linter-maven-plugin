package sample;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link Materialized} pinning the state-store name and (by
 * derivation) the changelog topic name.
 */
public final class GoodStreamsAggregateNoMaterialized {

    /** 3-arg SAM matching {@code (Initializer, Aggregator,
     *  Materialized)KTable} — same Materialized typing as the
     *  KGroupedStream overload. */
    @FunctionalInterface
    interface AggFactory<K, V, VR> {
        KTable<K, VR> apply(
                Initializer<VR> init,
                Aggregator<? super K, ? super V, VR> agg,
                Materialized<K, VR, KeyValueStore<Bytes, byte[]>> m);
    }

    private static final Initializer<Long> INIT = () -> 0L;
    private static final Aggregator<String, String, Long> AGG = (k, v, acc) -> acc + 1L;
    private static final Aggregator<String, String, Long> SUB = (k, v, acc) -> acc - 1L;
    private static final Merger<String, Long> MERGER = (k, a, b) -> a + b;
    private static final Materialized<String, Long, KeyValueStore<Bytes, byte[]>> M =
            Materialized.as("shared-aggregate-store");

    public KTable<String, Long> aggregateA(KGroupedStream<String, String> g) {
        return g.aggregate(INIT, AGG, Materialized.as("aggregate-a-store"));
    }

    public KTable<String, Long> aggregateB(KGroupedTable<String, String> g) {
        return g.aggregate(INIT, AGG, SUB,
                Named.as("aggregate-b"), Materialized.as("aggregate-b-store"));
    }

    public KTable<Windowed<String>, Long> aggregateC(TimeWindowedKStream<String, String> w) {
        Materialized<String, Long, WindowStore<Bytes, byte[]>> mw =
                Materialized.as("aggregate-c-window-store");
        return w.aggregate(INIT, AGG, mw);
    }

    public KTable<Windowed<String>, Long> aggregateD(SessionWindowedKStream<String, String> s) {
        Materialized<String, Long, SessionStore<Bytes, byte[]>> ms =
                Materialized.as("aggregate-d-session-store");
        return s.aggregate(INIT, AGG, MERGER, Named.as("aggregate-d"), ms);
    }

    public Function<Materialized<String, Long, KeyValueStore<Bytes, byte[]>>, KTable<String, Long>>
            buildMaterializedFactory(KGroupedStream<String, String> g) {
        return m -> g.aggregate(INIT, AGG, m);
    }

    public AggFactory<String, String, Long> buildCustomFactory(KGroupedStream<String, String> g) {
        return g::aggregate;
    }

    public KTable<String, Long> useLocalFactory(KGroupedStream<String, String> g) {
        AggFactory<String, String, Long> factory = g::aggregate;
        return factory.apply(INIT, AGG, M);
    }

    public Stream<KTable<String, Long>> aggregateAll(List<KGroupedStream<String, String>> groups) {
        return groups.stream().map(g -> g.aggregate(INIT, AGG, M));
    }

    public static void main(String[] args) {
        new GoodStreamsAggregateNoMaterialized();
    }
}
