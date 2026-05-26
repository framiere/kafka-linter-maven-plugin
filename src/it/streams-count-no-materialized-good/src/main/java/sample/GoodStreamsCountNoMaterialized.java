package sample;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link Materialized} pinning the state-store name and (by
 * derivation) the changelog topic name.
 */
public final class GoodStreamsCountNoMaterialized {

    /** 1-arg SAM matching {@code (Materialized)KTable}. */
    @FunctionalInterface
    interface CountFactory<K> {
        KTable<K, Long> apply(Materialized<K, Long, KeyValueStore<Bytes, byte[]>> m);
    }

    /** 2-arg SAM matching {@code (Named, Materialized)KTable}. */
    @FunctionalInterface
    interface NamedCountFactory<K> {
        KTable<K, Long> apply(Named named, Materialized<K, Long, KeyValueStore<Bytes, byte[]>> m);
    }

    private static final Materialized<String, Long, KeyValueStore<Bytes, byte[]>> M =
            Materialized.as("shared-count-store");

    public KTable<String, Long> countA(KGroupedStream<String, String> g) {
        return g.count(Materialized.as("count-a-store"));
    }

    public KTable<String, Long> countB(KGroupedStream<String, String> g) {
        return g.count(Named.as("count-b"), Materialized.as("count-b-store"));
    }

    public KTable<Windowed<String>, Long> countC(TimeWindowedKStream<String, String> w) {
        Materialized<String, Long, WindowStore<Bytes, byte[]>> mw =
                Materialized.as("count-c-window-store");
        return w.count(mw);
    }

    public KTable<Windowed<String>, Long> countD(SessionWindowedKStream<String, String> s) {
        Materialized<String, Long, SessionStore<Bytes, byte[]>> ms =
                Materialized.as("count-d-session-store");
        return s.count(Named.as("count-d"), ms);
    }

    public Function<Materialized<String, Long, KeyValueStore<Bytes, byte[]>>, KTable<String, Long>>
            buildFunctionFactory(KGroupedStream<String, String> g) {
        return g::count;
    }

    public BiFunction<Named, Materialized<String, Long, KeyValueStore<Bytes, byte[]>>, KTable<String, Long>>
            buildNamedFactory(KGroupedStream<String, String> g) {
        return g::count;
    }

    public KTable<String, Long> useLocalFactory(KGroupedStream<String, String> g) {
        CountFactory<String> factory = g::count;
        return factory.apply(M);
    }

    public Stream<KTable<String, Long>> countAll(List<KGroupedStream<String, String>> groups) {
        return groups.stream().map(g -> g.count(M));
    }

    public static void main(String[] args) {
        new GoodStreamsCountNoMaterialized();
    }
}
