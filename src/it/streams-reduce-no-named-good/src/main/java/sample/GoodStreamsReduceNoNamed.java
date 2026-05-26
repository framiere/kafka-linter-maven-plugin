package sample;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every reduce invocation passes a Named (and,
 * where appropriate, a Materialized too), so the descriptor
 * contains org/apache/kafka/streams/kstream/Named and the
 * predicate rejects it.
 *
 * <p>API note: KGroupedStream and KGroupedTable do not expose a
 * (Reducer, Named) 2-arg overload — only the 3-arg form
 * {@code (Reducer, Named, Materialized)} (and KGroupedTable's
 * {@code (Reducer, Reducer, Named, Materialized)}). Only
 * TimeWindowedKStream offers a (Reducer, Named) 2-arg overload.
 */
public final class GoodStreamsReduceNoNamed {

    private static final Reducer<Long> SUM = Long::sum;

    /** Custom SAM for the 3-arg (Reducer, Named, Materialized)
     *  KGroupedStream overload, so the method-reference capture
     *  binds to a Named-bearing descriptor. */
    @FunctionalInterface
    interface NamedMatFactory<K, V> {
        KTable<K, V> apply(
                Reducer<V> r,
                Named named,
                Materialized<K, V, KeyValueStore<Bytes, byte[]>> mat);
    }

    public KTable<String, Long> reduceA(KGroupedStream<String, Long> g) {
        return g.reduce(
                SUM,
                Named.as("reduce-a"),
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("reduce-a-store"));
    }

    public KTable<String, Long> reduceB(KGroupedStream<String, Long> g) {
        return g.reduce(
                SUM,
                Named.as("reduce-b"),
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("reduce-b-store"));
    }

    public KTable<Windowed<String>, Long> reduceC(TimeWindowedKStream<String, Long> w) {
        return w.reduce(SUM, Named.as("reduce-c"));
    }

    public KTable<String, Long> reduceD(KGroupedTable<String, Long> gt) {
        return gt.reduce(
                SUM,
                (v, agg) -> agg - v,
                Named.as("reduce-d"),
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("reduce-d-store"));
    }

    public NamedMatFactory<String, Long> buildNamedMaterializedFactory(
            KGroupedStream<String, Long> g) {
        return g::reduce;
    }

    public KTable<String, Long> useLocalFactory(KGroupedStream<String, Long> g) {
        NamedMatFactory<String, Long> factory = g::reduce;
        return factory.apply(
                SUM,
                Named.as("reduce-local"),
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("reduce-local-store"));
    }

    public Stream<KTable<Windowed<String>, Long>> reduceAll(
            List<TimeWindowedKStream<String, Long>> windowed) {
        return windowed.stream().map(w -> w.reduce(SUM, Named.as("reduce-each")));
    }

    public static void main(String[] args) {
        new GoodStreamsReduceNoNamed();
    }
}
