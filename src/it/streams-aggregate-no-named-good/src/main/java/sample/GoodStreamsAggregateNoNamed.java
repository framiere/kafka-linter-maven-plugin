package sample;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every aggregate invocation passes a Named
 * (and, where appropriate, a Materialized too), so the
 * descriptor contains org/apache/kafka/streams/kstream/Named
 * and the predicate rejects it.
 *
 * <p>API note: KGroupedStream does not expose a (Init, Agg,
 * Named) 3-arg overload — only the 4-arg (Init, Agg, Named,
 * Materialized). KGroupedTable does not expose a (Init, Agg,
 * Agg, Named) — wait, it actually DOES; we exercise both the
 * 4-arg and 5-arg variants. TimeWindowedKStream has both
 * (Init, Agg, Named) and (Init, Agg, Named, Materialized).
 */
public final class GoodStreamsAggregateNoNamed {

    private static final Initializer<Long> INIT = () -> 0L;
    private static final Aggregator<String, Long, Long> AGG = (k, v, agg) -> agg + v;
    private static final Aggregator<String, Long, Long> SUB = (k, v, agg) -> agg - v;

    /** 4-arg SAM matching KGroupedStream.aggregate(Init, Agg,
     *  Named, Materialized) so the method-reference capture
     *  binds to a Named-bearing descriptor. */
    @FunctionalInterface
    interface NamedMatAggFactory<K, V, VA> {
        KTable<K, VA> apply(
                Initializer<VA> init,
                Aggregator<? super K, ? super V, VA> agg,
                Named named,
                Materialized<K, VA, KeyValueStore<Bytes, byte[]>> mat);
    }

    public KTable<String, Long> aggregateA(KGroupedStream<String, Long> g) {
        return g.aggregate(
                INIT, AGG,
                Named.as("aggregate-a"),
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregate-a-store"));
    }

    public KTable<String, Long> aggregateB(KGroupedStream<String, Long> g) {
        return g.aggregate(
                INIT, AGG,
                Named.as("aggregate-b"),
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregate-b-store"));
    }

    public KTable<Windowed<String>, Long> aggregateC(TimeWindowedKStream<String, Long> w) {
        return w.aggregate(INIT, AGG, Named.as("aggregate-c"));
    }

    public KTable<String, Long> aggregateD(KGroupedTable<String, Long> gt) {
        return gt.aggregate(
                INIT, AGG, SUB,
                Named.as("aggregate-d"),
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregate-d-store"));
    }

    public NamedMatAggFactory<String, Long, Long> buildNamedMaterializedFactory(
            KGroupedStream<String, Long> g) {
        return g::aggregate;
    }

    public KTable<String, Long> useLocalFactory(KGroupedStream<String, Long> g) {
        NamedMatAggFactory<String, Long, Long> factory = g::aggregate;
        return factory.apply(
                INIT, AGG,
                Named.as("aggregate-local"),
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregate-local-store"));
    }

    public Stream<KTable<Windowed<String>, Long>> aggregateAll(
            List<TimeWindowedKStream<String, Long>> windowed) {
        return windowed.stream().map(w -> w.aggregate(INIT, AGG, Named.as("aggregate-each")));
    }

    public static void main(String[] args) {
        new GoodStreamsAggregateNoNamed();
    }
}
