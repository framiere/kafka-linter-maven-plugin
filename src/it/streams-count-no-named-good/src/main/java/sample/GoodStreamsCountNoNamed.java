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

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every count invocation passes a Named (and,
 * where appropriate, a Materialized too), so the descriptor
 * contains org/apache/kafka/streams/kstream/Named and the
 * predicate rejects it.
 */
public final class GoodStreamsCountNoNamed {

    public KTable<String, Long> countA(KGroupedStream<String, String> g) {
        return g.count(Named.as("count-a"));
    }

    public KTable<String, Long> countB(KGroupedStream<String, String> g) {
        return g.count(
                Named.as("count-b"),
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count-b-store"));
    }

    public KTable<Windowed<String>, Long> countC(TimeWindowedKStream<String, String> w) {
        return w.count(Named.as("count-c"));
    }

    public KTable<Windowed<String>, Long> countD(SessionWindowedKStream<String, String> s) {
        return s.count(
                Named.as("count-d"),
                Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("count-d-store"));
    }

    public Function<Named, KTable<String, Long>> buildNamedFactory(KGroupedStream<String, String> g) {
        return g::count;
    }

    public BiFunction<Named, Materialized<String, Long, KeyValueStore<Bytes, byte[]>>,
            KTable<String, Long>> buildNamedMaterializedFactory(KGroupedStream<String, String> g) {
        return g::count;
    }

    public KTable<String, Long> useLocalFactory(KGroupedStream<String, String> g) {
        Function<Named, KTable<String, Long>> factory = g::count;
        return factory.apply(Named.as("count-local"));
    }

    public Stream<KTable<String, Long>> countAll(List<KGroupedStream<String, String>> groups) {
        return groups.stream().map(g -> g.count(Named.as("count-each")));
    }

    public static void main(String[] args) {
        new GoodStreamsCountNoNamed();
    }
}
