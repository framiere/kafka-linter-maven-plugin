package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link Grouped} pinning the auto-repartition topic name, the
 * downstream state-store name, and the changelog topic name.
 */
public final class GoodStreamsGroupByNoGrouped {

    /** 2-arg SAM matching {@code (KeyValueMapper, Grouped)
     *  KGroupedStream}. */
    @FunctionalInterface
    interface GroupByFactory<K, V, KR> {
        KGroupedStream<KR, V> apply(KeyValueMapper<? super K, ? super V, KR> mapper, Grouped<KR, V> grouped);
    }

    /** A second 2-arg SAM at a distinct site. */
    @FunctionalInterface
    interface AnotherGroupByFactory<K, V, KR> {
        KGroupedStream<KR, V> with(KeyValueMapper<? super K, ? super V, KR> mapper, Grouped<KR, V> grouped);
    }

    private static final Grouped<String, String> G =
            Grouped.with(Serdes.String(), Serdes.String()).withName("shared-grouped");

    public KGroupedStream<String, String> groupByUserId(KStream<String, String> a) {
        return a.groupBy((k, v) -> v + "-userId", Grouped.as("group-by-userId"));
    }

    public KGroupedStream<String, String> groupByOrderId(KStream<String, String> b) {
        return b.groupBy((k, v) -> v + "-orderId", Grouped.as("group-by-orderId"));
    }

    public KGroupedStream<String, String> groupBySessionId(KStream<String, String> c) {
        return c.groupBy((k, v) -> v + "-sessionId", Grouped.as("group-by-sessionId"));
    }

    public BiFunction<KeyValueMapper<? super String, ? super String, String>, Grouped<String, String>, KGroupedStream<String, String>>
            buildBiFunctionFactory(KStream<String, String> stream) {
        return stream::groupBy;
    }

    public GroupByFactory<String, String, String> buildCustomFactory(KStream<String, String> stream) {
        return stream::groupBy;
    }

    public AnotherGroupByFactory<String, String, String> buildAnotherCustomFactory(KStream<String, String> stream) {
        return stream::groupBy;
    }

    public KGroupedStream<String, String> useLocalFactory(KStream<String, String> stream,
            KeyValueMapper<? super String, ? super String, String> mapper) {
        GroupByFactory<String, String, String> factory = stream::groupBy;
        return factory.apply(mapper, G);
    }

    public Stream<KGroupedStream<String, String>> groupByAll(
            List<KStream<String, String>> streams,
            KeyValueMapper<? super String, ? super String, String> mapper) {
        return streams.stream().map(s -> s.groupBy(mapper, G));
    }

    public static void main(String[] args) {
        new GoodStreamsGroupByNoGrouped();
    }
}
