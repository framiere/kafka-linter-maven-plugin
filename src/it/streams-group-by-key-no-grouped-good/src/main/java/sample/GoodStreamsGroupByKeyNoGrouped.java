package sample;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site uses the
 * {@code groupByKey(Grouped)} overload, so the resulting
 * KGroupedStream and any synthesised repartition topic carry a
 * stable, user-chosen name across topology edits.
 */
public final class GoodStreamsGroupByKeyNoGrouped {

    /** 1-arg groupByKey SAM matching {@code (Grouped)KGroupedStream}. */
    @FunctionalInterface
    interface GroupByKeyWithGroupedFactory<K, V> {
        KGroupedStream<K, V> apply(Grouped<K, V> grouped);
    }

    public KGroupedStream<String, String> groupOrders(KStream<String, String> orders) {
        return orders.groupByKey(Grouped.as("orders-by-key"));
    }

    public KGroupedStream<String, String> groupAfterSelectKey(KStream<String, String> events) {
        return events.selectKey((k, v) -> v).groupByKey(Grouped.as("events-rekeyed"));
    }

    public KGroupedStream<String, String> groupAfterFilter(KStream<String, String> events) {
        return events.filter((k, v) -> v != null).groupByKey(Grouped.as("events-filtered"));
    }

    public Function<Grouped<String, String>, KGroupedStream<String, String>>
            buildFunctionFactory(KStream<String, String> stream) {
        return stream::groupByKey;
    }

    public GroupByKeyWithGroupedFactory<String, String>
            buildNullaryFactory(KStream<String, String> stream) {
        return stream::groupByKey;
    }

    public Function<Grouped<String, String>, KGroupedStream<String, String>>
            buildAnotherFunction(KStream<String, String> stream) {
        Function<Grouped<String, String>, KGroupedStream<String, String>> factory = stream::groupByKey;
        return factory;
    }

    public KGroupedStream<String, String> useLocalFactory(KStream<String, String> stream) {
        Function<Grouped<String, String>, KGroupedStream<String, String>> factory = stream::groupByKey;
        return factory.apply(Grouped.as("local-grouped"));
    }

    public Stream<KGroupedStream<String, String>> groupAll(
            List<KStream<String, String>> streams) {
        return streams.stream().map(s -> s.groupByKey(Grouped.as("group-all-store")));
    }

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        System.out.println(builder);
        new GoodStreamsGroupByKeyNoGrouped();
    }
}
