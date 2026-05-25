package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link Consumed}, so the source node carries a stable user-
 * chosen name AND the key/value serdes are pinned independently
 * of the global default.key.serde / default.value.serde config.
 */
public final class GoodStreamsStreamNoConsumed {

    /** 2-arg SAM matching {@code (String, Consumed)KStream}. */
    @FunctionalInterface
    interface StreamFromTopicWithConsumedFactory<K, V> {
        KStream<K, V> apply(String topic, Consumed<K, V> consumed);
    }

    private static Consumed<String, String> consumed(String name) {
        return Consumed.with(Serdes.String(), Serdes.String()).withName(name);
    }

    public KStream<String, String> streamOrders(StreamsBuilder builder) {
        return builder.stream("orders", consumed("orders-source"));
    }

    public KStream<String, String> streamMulti(
            StreamsBuilder builder, Collection<String> topics) {
        return builder.stream(topics, consumed("multi-source"));
    }

    public KStream<String, String> streamPattern(
            StreamsBuilder builder, Pattern pattern) {
        return builder.stream(pattern, consumed("pattern-source"));
    }

    public BiFunction<String, Consumed<String, String>, KStream<String, String>>
            buildFromTopicFactory(StreamsBuilder builder) {
        return builder::stream;
    }

    public BiFunction<Pattern, Consumed<String, String>, KStream<String, String>>
            buildFromPatternFactory(StreamsBuilder builder) {
        return builder::stream;
    }

    public StreamFromTopicWithConsumedFactory<String, String>
            buildCustomFactory(StreamsBuilder builder) {
        return builder::stream;
    }

    public KStream<String, String> useLocalFactory(StreamsBuilder builder) {
        StreamFromTopicWithConsumedFactory<String, String> factory = builder::stream;
        return factory.apply("local-topic", consumed("local-source"));
    }

    public Stream<KStream<String, String>> streamAll(
            StreamsBuilder builder, List<String> topics) {
        return topics.stream().map(t -> builder.stream(t, consumed(t + "-source")));
    }

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        System.out.println(builder);
        new GoodStreamsStreamNoConsumed();
    }
}
