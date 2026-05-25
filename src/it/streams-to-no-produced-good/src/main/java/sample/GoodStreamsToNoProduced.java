package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.TopicNameExtractor;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link Produced}, so the sink node carries a stable user-chosen
 * name AND the key/value serdes are pinned independently of the
 * global default.key.serde / default.value.serde config.
 */
public final class GoodStreamsToNoProduced {

    /** 2-arg SAM matching {@code (String, Produced)V}. */
    @FunctionalInterface
    interface ToTopicWithProducedFactory<K, V> {
        void apply(String topic, Produced<K, V> produced);
    }

    private static Produced<String, String> produced(String name) {
        return Produced.with(Serdes.String(), Serdes.String()).withName(name);
    }

    private static TopicNameExtractor<String, String> extractor() {
        return (key, value, recordContext) -> "extracted-topic";
    }

    public void toOrders(KStream<String, String> stream) {
        stream.to("orders-out", produced("orders-out-sink"));
    }

    public void toExtractor(KStream<String, String> stream) {
        stream.to(extractor(), produced("extractor-sink"));
    }

    public void toAfterFilter(KStream<String, String> stream) {
        stream.filter((k, v) -> v != null)
                .to("filtered-out", produced("filtered-out-sink"));
    }

    public BiConsumer<String, Produced<String, String>>
            buildConsumerFactory(KStream<String, String> stream) {
        return stream::to;
    }

    public BiConsumer<TopicNameExtractor<String, String>, Produced<String, String>>
            buildExtractorConsumerFactory(KStream<String, String> stream) {
        return stream::to;
    }

    public ToTopicWithProducedFactory<String, String>
            buildCustomFactory(KStream<String, String> stream) {
        return stream::to;
    }

    public void useLocalFactory(KStream<String, String> stream) {
        ToTopicWithProducedFactory<String, String> factory = stream::to;
        factory.apply("local-out", produced("local-out-sink"));
    }

    public void toAll(KStream<String, String> stream, List<String> topics) {
        topics.forEach(t -> stream.to(t, produced(t + "-sink")));
    }

    public static void main(String[] args) {
        new GoodStreamsToNoProduced();
    }
}
