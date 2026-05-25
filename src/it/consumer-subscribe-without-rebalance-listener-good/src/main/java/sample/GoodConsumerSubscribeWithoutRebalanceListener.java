package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

/**
 * RULE: CONSUMER_SUBSCRIBE_WITHOUT_REBALANCE_LISTENER — must NOT fire
 * on any method below.
 *
 * <p>Mirror of {@code BadConsumerSubscribeWithoutRebalanceListener}
 * where every call site uses one of the listener-carrying overloads:
 * {@code subscribe(Collection, ConsumerRebalanceListener)} or
 * {@code subscribe(Pattern, ConsumerRebalanceListener)}. Neither
 * matches the rule's no-listener descriptors.
 */
public final class GoodConsumerSubscribeWithoutRebalanceListener {

    @FunctionalInterface
    interface TopicListSubscriber {
        void subscribe(Collection<String> topics);
    }

    @FunctionalInterface
    interface PatternSubscriber {
        void subscribe(Pattern pattern);
    }

    private static final ConsumerRebalanceListener LISTENER = new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            System.err.println("revoked " + partitions);
        }
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.err.println("assigned " + partitions);
        }
    };

    public void consumerLoopInitBounded(KafkaConsumer<String, String> consumer, List<String> topics) {
        consumer.subscribe(topics, LISTENER);
    }

    public void patternBasedDiscoveryBounded(KafkaConsumer<String, String> consumer, Pattern pattern) {
        consumer.subscribe(pattern, LISTENER);
    }

    public void interfaceSubscribeBounded(Consumer<String, String> consumer, List<String> topics) {
        consumer.subscribe(topics, LISTENER);
    }

    public TopicListSubscriber buildTopicListSubscriberBounded(KafkaConsumer<String, String> consumer) {
        return topics -> consumer.subscribe(topics, LISTENER);
    }

    public TopicListSubscriber buildTopicListSubscriberInterfaceBounded(Consumer<String, String> consumer) {
        return topics -> consumer.subscribe(topics, LISTENER);
    }

    public PatternSubscriber buildPatternSubscriberBounded(KafkaConsumer<String, String> consumer) {
        return pattern -> consumer.subscribe(pattern, LISTENER);
    }

    public java.util.function.Consumer<Collection<String>> buildJavaUtilFunctionConsumerBounded(
            KafkaConsumer<String, String> consumer) {
        return topics -> consumer.subscribe(topics, LISTENER);
    }

    public Future<?> scheduleSubscribeBounded(
            ExecutorService executor,
            KafkaConsumer<String, String> consumer,
            List<String> topics) {
        return executor.submit(() -> consumer.subscribe(topics, LISTENER));
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new GoodConsumerSubscribeWithoutRebalanceListener().consumerLoopInitBounded(c, List.of());
        }
    }
}
