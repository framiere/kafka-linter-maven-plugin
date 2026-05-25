package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * RULE: CONSUMER_LIST_TOPICS_NO_TIMEOUT — must NOT fire on any
 * method below.
 *
 * <p>Mirror of {@code BadConsumerListTopicsNoTimeout} where every
 * call site uses the bounded {@code listTopics(Duration)} overload.
 * Descriptor {@code (Ljava/time/Duration;)Ljava/util/Map;} — never
 * matches the rule's no-Duration descriptor.
 */
public final class GoodConsumerListTopicsNoTimeout {

    private static final Duration READ_TIMEOUT = Duration.ofSeconds(5);

    @FunctionalInterface
    interface TopicCatalogReader {
        Map<String, List<PartitionInfo>> readAll();
    }

    public Map<String, List<PartitionInfo>> autocompletionDropdownBounded(
            KafkaConsumer<String, String> consumer) {
        return consumer.listTopics(READ_TIMEOUT);
    }

    public Map<String, List<PartitionInfo>> scheduledCatalogRefreshBounded(
            KafkaConsumer<String, String> consumer) {
        return consumer.listTopics(READ_TIMEOUT);
    }

    public Map<String, List<PartitionInfo>> readinessProbeBounded(
            Consumer<String, String> consumer) {
        return consumer.listTopics(READ_TIMEOUT);
    }

    public TopicCatalogReader buildReaderBounded(KafkaConsumer<String, String> consumer) {
        return () -> consumer.listTopics(READ_TIMEOUT);
    }

    public TopicCatalogReader buildReaderInterfaceBounded(Consumer<String, String> consumer) {
        return () -> consumer.listTopics(READ_TIMEOUT);
    }

    public Supplier<Map<String, List<PartitionInfo>>> buildReaderSupplierBounded(
            KafkaConsumer<String, String> consumer) {
        return () -> consumer.listTopics(READ_TIMEOUT);
    }

    public ScheduledFuture<?> scheduleListTopicsReadBounded(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer) {
        Callable<Map<String, List<PartitionInfo>>> task = () -> consumer.listTopics(READ_TIMEOUT);
        return scheduler.schedule(task, 30, TimeUnit.SECONDS);
    }

    public Future<Map<String, List<PartitionInfo>>> submitListTopicsReadBounded(
            ExecutorService executor,
            KafkaConsumer<String, String> consumer) {
        Callable<Map<String, List<PartitionInfo>>> reader = () -> consumer.listTopics(READ_TIMEOUT);
        return executor.submit(reader);
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new GoodConsumerListTopicsNoTimeout().autocompletionDropdownBounded(c);
        }
    }
}
