package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * RULE: CONSUMER_PARTITIONS_FOR_NO_TIMEOUT — must NOT fire on any
 * method below.
 *
 * <p>Mirror of {@code BadConsumerPartitionsForNoTimeout} where every
 * call site uses the bounded {@code partitionsFor(String, Duration)}
 * overload. Descriptor
 * {@code (Ljava/lang/String;Ljava/time/Duration;)Ljava/util/List;} —
 * never matches the rule's no-Duration descriptor.
 */
public final class GoodConsumerPartitionsForNoTimeout {

    private static final Duration READ_TIMEOUT = Duration.ofSeconds(5);

    @FunctionalInterface
    interface PartitionListReader {
        List<PartitionInfo> read(String topic);
    }

    public List<PartitionInfo> startupHealthProbeBounded(
            KafkaConsumer<String, String> consumer,
            String topic) {
        return consumer.partitionsFor(topic, READ_TIMEOUT);
    }

    public List<PartitionInfo> topicDiscoveryProbeBounded(
            KafkaConsumer<String, String> consumer,
            String topic) {
        return consumer.partitionsFor(topic, READ_TIMEOUT);
    }

    public List<PartitionInfo> topicExistenceCheckBounded(
            Consumer<String, String> consumer,
            String topic) {
        return consumer.partitionsFor(topic, READ_TIMEOUT);
    }

    public PartitionListReader buildReaderBounded(KafkaConsumer<String, String> consumer) {
        return t -> consumer.partitionsFor(t, READ_TIMEOUT);
    }

    public PartitionListReader buildReaderInterfaceBounded(Consumer<String, String> consumer) {
        return t -> consumer.partitionsFor(t, READ_TIMEOUT);
    }

    public Function<String, List<PartitionInfo>> buildReaderFunctionBounded(
            KafkaConsumer<String, String> consumer) {
        return t -> consumer.partitionsFor(t, READ_TIMEOUT);
    }

    public ScheduledFuture<?> schedulePartitionsForReadBounded(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer,
            String topic) {
        Callable<List<PartitionInfo>> task = () -> consumer.partitionsFor(topic, READ_TIMEOUT);
        return scheduler.schedule(task, 30, TimeUnit.SECONDS);
    }

    public Future<List<PartitionInfo>> submitPartitionsForReadBounded(
            ExecutorService executor,
            KafkaConsumer<String, String> consumer,
            String topic) {
        Function<String, List<PartitionInfo>> reader = t -> consumer.partitionsFor(t, READ_TIMEOUT);
        return executor.submit(() -> reader.apply(topic));
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new GoodConsumerPartitionsForNoTimeout().startupHealthProbeBounded(c, "t");
        }
    }
}
