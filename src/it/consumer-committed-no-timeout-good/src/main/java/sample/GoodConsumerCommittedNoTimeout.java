package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * RULE: CONSUMER_COMMITTED_NO_TIMEOUT — must NOT fire on any method
 * below.
 *
 * <p>Mirror of {@code BadConsumerCommittedNoTimeout} where every
 * call site uses the bounded {@code committed(Set, Duration)}
 * overload. Descriptor
 * {@code (Ljava/util/Set;Ljava/time/Duration;)Ljava/util/Map;} —
 * never matches the rule's no-Duration descriptor.
 */
public final class GoodConsumerCommittedNoTimeout {

    private static final Duration READ_TIMEOUT = Duration.ofSeconds(5);

    @FunctionalInterface
    interface CommittedReader {
        Map<TopicPartition, OffsetAndMetadata> read(Set<TopicPartition> partitions);
    }

    public Map<TopicPartition, OffsetAndMetadata> lagMonitorTickBounded(
            KafkaConsumer<String, String> consumer,
            Set<TopicPartition> partitions) {
        return consumer.committed(partitions, READ_TIMEOUT);
    }

    public Map<TopicPartition, OffsetAndMetadata> gracefulShutdownFinalCommitBounded(
            KafkaConsumer<String, String> consumer,
            Set<TopicPartition> partitions) {
        return consumer.committed(partitions, READ_TIMEOUT);
    }

    public Map<TopicPartition, OffsetAndMetadata> httpAdminCommittedEndpointBounded(
            Consumer<String, String> consumer,
            Set<TopicPartition> partitions) {
        return consumer.committed(partitions, READ_TIMEOUT);
    }

    public CommittedReader buildReaderBounded(KafkaConsumer<String, String> consumer) {
        return p -> consumer.committed(p, READ_TIMEOUT);
    }

    public CommittedReader buildReaderInterfaceBounded(Consumer<String, String> consumer) {
        return p -> consumer.committed(p, READ_TIMEOUT);
    }

    public Function<Set<TopicPartition>, Map<TopicPartition, OffsetAndMetadata>>
            buildReaderFunctionBounded(KafkaConsumer<String, String> consumer) {
        return p -> consumer.committed(p, READ_TIMEOUT);
    }

    public ScheduledFuture<?> scheduleCommittedReadBounded(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer,
            Set<TopicPartition> partitions) {
        Callable<Map<TopicPartition, OffsetAndMetadata>> task =
                () -> consumer.committed(partitions, READ_TIMEOUT);
        return scheduler.schedule(task, 30, TimeUnit.SECONDS);
    }

    public Future<Map<TopicPartition, OffsetAndMetadata>> submitCommittedReadBounded(
            ExecutorService executor,
            KafkaConsumer<String, String> consumer,
            Set<TopicPartition> partitions) {
        Function<Set<TopicPartition>, Map<TopicPartition, OffsetAndMetadata>> reader =
                p -> consumer.committed(p, READ_TIMEOUT);
        return executor.submit(() -> reader.apply(partitions));
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new GoodConsumerCommittedNoTimeout().lagMonitorTickBounded(c, Set.of());
        }
    }
}
