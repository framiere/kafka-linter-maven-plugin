package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * RULE: CONSUMER_BEGINNING_OFFSETS_NO_TIMEOUT — must NOT fire on any
 * method below.
 *
 * <p>Mirror of {@code BadConsumerBeginningOffsetsNoTimeout} where every
 * call site uses the bounded {@code beginningOffsets(Collection,
 * Duration)} overload. Descriptor
 * {@code (Ljava/util/Collection;Ljava/time/Duration;)Ljava/util/Map;}
 * — never matches the rule's no-Duration descriptor.
 */
public final class GoodConsumerBeginningOffsetsNoTimeout {

    private static final Duration READ_TIMEOUT = Duration.ofSeconds(5);

    @FunctionalInterface
    interface EarliestOffsetReader {
        Map<TopicPartition, Long> read(Collection<TopicPartition> partitions);
    }

    public Map<TopicPartition, Long> retentionMonitorTickBounded(
            KafkaConsumer<String, String> consumer,
            Collection<TopicPartition> assignment) {
        return consumer.beginningOffsets(assignment, READ_TIMEOUT);
    }

    public Map<TopicPartition, Long> replayFromEarliestKickoffBounded(
            KafkaConsumer<String, String> consumer,
            Collection<TopicPartition> partitions) {
        return consumer.beginningOffsets(partitions, READ_TIMEOUT);
    }

    public Map<TopicPartition, Long> healthProbeBounded(
            Consumer<String, String> consumer,
            Collection<TopicPartition> partitions) {
        return consumer.beginningOffsets(partitions, READ_TIMEOUT);
    }

    public EarliestOffsetReader buildEarliestReaderBounded(KafkaConsumer<String, String> consumer) {
        return partitions -> consumer.beginningOffsets(partitions, READ_TIMEOUT);
    }

    public EarliestOffsetReader buildEarliestReaderInterfaceBounded(Consumer<String, String> consumer) {
        return partitions -> consumer.beginningOffsets(partitions, READ_TIMEOUT);
    }

    public Function<Collection<TopicPartition>, Map<TopicPartition, Long>>
            buildEarliestFunctionBounded(KafkaConsumer<String, String> consumer) {
        return partitions -> consumer.beginningOffsets(partitions, READ_TIMEOUT);
    }

    public ScheduledFuture<?> scheduleBeginningOffsetsReadBounded(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer,
            Collection<TopicPartition> partitions) {
        Callable<Map<TopicPartition, Long>> task =
                () -> consumer.beginningOffsets(partitions, READ_TIMEOUT);
        return scheduler.schedule(task, 30, TimeUnit.SECONDS);
    }

    public Future<Map<TopicPartition, Long>> submitBeginningOffsetsReadBounded(
            ExecutorService executor,
            KafkaConsumer<String, String> consumer,
            Collection<TopicPartition> partitions) {
        Function<Collection<TopicPartition>, Map<TopicPartition, Long>> reader =
                p -> consumer.beginningOffsets(p, READ_TIMEOUT);
        return executor.submit(() -> reader.apply(partitions));
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new GoodConsumerBeginningOffsetsNoTimeout()
                    .retentionMonitorTickBounded(c, java.util.Set.of());
        }
    }
}
