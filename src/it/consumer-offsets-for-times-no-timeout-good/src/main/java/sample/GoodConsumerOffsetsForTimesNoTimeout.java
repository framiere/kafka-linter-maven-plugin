package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
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
 * RULE: CONSUMER_OFFSETS_FOR_TIMES_NO_TIMEOUT — must NOT fire on any
 * method below.
 *
 * <p>Mirror of {@code BadConsumerOffsetsForTimesNoTimeout} where every
 * call site uses the bounded {@code offsetsForTimes(Map, Duration)}
 * overload. Descriptor
 * {@code (Ljava/util/Map;Ljava/time/Duration;)Ljava/util/Map;} — never
 * matches the rule's no-Duration descriptor.
 */
public final class GoodConsumerOffsetsForTimesNoTimeout {

    private static final Duration READ_TIMEOUT = Duration.ofSeconds(5);

    @FunctionalInterface
    interface TimestampToOffsetReader {
        Map<TopicPartition, OffsetAndTimestamp> read(
                Map<TopicPartition, Long> timestampsToSearch);
    }

    public Map<TopicPartition, OffsetAndTimestamp> replayKickoffBounded(
            KafkaConsumer<String, String> consumer,
            Map<TopicPartition, Long> timestampsToSearch) {
        return consumer.offsetsForTimes(timestampsToSearch, READ_TIMEOUT);
    }

    public Map<TopicPartition, OffsetAndTimestamp> nightlyAuditBounded(
            KafkaConsumer<String, String> consumer,
            Map<TopicPartition, Long> timestampsToSearch) {
        return consumer.offsetsForTimes(timestampsToSearch, READ_TIMEOUT);
    }

    public Map<TopicPartition, OffsetAndTimestamp> lineageQueryBounded(
            Consumer<String, String> consumer,
            Map<TopicPartition, Long> timestampsToSearch) {
        return consumer.offsetsForTimes(timestampsToSearch, READ_TIMEOUT);
    }

    public TimestampToOffsetReader buildReaderBounded(KafkaConsumer<String, String> consumer) {
        return t -> consumer.offsetsForTimes(t, READ_TIMEOUT);
    }

    public TimestampToOffsetReader buildReaderInterfaceBounded(Consumer<String, String> consumer) {
        return t -> consumer.offsetsForTimes(t, READ_TIMEOUT);
    }

    public Function<Map<TopicPartition, Long>, Map<TopicPartition, OffsetAndTimestamp>>
            buildReaderFunctionBounded(KafkaConsumer<String, String> consumer) {
        return t -> consumer.offsetsForTimes(t, READ_TIMEOUT);
    }

    public ScheduledFuture<?> scheduleOffsetsForTimesReadBounded(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer,
            Map<TopicPartition, Long> timestampsToSearch) {
        Callable<Map<TopicPartition, OffsetAndTimestamp>> task =
                () -> consumer.offsetsForTimes(timestampsToSearch, READ_TIMEOUT);
        return scheduler.schedule(task, 30, TimeUnit.SECONDS);
    }

    public Future<Map<TopicPartition, OffsetAndTimestamp>> submitOffsetsForTimesReadBounded(
            ExecutorService executor,
            KafkaConsumer<String, String> consumer,
            Map<TopicPartition, Long> timestampsToSearch) {
        Function<Map<TopicPartition, Long>, Map<TopicPartition, OffsetAndTimestamp>> reader =
                t -> consumer.offsetsForTimes(t, READ_TIMEOUT);
        return executor.submit(() -> reader.apply(timestampsToSearch));
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new GoodConsumerOffsetsForTimesNoTimeout().replayKickoffBounded(c, java.util.Map.of());
        }
    }
}
