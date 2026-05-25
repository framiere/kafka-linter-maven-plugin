package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

/**
 * RULE: CONSUMER_POSITION_NO_TIMEOUT — must NOT fire on any
 * method below.
 *
 * <p>Mirror of {@code BadConsumerPositionNoTimeout} where every
 * call site uses the bounded {@code position(TopicPartition,
 * Duration)} overload. Descriptor
 * {@code (Lorg/apache/kafka/common/TopicPartition;Ljava/time/Duration;)J}
 * — never matches the rule's no-Duration descriptor.
 */
public final class GoodConsumerPositionNoTimeout {

    private static final Duration READ_TIMEOUT = Duration.ofSeconds(5);

    @FunctionalInterface
    interface PositionReader {
        long get(TopicPartition tp);
    }

    public long perPartitionLagProbeBounded(
            KafkaConsumer<String, String> consumer,
            TopicPartition tp) {
        return consumer.position(tp, READ_TIMEOUT);
    }

    public long gracefulShutdownFinalPositionBounded(
            KafkaConsumer<String, String> consumer,
            TopicPartition tp) {
        return consumer.position(tp, READ_TIMEOUT);
    }

    public long httpAdminPositionEndpointBounded(
            Consumer<String, String> consumer,
            TopicPartition tp) {
        return consumer.position(tp, READ_TIMEOUT);
    }

    public PositionReader buildReaderBounded(KafkaConsumer<String, String> consumer) {
        return tp -> consumer.position(tp, READ_TIMEOUT);
    }

    public PositionReader buildReaderInterfaceBounded(Consumer<String, String> consumer) {
        return tp -> consumer.position(tp, READ_TIMEOUT);
    }

    public ToLongFunction<TopicPartition> buildReaderToLongFunctionBounded(
            KafkaConsumer<String, String> consumer) {
        return tp -> consumer.position(tp, READ_TIMEOUT);
    }

    public ScheduledFuture<?> schedulePositionReadBounded(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer,
            TopicPartition tp) {
        Callable<Long> task = () -> consumer.position(tp, READ_TIMEOUT);
        return scheduler.schedule(task, 30, TimeUnit.SECONDS);
    }

    public Future<Long> submitPositionReadBounded(
            ExecutorService executor,
            KafkaConsumer<String, String> consumer,
            TopicPartition tp) {
        ToLongFunction<TopicPartition> reader = t -> consumer.position(t, READ_TIMEOUT);
        return executor.submit(() -> reader.applyAsLong(tp));
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new GoodConsumerPositionNoTimeout().perPartitionLagProbeBounded(
                    c, new TopicPartition("t", 0));
        }
    }
}
