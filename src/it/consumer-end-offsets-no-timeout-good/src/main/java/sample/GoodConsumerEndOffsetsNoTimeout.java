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
 * RULE: CONSUMER_END_OFFSETS_NO_TIMEOUT — must NOT fire on any method
 * below.
 *
 * <p>Mirror of {@code BadConsumerEndOffsetsNoTimeout} where every call
 * site uses the bounded {@code endOffsets(Collection, Duration)}
 * overload. The descriptor is
 * {@code (Ljava/util/Collection;Ljava/time/Duration;)Ljava/util/Map;}
 * — neither matches the rule's no-Duration descriptor
 * {@code (Ljava/util/Collection;)Ljava/util/Map;}.
 *
 * <p>The functional-interface SAMs used in the BAD fixture
 * ({@code OffsetReader}, {@link Function}) accept a single
 * {@link Collection} parameter; the bounded overload takes
 * {@code (Collection, Duration)} which doesn't directly fit. To
 * preserve the migration shape the lambdas below explicitly invoke
 * the bounded overload with a pre-chosen Duration — the indy capture
 * either targets a synthetic lambda method (descriptor mismatch) or
 * the bounded overload itself (descriptor mismatch). Either way the
 * rule does not fire.
 */
public final class GoodConsumerEndOffsetsNoTimeout {

    private static final Duration READ_TIMEOUT = Duration.ofSeconds(5);

    @FunctionalInterface
    interface OffsetReader {
        Map<TopicPartition, Long> read(Collection<TopicPartition> partitions);
    }

    /**
     * OK — bounded INVOKEVIRTUAL on
     * {@code KafkaConsumer.endOffsets(Collection, Duration)}.
     * Descriptor
     * {@code (Ljava/util/Collection;Ljava/time/Duration;)Ljava/util/Map;};
     * the rule's predicate matches only
     * {@code (Ljava/util/Collection;)Ljava/util/Map;}.
     */
    public Map<TopicPartition, Long> lagMonitorTickBounded(
            KafkaConsumer<String, String> consumer,
            Collection<TopicPartition> assignment) {
        return consumer.endOffsets(assignment, READ_TIMEOUT);
    }

    /**
     * OK — bounded replay-tool kickoff. A slow leader surfaces a
     * TimeoutException after 5 s rather than a 60 s hang.
     */
    public Map<TopicPartition, Long> replayKickoffBounded(
            KafkaConsumer<String, String> consumer,
            Collection<TopicPartition> partitions) {
        return consumer.endOffsets(partitions, READ_TIMEOUT);
    }

    /**
     * OK — interface-typed receiver, bounded overload.
     */
    public Map<TopicPartition, Long> healthProbeBounded(
            Consumer<String, String> consumer,
            Collection<TopicPartition> partitions) {
        return consumer.endOffsets(partitions, READ_TIMEOUT);
    }

    /**
     * OK — the OffsetReader functional interface is satisfied by an
     * explicit lambda that calls the bounded overload. The
     * INVOKEDYNAMIC bsm-args target either a synthetic lambda method
     * (descriptor mismatch) or the bounded overload (descriptor
     * mismatch); the rule's predicate sees neither.
     */
    public OffsetReader buildOffsetReaderBounded(KafkaConsumer<String, String> consumer) {
        return partitions -> consumer.endOffsets(partitions, READ_TIMEOUT);
    }

    /**
     * OK — interface-typed receiver bounded reader.
     */
    public OffsetReader buildOffsetReaderInterfaceBounded(Consumer<String, String> consumer) {
        return partitions -> consumer.endOffsets(partitions, READ_TIMEOUT);
    }

    /**
     * OK — Function-typed bounded reader.
     */
    public Function<Collection<TopicPartition>, Map<TopicPartition, Long>>
            buildOffsetFunctionBounded(KafkaConsumer<String, String> consumer) {
        return partitions -> consumer.endOffsets(partitions, READ_TIMEOUT);
    }

    /**
     * OK — Callable bounded reader scheduled on a worker pool.
     */
    public ScheduledFuture<?> scheduleEndOffsetsReadBounded(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer,
            Collection<TopicPartition> partitions) {
        Callable<Map<TopicPartition, Long>> task =
                () -> consumer.endOffsets(partitions, READ_TIMEOUT);
        return scheduler.schedule(task, 30, TimeUnit.SECONDS);
    }

    /**
     * OK — Function bounded reader submitted to an ExecutorService.
     */
    public Future<Map<TopicPartition, Long>> submitEndOffsetsReadBounded(
            ExecutorService executor,
            KafkaConsumer<String, String> consumer,
            Collection<TopicPartition> partitions) {
        Function<Collection<TopicPartition>, Map<TopicPartition, Long>> reader =
                p -> consumer.endOffsets(p, READ_TIMEOUT);
        return executor.submit(() -> reader.apply(partitions));
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new GoodConsumerEndOffsetsNoTimeout().lagMonitorTickBounded(c, java.util.Set.of());
        }
    }
}
