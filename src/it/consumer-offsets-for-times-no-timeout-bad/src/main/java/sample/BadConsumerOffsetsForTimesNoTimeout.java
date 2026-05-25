package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

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
 * RULE: CONSUMER_OFFSETS_FOR_TIMES_NO_TIMEOUT — must fire on EVERY
 * method below.
 *
 * <p>Each method exercises one of the bytecode shapes the rule is
 * required to catch:
 *
 * <ol>
 *   <li>direct {@code INVOKEVIRTUAL} on
 *       {@code KafkaConsumer.offsetsForTimes(Map)Ljava/util/Map;};</li>
 *   <li>direct {@code INVOKEINTERFACE} on
 *       {@code Consumer.offsetsForTimes(Map)Ljava/util/Map;};</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code consumer::offsetsForTimes} bound to {@link Function};</li>
 *   <li>{@code INVOKEDYNAMIC} capture against the {@link Consumer}
 *       interface-typed receiver (REF_invokeInterface) vs. against
 *       the {@link KafkaConsumer} typed receiver (REF_invokeVirtual)
 *       — both are caught;</li>
 *   <li>{@link Callable}-wrapped dispatch via
 *       {@link ScheduledExecutorService} and {@link ExecutorService}.</li>
 * </ol>
 *
 * <h2>Why the bounded {@code offsetsForTimes(Map, Duration)} overload
 * exists</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer#offsetsForTimes(Map)}
 * is documented as equivalent to {@code offsetsForTimes(
 * timestampsToSearch, Duration.ofMillis(defaultApiTimeoutMs))} where
 * {@code default.api.timeout.ms} defaults to 60 s — the
 * ListOffsetsForTimestamp request is retried against the partition
 * leader until that budget expires, with no shorter caller-side
 * bound available. Point-in-time replay tooling and audit jobs that
 * call this overload pin threads for a full minute during broker
 * incidents.
 */
public final class BadConsumerOffsetsForTimesNoTimeout {

    @FunctionalInterface
    interface TimestampToOffsetReader {
        Map<TopicPartition, OffsetAndTimestamp> read(
                Map<TopicPartition, Long> timestampsToSearch);
    }

    // ===== Direct INVOKEVIRTUAL on KafkaConsumer.offsetsForTimes(Map) =====

    /**
     * MUST FIRE — direct INVOKEVIRTUAL on
     * {@code KafkaConsumer.offsetsForTimes(Map)Ljava/util/Map;}.
     * Point-in-time replay kickoff: operator chooses to replay from
     * "10 minutes before the incident"; tooling calls
     * offsetsForTimes to map each partition's timestamp to an offset,
     * then seek + replay. Under broker slowness the call blocks for
     * the full 60 s default.api.timeout.ms.
     */
    public Map<TopicPartition, OffsetAndTimestamp> replayKickoff(
            KafkaConsumer<String, String> consumer,
            Map<TopicPartition, Long> timestampsToSearch) {
        return consumer.offsetsForTimes(timestampsToSearch);
    }

    /**
     * MUST FIRE — nightly audit job. Maps a per-partition target
     * timestamp ("the offset at midnight UTC") to confirm retention
     * windows. Under broker outage the job stalls for 60 s per call.
     */
    public Map<TopicPartition, OffsetAndTimestamp> nightlyAudit(
            KafkaConsumer<String, String> consumer,
            Map<TopicPartition, Long> timestampsToSearch) {
        return consumer.offsetsForTimes(timestampsToSearch);
    }

    // ===== Direct INVOKEINTERFACE on Consumer.offsetsForTimes(Map) =====

    /**
     * MUST FIRE — interface-typed receiver. INVOKEINTERFACE on
     * {@code org/apache/kafka/clients/consumer/Consumer.offsetsForTimes(
     * Map)Ljava/util/Map;}. Common in code that programs against the
     * Consumer interface for testability (lineage tooling, regulatory
     * query handlers).
     */
    public Map<TopicPartition, OffsetAndTimestamp> lineageQueryViaInterface(
            Consumer<String, String> consumer,
            Map<TopicPartition, Long> timestampsToSearch) {
        return consumer.offsetsForTimes(timestampsToSearch);
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /**
     * MUST FIRE — {@code consumer::offsetsForTimes} bound to a custom
     * functional interface whose erased SAM signature matches
     * {@code (Ljava/util/Map;)Ljava/util/Map;}. INVOKEDYNAMIC bsm-args
     * contain a REF_invokeVirtual handle pointing at
     * {@code KafkaConsumer.offsetsForTimes(Map)Ljava/util/Map;}.
     */
    public TimestampToOffsetReader buildReader(KafkaConsumer<String, String> consumer) {
        return consumer::offsetsForTimes;
    }

    /**
     * MUST FIRE — same indy capture against the Consumer
     * interface-typed receiver. Handle is REF_invokeInterface; the
     * (owner, name, desc) tuple matches.
     */
    public TimestampToOffsetReader buildReaderInterface(Consumer<String, String> consumer) {
        return consumer::offsetsForTimes;
    }

    /**
     * MUST FIRE — {@code consumer::offsetsForTimes} bound to
     * {@link Function}{@code <Map<TopicPartition, Long>,
     * Map<TopicPartition, OffsetAndTimestamp>>}.
     */
    public Function<Map<TopicPartition, Long>, Map<TopicPartition, OffsetAndTimestamp>>
            buildReaderFunction(KafkaConsumer<String, String> consumer) {
        return consumer::offsetsForTimes;
    }

    /**
     * MUST FIRE — explicit lambda body that invokes
     * {@code consumer.offsetsForTimes(...)}. The synthetic lambda
     * method's bytecode contains a direct INVOKEVIRTUAL on the
     * no-Duration overload — the rule walks all methods on the class,
     * including synthetic lambda bodies, and fires there.
     */
    public ScheduledFuture<?> scheduleOffsetsForTimesRead(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer,
            Map<TopicPartition, Long> timestampsToSearch) {
        Callable<Map<TopicPartition, OffsetAndTimestamp>> task =
                () -> consumer.offsetsForTimes(timestampsToSearch);
        return scheduler.schedule(task, 30, TimeUnit.SECONDS);
    }

    /**
     * MUST FIRE — Function method-reference captured and submitted to
     * an {@link ExecutorService}.
     */
    public Future<Map<TopicPartition, OffsetAndTimestamp>> submitOffsetsForTimesRead(
            ExecutorService executor,
            KafkaConsumer<String, String> consumer,
            Map<TopicPartition, Long> timestampsToSearch) {
        Function<Map<TopicPartition, Long>, Map<TopicPartition, OffsetAndTimestamp>> reader =
                consumer::offsetsForTimes;
        return executor.submit(() -> reader.apply(timestampsToSearch));
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new BadConsumerOffsetsForTimesNoTimeout().replayKickoff(c, java.util.Map.of());
        }
    }
}
