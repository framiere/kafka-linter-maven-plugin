package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

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
 * RULE: CONSUMER_BEGINNING_OFFSETS_NO_TIMEOUT — must fire on EVERY
 * method below.
 *
 * <p>Each method exercises one of the bytecode shapes the rule is
 * required to catch:
 *
 * <ol>
 *   <li>direct {@code INVOKEVIRTUAL} on
 *       {@code KafkaConsumer.beginningOffsets(Collection)Ljava/util/Map;};</li>
 *   <li>direct {@code INVOKEINTERFACE} on
 *       {@code Consumer.beginningOffsets(Collection)Ljava/util/Map;};</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code consumer::beginningOffsets} bound to {@link Function};</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture against a
 *       {@link Consumer} interface-typed receiver (REF_invokeInterface)
 *       vs. against a {@link KafkaConsumer} typed receiver
 *       (REF_invokeVirtual) — both are caught;</li>
 *   <li>{@link Callable}-wrapped dispatch via
 *       {@link ScheduledExecutorService} and {@link ExecutorService}.</li>
 * </ol>
 *
 * <h2>Why the bounded {@code beginningOffsets(Collection, Duration)}
 * overload exists</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer#beginningOffsets(Collection)}
 * is documented as equivalent to {@code beginningOffsets(partitions,
 * Duration.ofMillis(defaultApiTimeoutMs))} where
 * {@code default.api.timeout.ms} defaults to 60 s — the ListOffsets
 * request is retried against the partition leader until that budget
 * expires, with no shorter caller-side bound available. Retention /
 * SLA monitors and replay-from-earliest tooling that call this
 * overload pin threads for a full minute during exactly the broker
 * incidents they exist to detect.
 */
public final class BadConsumerBeginningOffsetsNoTimeout {

    @FunctionalInterface
    interface EarliestOffsetReader {
        Map<TopicPartition, Long> read(Collection<TopicPartition> partitions);
    }

    // ===== Direct INVOKEVIRTUAL on KafkaConsumer.beginningOffsets(Collection) =====

    /**
     * MUST FIRE — direct INVOKEVIRTUAL on
     * {@code KafkaConsumer.beginningOffsets(Collection)Ljava/util/Map;}.
     * Retention monitor agent: poll every N seconds and alert when the
     * earliest offset advances past a configured threshold; under
     * broker outage the call blocks for the full 60 s
     * default.api.timeout.ms.
     */
    public Map<TopicPartition, Long> retentionMonitorTick(
            KafkaConsumer<String, String> consumer,
            Collection<TopicPartition> assignment) {
        return consumer.beginningOffsets(assignment);
    }

    /**
     * MUST FIRE — replay-from-earliest kickoff. Operator workflow:
     * call beginningOffsets to discover the earliest available offset
     * (so seek does not target an offset retention has deleted), then
     * seek + replay. Under broker slowness the operator cannot
     * distinguish "tool hung" from "cluster slow".
     */
    public Map<TopicPartition, Long> replayFromEarliestKickoff(
            KafkaConsumer<String, String> consumer,
            Collection<TopicPartition> partitions) {
        return consumer.beginningOffsets(partitions);
    }

    // ===== Direct INVOKEINTERFACE on Consumer.beginningOffsets(Collection) =====

    /**
     * MUST FIRE — interface-typed receiver. INVOKEINTERFACE on
     * {@code org/apache/kafka/clients/consumer/Consumer.beginningOffsets(
     * Collection)Ljava/util/Map;}.
     */
    public Map<TopicPartition, Long> healthProbeViaInterface(
            Consumer<String, String> consumer,
            Collection<TopicPartition> partitions) {
        return consumer.beginningOffsets(partitions);
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /**
     * MUST FIRE — {@code consumer::beginningOffsets} bound to a custom
     * functional interface whose erased SAM signature matches
     * {@code (Ljava/util/Collection;)Ljava/util/Map;}. INVOKEDYNAMIC
     * bsm-args contain a REF_invokeVirtual handle pointing at
     * {@code KafkaConsumer.beginningOffsets(Collection)Ljava/util/Map;}.
     */
    public EarliestOffsetReader buildEarliestReader(KafkaConsumer<String, String> consumer) {
        return consumer::beginningOffsets;
    }

    /**
     * MUST FIRE — same indy capture against the Consumer
     * interface-typed receiver. Handle is REF_invokeInterface; the
     * (owner, name, desc) tuple matches and the rule fires.
     */
    public EarliestOffsetReader buildEarliestReaderInterface(Consumer<String, String> consumer) {
        return consumer::beginningOffsets;
    }

    /**
     * MUST FIRE — {@code consumer::beginningOffsets} bound to
     * {@link Function}{@code <Collection<TopicPartition>,
     * Map<TopicPartition, Long>>}.
     */
    public Function<Collection<TopicPartition>, Map<TopicPartition, Long>>
            buildEarliestFunction(KafkaConsumer<String, String> consumer) {
        return consumer::beginningOffsets;
    }

    /**
     * MUST FIRE — explicit lambda body that invokes
     * {@code consumer.beginningOffsets(partitions)} directly. The
     * synthetic lambda method's bytecode contains a direct
     * INVOKEVIRTUAL on the no-Duration overload — the rule walks all
     * methods on the class, including synthetic lambda bodies, and
     * fires there.
     */
    public ScheduledFuture<?> scheduleBeginningOffsetsRead(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer,
            Collection<TopicPartition> partitions) {
        Callable<Map<TopicPartition, Long>> task = () -> consumer.beginningOffsets(partitions);
        return scheduler.schedule(task, 30, TimeUnit.SECONDS);
    }

    /**
     * MUST FIRE — Function method-reference captured and submitted to
     * an {@link ExecutorService}.
     */
    public Future<Map<TopicPartition, Long>> submitBeginningOffsetsRead(
            ExecutorService executor,
            KafkaConsumer<String, String> consumer,
            Collection<TopicPartition> partitions) {
        Function<Collection<TopicPartition>, Map<TopicPartition, Long>> reader =
                consumer::beginningOffsets;
        return executor.submit(() -> reader.apply(partitions));
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new BadConsumerBeginningOffsetsNoTimeout()
                    .retentionMonitorTick(c, java.util.Set.of());
        }
    }
}
