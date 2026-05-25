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
 * RULE: CONSUMER_END_OFFSETS_NO_TIMEOUT — must fire on EVERY method
 * below.
 *
 * <p>Each method exercises one of the bytecode shapes the rule is
 * required to catch:
 *
 * <ol>
 *   <li>direct {@code INVOKEVIRTUAL} on
 *       {@code KafkaConsumer.endOffsets(Collection)Ljava/util/Map;};</li>
 *   <li>direct {@code INVOKEINTERFACE} on
 *       {@code Consumer.endOffsets(Collection)Ljava/util/Map;};</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code consumer::endOffsets} bound to {@link Function} (the
 *       universal shape when a lag-monitor agent abstracts the offset
 *       read behind a Function&lt;Collection, Map&gt;);</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code consumer::endOffsets} bound to {@link Callable} (typical
 *       when scheduling out-of-band offset reads via
 *       {@link ExecutorService#submit(Callable)});</li>
 *   <li>{@code INVOKEDYNAMIC} capture against a {@link Consumer}
 *       interface-typed receiver (REF_invokeInterface) vs. against a
 *       {@link KafkaConsumer} typed receiver (REF_invokeVirtual) —
 *       both are caught.</li>
 * </ol>
 *
 * <h2>Why the bounded {@code endOffsets(Collection, Duration)}
 * overload exists</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer#endOffsets(Collection)}
 * is documented as equivalent to {@code endOffsets(partitions,
 * Duration.ofMillis(defaultApiTimeoutMs))} where
 * {@code default.api.timeout.ms} defaults to 60 s — the ListOffsets
 * request is retried against the partition leader until that budget
 * expires, with no shorter caller-side bound available. Lag monitors
 * and admin tooling that call this overload pin threads for a full
 * minute during the exact broker outages they exist to detect.
 */
public final class BadConsumerEndOffsetsNoTimeout {

    @FunctionalInterface
    interface OffsetReader {
        Map<TopicPartition, Long> read(Collection<TopicPartition> partitions);
    }

    // ===== Direct INVOKEVIRTUAL on KafkaConsumer.endOffsets(Collection) =====

    /**
     * MUST FIRE — direct INVOKEVIRTUAL on
     * {@code KafkaConsumer.endOffsets(Collection)Ljava/util/Map;}. Lag
     * monitor agent: poll every N seconds, diff against committed
     * offsets, publish gauge. Under broker outage the call blocks for
     * the full 60 s default.api.timeout.ms; subsequent ticks queue
     * behind it.
     */
    public Map<TopicPartition, Long> lagMonitorTick(
            KafkaConsumer<String, String> consumer,
            Collection<TopicPartition> assignment) {
        return consumer.endOffsets(assignment);
    }

    /**
     * MUST FIRE — replay-tool kickoff. Operator workflow: discover an
     * issue, call endOffsets to bound the replay window, seek + replay.
     * Under broker slowness the operator cannot distinguish "tool
     * hung" from "cluster slow".
     */
    public Map<TopicPartition, Long> replayKickoff(
            KafkaConsumer<String, String> consumer,
            Collection<TopicPartition> partitions) {
        return consumer.endOffsets(partitions);
    }

    // ===== Direct INVOKEINTERFACE on Consumer.endOffsets(Collection) =====

    /**
     * MUST FIRE — interface-typed receiver. INVOKEINTERFACE on
     * {@code org/apache/kafka/clients/consumer/Consumer.endOffsets(
     * Collection)Ljava/util/Map;}. Common in code that programs
     * against the Consumer interface for testability.
     */
    public Map<TopicPartition, Long> healthProbeViaInterface(
            Consumer<String, String> consumer,
            Collection<TopicPartition> partitions) {
        return consumer.endOffsets(partitions);
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /**
     * MUST FIRE — {@code consumer::endOffsets} bound to a custom
     * functional interface whose erased SAM signature matches
     * {@code (Ljava/util/Collection;)Ljava/util/Map;}. The user-class
     * bytecode contains ZERO direct INVOKEVIRTUAL on
     * KafkaConsumer.endOffsets; only an INVOKEDYNAMIC +
     * LambdaMetafactory bridge whose bsm-args contain a
     * REF_invokeVirtual handle pointing at
     * {@code KafkaConsumer.endOffsets(Collection)Ljava/util/Map;}. A
     * naïve MethodInsnNode-only lint misses this entirely.
     */
    public OffsetReader buildOffsetReader(KafkaConsumer<String, String> consumer) {
        return consumer::endOffsets;
    }

    /**
     * MUST FIRE — same indy capture against the Consumer
     * interface-typed receiver. The bsm-args handle is
     * REF_invokeInterface; (owner, name, desc) tuple matches and the
     * rule fires.
     */
    public OffsetReader buildOffsetReaderInterface(Consumer<String, String> consumer) {
        return consumer::endOffsets;
    }

    /**
     * MUST FIRE — {@code consumer::endOffsets} bound to
     * {@link Function}{@code <Collection<TopicPartition>,
     * Map<TopicPartition, Long>>}, a generic functional-interface
     * shape used by lag-monitor agents that want to abstract the
     * offset read for unit testing.
     */
    public Function<Collection<TopicPartition>, Map<TopicPartition, Long>>
            buildOffsetFunction(KafkaConsumer<String, String> consumer) {
        return consumer::endOffsets;
    }

    /**
     * MUST FIRE — {@code consumer::endOffsets} bound to
     * {@link Callable}, scheduled via
     * {@link ScheduledExecutorService}. Scheduler worker-thread
     * pinning hazard: the executor's queue saturates when the
     * coordinator/leader is slow.
     */
    public ScheduledFuture<?> scheduleEndOffsetsRead(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer,
            Collection<TopicPartition> partitions) {
        Callable<Map<TopicPartition, Long>> task = () -> consumer.endOffsets(partitions);
        return scheduler.schedule(task, 30, TimeUnit.SECONDS);
    }

    /**
     * MUST FIRE — Function method-reference submitted to an
     * {@link ExecutorService}. The producer code captures
     * {@code consumer::endOffsets} as a Function then applies it on a
     * worker thread; the application call site is on the worker
     * thread, but the indy capture is on this method's bytecode.
     */
    public Future<Map<TopicPartition, Long>> submitEndOffsetsRead(
            ExecutorService executor,
            KafkaConsumer<String, String> consumer,
            Collection<TopicPartition> partitions) {
        Function<Collection<TopicPartition>, Map<TopicPartition, Long>> reader =
                consumer::endOffsets;
        return executor.submit(() -> reader.apply(partitions));
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new BadConsumerEndOffsetsNoTimeout().lagMonitorTick(c, java.util.Set.of());
        }
    }
}
