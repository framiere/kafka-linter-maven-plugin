package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

/**
 * RULE: CONSUMER_POSITION_NO_TIMEOUT — must fire on EVERY method below.
 *
 * <p>Each method exercises one of the bytecode shapes the rule is
 * required to catch:
 *
 * <ol>
 *   <li>direct {@code INVOKEVIRTUAL} on
 *       {@code KafkaConsumer.position(TopicPartition)J};</li>
 *   <li>direct {@code INVOKEINTERFACE} on
 *       {@code Consumer.position(TopicPartition)J};</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code consumer::position} bound to a custom SAM
 *       ({@code PositionReader}) or to {@link ToLongFunction};</li>
 *   <li>{@code INVOKEDYNAMIC} capture against the {@link Consumer}
 *       interface-typed receiver (REF_invokeInterface) vs. against
 *       the {@link KafkaConsumer} typed receiver (REF_invokeVirtual)
 *       — both are caught;</li>
 *   <li>{@link Callable}-wrapped dispatch via
 *       {@link ScheduledExecutorService} and {@link ExecutorService}.</li>
 * </ol>
 *
 * <h2>Why the bounded {@code position(TopicPartition, Duration)}
 * overload exists</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer#position(org.apache.kafka.common.TopicPartition)}
 * is documented as equivalent to {@code position(partition,
 * Duration.ofMillis(defaultApiTimeoutMs))} where
 * {@code default.api.timeout.ms} defaults to 60 s. When the consumer
 * holds a cached position the call returns immediately; when the
 * cached position is stale (just after a rebalance, after a seek to a
 * timestamp that needs server resolution, after an
 * {@code OFFSET_OUT_OF_RANGE} reset), the call sends an
 * OffsetFetch / ListOffsets / coordinator request and parks the
 * caller for the full 60 s budget under coordinator unavailability.
 * The hazard is multiplied per partition — a 12-partition assignment
 * can hang for 12 × 60 s = 12 minutes before the first
 * TimeoutException surfaces.
 */
public final class BadConsumerPositionNoTimeout {

    @FunctionalInterface
    interface PositionReader {
        long get(TopicPartition tp);
    }

    // ===== Direct INVOKEVIRTUAL on KafkaConsumer.position(TopicPartition) =====

    /**
     * MUST FIRE — direct INVOKEVIRTUAL on
     * {@code KafkaConsumer.position(TopicPartition)J}. Per-partition
     * lag probe loop: with 12 assigned partitions and a stale
     * coordinator the loop hangs for up to 12 × 60 s = 12 minutes
     * before the first TimeoutException surfaces.
     */
    public long perPartitionLagProbe(
            KafkaConsumer<String, String> consumer,
            TopicPartition tp) {
        return consumer.position(tp);
    }

    /**
     * MUST FIRE — direct INVOKEVIRTUAL on
     * {@code KafkaConsumer.position(TopicPartition)J}. Graceful
     * shutdown final-offset recorder: records final offsets to a
     * sidecar store before {@code close}; under coordinator slowness
     * the call eats the entire 60 s terminationGracePeriodSeconds —
     * kubelet sends SIGKILL, the sidecar store is not updated, the
     * next instance starts from a stale offset baseline.
     */
    public long gracefulShutdownFinalPosition(
            KafkaConsumer<String, String> consumer,
            TopicPartition tp) {
        return consumer.position(tp);
    }

    // ===== Direct INVOKEINTERFACE on Consumer.position(TopicPartition) =====

    /**
     * MUST FIRE — interface-typed receiver. INVOKEINTERFACE on
     * {@code org/apache/kafka/clients/consumer/Consumer.position(
     * TopicPartition)J}. HTTP admin endpoint that exposes 'current
     * position per partition' — under broker slowness each request
     * blocks for 60 s; the Jetty/Tomcat handler pool fills up; new
     * requests are rejected with 503; the dashboard goes blank during
     * the exact incident it exists to observe.
     */
    public long httpAdminPositionEndpoint(
            Consumer<String, String> consumer,
            TopicPartition tp) {
        return consumer.position(tp);
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /**
     * MUST FIRE — {@code consumer::position} bound to a custom
     * functional interface whose erased SAM signature matches
     * {@code (Lorg/apache/kafka/common/TopicPartition;)J}.
     * INVOKEDYNAMIC bsm-args contain a REF_invokeVirtual handle
     * pointing at {@code KafkaConsumer.position(TopicPartition)J}.
     */
    public PositionReader buildReader(KafkaConsumer<String, String> consumer) {
        return consumer::position;
    }

    /**
     * MUST FIRE — same indy capture against the Consumer
     * interface-typed receiver. Handle is REF_invokeInterface; the
     * (owner, name, desc) tuple matches.
     */
    public PositionReader buildReaderInterface(Consumer<String, String> consumer) {
        return consumer::position;
    }

    /**
     * MUST FIRE — {@code consumer::position} bound to
     * {@link ToLongFunction}{@code <TopicPartition>}. The SAM
     * {@code applyAsLong(T)J} erases to
     * {@code (Ljava/lang/Object;)J}; the bsm-arg "implementation
     * method" handle resolves to
     * {@code KafkaConsumer.position(TopicPartition)J}.
     */
    public ToLongFunction<TopicPartition> buildReaderToLongFunction(
            KafkaConsumer<String, String> consumer) {
        return consumer::position;
    }

    /**
     * MUST FIRE — explicit lambda body that invokes
     * {@code consumer.position(...)}. The synthetic lambda method's
     * bytecode contains a direct INVOKEVIRTUAL on the no-Duration
     * overload — the rule walks all methods on the class, including
     * synthetic lambda bodies, and fires there.
     */
    public ScheduledFuture<?> schedulePositionRead(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer,
            TopicPartition tp) {
        Callable<Long> task = () -> consumer.position(tp);
        return scheduler.schedule(task, 30, TimeUnit.SECONDS);
    }

    /**
     * MUST FIRE — ToLongFunction method-reference captured and
     * submitted to an {@link ExecutorService}.
     */
    public Future<Long> submitPositionRead(
            ExecutorService executor,
            KafkaConsumer<String, String> consumer,
            TopicPartition tp) {
        ToLongFunction<TopicPartition> reader = consumer::position;
        return executor.submit(() -> reader.applyAsLong(tp));
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new BadConsumerPositionNoTimeout().perPartitionLagProbe(
                    c, new TopicPartition("t", 0));
        }
    }
}
