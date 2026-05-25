package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

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
 * RULE: CONSUMER_COMMITTED_NO_TIMEOUT — must fire on EVERY method
 * below.
 *
 * <p>Each method exercises one of the bytecode shapes the rule is
 * required to catch:
 *
 * <ol>
 *   <li>direct {@code INVOKEVIRTUAL} on
 *       {@code KafkaConsumer.committed(Set)Ljava/util/Map;};</li>
 *   <li>direct {@code INVOKEINTERFACE} on
 *       {@code Consumer.committed(Set)Ljava/util/Map;};</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code consumer::committed} bound to a custom SAM
 *       ({@code CommittedReader}) or to {@link Function};</li>
 *   <li>{@code INVOKEDYNAMIC} capture against the {@link Consumer}
 *       interface-typed receiver (REF_invokeInterface) vs. against
 *       the {@link KafkaConsumer} typed receiver (REF_invokeVirtual)
 *       — both are caught;</li>
 *   <li>{@link Callable}-wrapped dispatch via
 *       {@link ScheduledExecutorService} and {@link ExecutorService}.</li>
 * </ol>
 *
 * <h2>Why the bounded {@code committed(Set, Duration)} overload
 * exists</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer#committed(Set)}
 * is documented as equivalent to {@code committed(partitions,
 * Duration.ofMillis(defaultApiTimeoutMs))} where
 * {@code default.api.timeout.ms} defaults to 60 s. Under coordinator
 * outage the call blocks for the full 60 s budget. Lag monitoring
 * is one of the most common consumers of this overload — and the
 * operational dynamic is paradoxical: the tool that's supposed to
 * alert on coordinator incidents pins its own threads during
 * coordinator incidents and stops emitting fresh lag samples right
 * when those samples matter most.
 */
public final class BadConsumerCommittedNoTimeout {

    @FunctionalInterface
    interface CommittedReader {
        Map<TopicPartition, OffsetAndMetadata> read(Set<TopicPartition> partitions);
    }

    // ===== Direct INVOKEVIRTUAL on KafkaConsumer.committed(Set) =====

    /**
     * MUST FIRE — direct INVOKEVIRTUAL on
     * {@code KafkaConsumer.committed(Set)Ljava/util/Map;}. Lag
     * monitor: a `@Scheduled(fixedDelay = 30_000)` job calls
     * `consumer.committed(assignment)` every 30 s, diffs against
     * endOffsets, emits a consumer_lag metric. Under coordinator
     * outage each tick takes 60 s; ticks overlap; the lag metric
     * goes stale precisely when it matters most.
     */
    public Map<TopicPartition, OffsetAndMetadata> lagMonitorTick(
            KafkaConsumer<String, String> consumer,
            Set<TopicPartition> partitions) {
        return consumer.committed(partitions);
    }

    /**
     * MUST FIRE — direct INVOKEVIRTUAL. Graceful shutdown final-commit
     * recorder: records committed offsets to a sidecar store before
     * close; under coordinator slowness the call eats the entire 60 s
     * grace period.
     */
    public Map<TopicPartition, OffsetAndMetadata> gracefulShutdownFinalCommit(
            KafkaConsumer<String, String> consumer,
            Set<TopicPartition> partitions) {
        return consumer.committed(partitions);
    }

    // ===== Direct INVOKEINTERFACE on Consumer.committed(Set) =====

    /**
     * MUST FIRE — interface-typed receiver. INVOKEINTERFACE on
     * {@code org/apache/kafka/clients/consumer/Consumer.committed(
     * Set)Ljava/util/Map;}. HTTP admin endpoint exposes 'committed
     * offset per partition' for ad-hoc operator queries; under
     * coordinator slowness each request blocks for 60 s; the
     * Jetty/Tomcat handler pool fills up; new requests are rejected
     * with 503.
     */
    public Map<TopicPartition, OffsetAndMetadata> httpAdminCommittedEndpoint(
            Consumer<String, String> consumer,
            Set<TopicPartition> partitions) {
        return consumer.committed(partitions);
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /**
     * MUST FIRE — {@code consumer::committed} bound to a custom
     * functional interface whose erased SAM signature matches
     * {@code (Ljava/util/Set;)Ljava/util/Map;}. INVOKEDYNAMIC
     * bsm-args contain a REF_invokeVirtual handle pointing at
     * {@code KafkaConsumer.committed(Set)Ljava/util/Map;}.
     */
    public CommittedReader buildReader(KafkaConsumer<String, String> consumer) {
        return consumer::committed;
    }

    /**
     * MUST FIRE — same indy capture against the Consumer
     * interface-typed receiver. Handle is REF_invokeInterface; the
     * (owner, name, desc) tuple matches.
     */
    public CommittedReader buildReaderInterface(Consumer<String, String> consumer) {
        return consumer::committed;
    }

    /**
     * MUST FIRE — {@code consumer::committed} bound to
     * {@link Function}{@code <Set<TopicPartition>, Map<TopicPartition,
     * OffsetAndMetadata>>}.
     */
    public Function<Set<TopicPartition>, Map<TopicPartition, OffsetAndMetadata>>
            buildReaderFunction(KafkaConsumer<String, String> consumer) {
        return consumer::committed;
    }

    /**
     * MUST FIRE — explicit lambda body that invokes
     * {@code consumer.committed(...)}. The synthetic lambda method's
     * bytecode contains a direct INVOKEVIRTUAL on the no-Duration
     * overload — the rule walks all methods on the class, including
     * synthetic lambda bodies, and fires there.
     */
    public ScheduledFuture<?> scheduleCommittedRead(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer,
            Set<TopicPartition> partitions) {
        Callable<Map<TopicPartition, OffsetAndMetadata>> task =
                () -> consumer.committed(partitions);
        return scheduler.schedule(task, 30, TimeUnit.SECONDS);
    }

    /**
     * MUST FIRE — Function method-reference captured and submitted to
     * an {@link ExecutorService}.
     */
    public Future<Map<TopicPartition, OffsetAndMetadata>> submitCommittedRead(
            ExecutorService executor,
            KafkaConsumer<String, String> consumer,
            Set<TopicPartition> partitions) {
        Function<Set<TopicPartition>, Map<TopicPartition, OffsetAndMetadata>> reader =
                consumer::committed;
        return executor.submit(() -> reader.apply(partitions));
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new BadConsumerCommittedNoTimeout().lagMonitorTick(c, Set.of());
        }
    }
}
