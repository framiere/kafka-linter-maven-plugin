package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * RULE: CONSUMER_LIST_TOPICS_NO_TIMEOUT — must fire on EVERY method
 * below.
 *
 * <p>Each method exercises one of the bytecode shapes the rule is
 * required to catch:
 *
 * <ol>
 *   <li>direct {@code INVOKEVIRTUAL} on
 *       {@code KafkaConsumer.listTopics()Ljava/util/Map;};</li>
 *   <li>direct {@code INVOKEINTERFACE} on
 *       {@code Consumer.listTopics()Ljava/util/Map;};</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code consumer::listTopics} bound to a custom SAM
 *       ({@code TopicCatalogReader}), {@link Supplier}, or
 *       {@link Callable};</li>
 *   <li>{@code INVOKEDYNAMIC} capture against the {@link Consumer}
 *       interface-typed receiver (REF_invokeInterface) vs. against
 *       the {@link KafkaConsumer} typed receiver (REF_invokeVirtual)
 *       — both are caught;</li>
 *   <li>{@link Callable}-wrapped dispatch via
 *       {@link ScheduledExecutorService} and {@link ExecutorService}.</li>
 * </ol>
 *
 * <h2>Why the bounded {@code listTopics(Duration)} overload
 * exists</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer#listTopics()}
 * is documented as equivalent to {@code listTopics(
 * Duration.ofMillis(defaultApiTimeoutMs))} where
 * {@code default.api.timeout.ms} defaults to 60 s. The call returns
 * the full-cluster topic-metadata snapshot — payload size grows
 * linearly with cluster size (50 k-partition clusters return tens of
 * MB on the wire); the call also blocks for the full 60 s budget
 * under bootstrap unreachability, controller slowness, or
 * stale-metadata retargeting.
 */
public final class BadConsumerListTopicsNoTimeout {

    @FunctionalInterface
    interface TopicCatalogReader {
        Map<String, List<PartitionInfo>> readAll();
    }

    // ===== Direct INVOKEVIRTUAL on KafkaConsumer.listTopics() =====

    /**
     * MUST FIRE — direct INVOKEVIRTUAL on
     * {@code KafkaConsumer.listTopics()Ljava/util/Map;}. Autocompletion
     * dropdown: an internal admin UI has a 'send a test message to
     * topic' form where the topic dropdown is populated by calling
     * `consumer.listTopics()` on every focus-or-keystroke. Under
     * broker slowness the dropdown freezes for 60 s; the Jetty handler
     * thread is pinned; concurrent admin actions queue behind it;
     * the whole admin app appears wedged.
     */
    public Map<String, List<PartitionInfo>> autocompletionDropdown(
            KafkaConsumer<String, String> consumer) {
        return consumer.listTopics();
    }

    /**
     * MUST FIRE — direct INVOKEVIRTUAL. Scheduled metadata-cache
     * refresher: a `@Scheduled` method runs `consumer.listTopics()`
     * every 30 s to refresh an application-side topic catalog. Under
     * broker slowness each tick takes 60 s; ticks overlap; the
     * scheduler pool fills up.
     */
    public Map<String, List<PartitionInfo>> scheduledCatalogRefresh(
            KafkaConsumer<String, String> consumer) {
        return consumer.listTopics();
    }

    // ===== Direct INVOKEINTERFACE on Consumer.listTopics() =====

    /**
     * MUST FIRE — interface-typed receiver. INVOKEINTERFACE on
     * {@code org/apache/kafka/clients/consumer/Consumer.listTopics()
     * Ljava/util/Map;}. Spring Boot HealthIndicator / Kubernetes
     * readiness probe that 'lists all topics to confirm the cluster
     * is reachable'. With a misconfigured bootstrap.servers the call
     * hangs for the full 60 s; readiness probe times out; pod cycles
     * into CrashLoopBackoff; the actual error is invisible in the
     * pod's logs.
     */
    public Map<String, List<PartitionInfo>> readinessProbeViaInterface(
            Consumer<String, String> consumer) {
        return consumer.listTopics();
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /**
     * MUST FIRE — {@code consumer::listTopics} bound to a custom
     * functional interface whose erased SAM signature matches
     * {@code ()Ljava/util/Map;}. INVOKEDYNAMIC bsm-args contain a
     * REF_invokeVirtual handle pointing at
     * {@code KafkaConsumer.listTopics()Ljava/util/Map;}.
     */
    public TopicCatalogReader buildReader(KafkaConsumer<String, String> consumer) {
        return consumer::listTopics;
    }

    /**
     * MUST FIRE — same indy capture against the Consumer
     * interface-typed receiver. Handle is REF_invokeInterface; the
     * (owner, name, desc) tuple matches.
     */
    public TopicCatalogReader buildReaderInterface(Consumer<String, String> consumer) {
        return consumer::listTopics;
    }

    /**
     * MUST FIRE — {@code consumer::listTopics} bound to
     * {@link Supplier}{@code <Map<String, List<PartitionInfo>>>}.
     */
    public Supplier<Map<String, List<PartitionInfo>>> buildReaderSupplier(
            KafkaConsumer<String, String> consumer) {
        return consumer::listTopics;
    }

    /**
     * MUST FIRE — explicit lambda body that invokes
     * {@code consumer.listTopics()}. The synthetic lambda method's
     * bytecode contains a direct INVOKEVIRTUAL on the no-Duration
     * overload — the rule walks all methods on the class, including
     * synthetic lambda bodies, and fires there.
     */
    public ScheduledFuture<?> scheduleListTopicsRead(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer) {
        Callable<Map<String, List<PartitionInfo>>> task = () -> consumer.listTopics();
        return scheduler.schedule(task, 30, TimeUnit.SECONDS);
    }

    /**
     * MUST FIRE — Callable method-reference captured and submitted to
     * an {@link ExecutorService}.
     */
    public Future<Map<String, List<PartitionInfo>>> submitListTopicsRead(
            ExecutorService executor,
            KafkaConsumer<String, String> consumer) {
        Callable<Map<String, List<PartitionInfo>>> reader = consumer::listTopics;
        return executor.submit(reader);
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new BadConsumerListTopicsNoTimeout().autocompletionDropdown(c);
        }
    }
}
