package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * RULE: CONSUMER_PARTITIONS_FOR_NO_TIMEOUT — must fire on EVERY
 * method below.
 *
 * <p>Each method exercises one of the bytecode shapes the rule is
 * required to catch:
 *
 * <ol>
 *   <li>direct {@code INVOKEVIRTUAL} on
 *       {@code KafkaConsumer.partitionsFor(String)Ljava/util/List;};</li>
 *   <li>direct {@code INVOKEINTERFACE} on
 *       {@code Consumer.partitionsFor(String)Ljava/util/List;};</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code consumer::partitionsFor} bound to a custom SAM
 *       ({@code PartitionListReader}) or to {@link Function};</li>
 *   <li>{@code INVOKEDYNAMIC} capture against the {@link Consumer}
 *       interface-typed receiver (REF_invokeInterface) vs. against
 *       the {@link KafkaConsumer} typed receiver (REF_invokeVirtual)
 *       — both are caught;</li>
 *   <li>{@link Callable}-wrapped dispatch via
 *       {@link ScheduledExecutorService} and {@link ExecutorService}.</li>
 * </ol>
 *
 * <h2>Why the bounded {@code partitionsFor(String, Duration)}
 * overload exists</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer#partitionsFor(String)}
 * is documented as equivalent to {@code partitionsFor(topic,
 * Duration.ofMillis(defaultApiTimeoutMs))} where
 * {@code default.api.timeout.ms} defaults to 60 s. When the
 * topic-metadata is missing or stale, the call triggers a
 * metadata-fetch request and parks the caller for the full 60 s
 * budget under bootstrap unreachability (every bootstrap broker
 * DNS-broken or wrong), controller slowness, or topic-missing
 * (UNKNOWN_TOPIC_OR_PARTITION returned for a typoed topic, retried
 * until the budget expires). The hazard multiplies per topic — a
 * 20-topic discovery loop hangs for 20 × 60 s = 20 minutes on an
 * unreachable cluster before the first TimeoutException.
 */
public final class BadConsumerPartitionsForNoTimeout {

    @FunctionalInterface
    interface PartitionListReader {
        List<PartitionInfo> read(String topic);
    }

    // ===== Direct INVOKEVIRTUAL on KafkaConsumer.partitionsFor(String) =====

    /**
     * MUST FIRE — direct INVOKEVIRTUAL on
     * {@code KafkaConsumer.partitionsFor(String)Ljava/util/List;}.
     * Startup health probe: a Kubernetes readiness probe calls
     * {@code consumer.partitionsFor("input-topic")} to confirm the
     * consumer can reach the cluster. With a mistyped
     * bootstrap.servers the call hangs for 60 s, the readiness probe
     * times out, the pod cycles into CrashLoopBackoff, and the actual
     * error is invisible in the pod's logs.
     */
    public List<PartitionInfo> startupHealthProbe(
            KafkaConsumer<String, String> consumer,
            String topic) {
        return consumer.partitionsFor(topic);
    }

    /**
     * MUST FIRE — direct INVOKEVIRTUAL. Topic-discovery loop: a
     * {@code for (String topic : configuredTopics)
     * consumer.partitionsFor(topic);} loop discovers partition counts
     * on startup. With 20 configured topics on an unreachable cluster
     * the loop runs for 20 × 60 s = 20 minutes before the first
     * TimeoutException surfaces.
     */
    public List<PartitionInfo> topicDiscoveryProbe(
            KafkaConsumer<String, String> consumer,
            String topic) {
        return consumer.partitionsFor(topic);
    }

    // ===== Direct INVOKEINTERFACE on Consumer.partitionsFor(String) =====

    /**
     * MUST FIRE — interface-typed receiver. INVOKEINTERFACE on
     * {@code org/apache/kafka/clients/consumer/Consumer.partitionsFor(
     * String)Ljava/util/List;}. Admin REST endpoint that validates a
     * topic exists before producing: under a typo the call blocks for
     * 60 s while the broker repeatedly returns
     * UNKNOWN_TOPIC_OR_PARTITION; the request handler is pinned for a
     * full minute on a one-character typo.
     */
    public List<PartitionInfo> topicExistenceCheckViaInterface(
            Consumer<String, String> consumer,
            String topic) {
        return consumer.partitionsFor(topic);
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /**
     * MUST FIRE — {@code consumer::partitionsFor} bound to a custom
     * functional interface whose erased SAM signature matches
     * {@code (Ljava/lang/String;)Ljava/util/List;}. INVOKEDYNAMIC
     * bsm-args contain a REF_invokeVirtual handle pointing at
     * {@code KafkaConsumer.partitionsFor(String)Ljava/util/List;}.
     */
    public PartitionListReader buildReader(KafkaConsumer<String, String> consumer) {
        return consumer::partitionsFor;
    }

    /**
     * MUST FIRE — same indy capture against the Consumer
     * interface-typed receiver. Handle is REF_invokeInterface; the
     * (owner, name, desc) tuple matches.
     */
    public PartitionListReader buildReaderInterface(Consumer<String, String> consumer) {
        return consumer::partitionsFor;
    }

    /**
     * MUST FIRE — {@code consumer::partitionsFor} bound to
     * {@link Function}{@code <String, List<PartitionInfo>>}.
     */
    public Function<String, List<PartitionInfo>> buildReaderFunction(
            KafkaConsumer<String, String> consumer) {
        return consumer::partitionsFor;
    }

    /**
     * MUST FIRE — explicit lambda body that invokes
     * {@code consumer.partitionsFor(...)}. The synthetic lambda
     * method's bytecode contains a direct INVOKEVIRTUAL on the
     * no-Duration overload — the rule walks all methods on the class,
     * including synthetic lambda bodies, and fires there.
     */
    public ScheduledFuture<?> schedulePartitionsForRead(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer,
            String topic) {
        Callable<List<PartitionInfo>> task = () -> consumer.partitionsFor(topic);
        return scheduler.schedule(task, 30, TimeUnit.SECONDS);
    }

    /**
     * MUST FIRE — Function method-reference captured and submitted to
     * an {@link ExecutorService}.
     */
    public Future<List<PartitionInfo>> submitPartitionsForRead(
            ExecutorService executor,
            KafkaConsumer<String, String> consumer,
            String topic) {
        Function<String, List<PartitionInfo>> reader = consumer::partitionsFor;
        return executor.submit(() -> reader.apply(topic));
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new BadConsumerPartitionsForNoTimeout().startupHealthProbe(c, "t");
        }
    }
}
