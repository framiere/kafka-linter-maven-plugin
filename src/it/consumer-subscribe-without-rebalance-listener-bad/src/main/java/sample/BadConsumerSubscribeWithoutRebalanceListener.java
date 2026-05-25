package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

/**
 * RULE: CONSUMER_SUBSCRIBE_WITHOUT_REBALANCE_LISTENER — must fire on
 * EVERY method below.
 *
 * <p>Each method exercises one of the bytecode shapes the rule is
 * required to catch:
 *
 * <ol>
 *   <li>direct {@code INVOKEVIRTUAL} on
 *       {@code KafkaConsumer.subscribe(Collection)V} and
 *       {@code KafkaConsumer.subscribe(Pattern)V};</li>
 *   <li>direct {@code INVOKEINTERFACE} on
 *       {@code Consumer.subscribe(Collection)V};</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code consumer::subscribe} bound to a custom collection-typed
 *       SAM ({@code TopicListSubscriber}) or pattern-typed SAM
 *       ({@code PatternSubscriber}) — bound to both the KafkaConsumer
 *       typed receiver (REF_invokeVirtual) and the Consumer
 *       interface-typed receiver (REF_invokeInterface);</li>
 *   <li>{@code INVOKEDYNAMIC} capture against
 *       {@link java.util.function.Consumer Consumer&lt;Collection&lt;String&gt;&gt;}
 *       where the SAM `void accept(Object)` erases the parameter but
 *       the implMethod handle in the bsm-args still carries the
 *       precise descriptor `(Ljava/util/Collection;)V`;</li>
 *   <li>synthetic lambda body containing direct INVOKEVIRTUAL —
 *       {@code executor.submit(() -> consumer.subscribe(topics))} — the
 *       rule walks all methods on the class including synthetic lambda
 *       bodies and fires there.</li>
 * </ol>
 *
 * <h2>Why no-listener subscribe is a correctness hazard</h2>
 *
 * <p>The group protocol guarantees at-most-one consumer per partition
 * by triggering a rebalance whenever group membership changes (new
 * instance joins, instance leaves, partition added to a topic, regex
 * matches a new topic). During a rebalance partitions are revoked
 * from one consumer instance and assigned to another. The
 * ConsumerRebalanceListener callback is the only hook the
 * application has between the 'partitions about to be revoked' signal
 * and the 'partitions actually revoked' event — meaning it is the
 * only place to flush in-memory state, commit final offsets, release
 * per-partition resources, or hand off in-flight work.
 */
public final class BadConsumerSubscribeWithoutRebalanceListener {

    @FunctionalInterface
    interface TopicListSubscriber {
        void subscribe(Collection<String> topics);
    }

    @FunctionalInterface
    interface PatternSubscriber {
        void subscribe(Pattern pattern);
    }

    // ===== Direct INVOKEVIRTUAL =====

    /**
     * MUST FIRE — direct INVOKEVIRTUAL on
     * {@code KafkaConsumer.subscribe(Collection)V}. Consumer loop
     * init: in-flight records buffered for batched DB writes are
     * silently dropped on the next rebalance; the new owner replays
     * from the offset before the dropped batch.
     */
    public void consumerLoopInit(KafkaConsumer<String, String> consumer, List<String> topics) {
        consumer.subscribe(topics);
    }

    /**
     * MUST FIRE — direct INVOKEVIRTUAL on
     * {@code KafkaConsumer.subscribe(Pattern)V}. Pattern variant
     * resubscribes automatically as new topics matching the regex are
     * created and triggers a rebalance every time; without a
     * listener every such auto-discovery event silently drops
     * in-flight work on every existing partition.
     */
    public void patternBasedDiscovery(KafkaConsumer<String, String> consumer, Pattern pattern) {
        consumer.subscribe(pattern);
    }

    // ===== Direct INVOKEINTERFACE =====

    /**
     * MUST FIRE — interface-typed receiver. INVOKEINTERFACE on
     * {@code Consumer.subscribe(Collection)V}. Helper hiding the
     * concrete consumer behind the {@link Consumer} interface — same
     * hazard.
     */
    public void interfaceSubscribe(Consumer<String, String> consumer, List<String> topics) {
        consumer.subscribe(topics);
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /**
     * MUST FIRE — {@code consumer::subscribe} bound to a custom
     * collection-typed SAM. INVOKEDYNAMIC bsm-args contain a
     * REF_invokeVirtual handle pointing at
     * {@code KafkaConsumer.subscribe(Collection)V}.
     */
    public TopicListSubscriber buildTopicListSubscriber(KafkaConsumer<String, String> consumer) {
        return consumer::subscribe;
    }

    /**
     * MUST FIRE — same indy capture against the Consumer
     * interface-typed receiver. Handle is REF_invokeInterface; the
     * (owner, name, desc) tuple matches.
     */
    public TopicListSubscriber buildTopicListSubscriberInterface(Consumer<String, String> consumer) {
        return consumer::subscribe;
    }

    /**
     * MUST FIRE — {@code consumer::subscribe} bound to a custom
     * pattern-typed SAM. Indy implMethod handle descriptor is
     * {@code (Ljava/util/regex/Pattern;)V}.
     */
    public PatternSubscriber buildPatternSubscriber(KafkaConsumer<String, String> consumer) {
        return consumer::subscribe;
    }

    /**
     * MUST FIRE — {@code consumer::subscribe} bound to
     * {@link java.util.function.Consumer Consumer&lt;Collection&lt;String&gt;&gt;}.
     * The SAM {@code void accept(Object)} erases the parameter type
     * but the indy implMethod handle in bsm-args still carries the
     * precise descriptor {@code (Ljava/util/Collection;)V}.
     */
    public java.util.function.Consumer<Collection<String>> buildJavaUtilFunctionConsumer(
            KafkaConsumer<String, String> consumer) {
        return consumer::subscribe;
    }

    /**
     * MUST FIRE — explicit lambda body that invokes
     * {@code consumer.subscribe(topics)}. The synthetic lambda
     * method's bytecode contains a direct INVOKEVIRTUAL on the
     * no-listener Collection overload — the rule walks all methods
     * including synthetic lambda bodies and fires there.
     */
    public Future<?> scheduleSubscribe(
            ExecutorService executor,
            KafkaConsumer<String, String> consumer,
            List<String> topics) {
        return executor.submit(() -> consumer.subscribe(topics));
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new BadConsumerSubscribeWithoutRebalanceListener().consumerLoopInit(c, List.of());
        }
    }
}
