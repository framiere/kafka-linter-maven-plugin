package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * RULE: PRODUCER_RECORD_NO_KEY — must fire EXACTLY 8 times across
 * this class (one per method below).
 *
 * <p>Each method exercises one of the bytecode shapes the rule is
 * required to catch:
 *
 * <ol>
 *   <li>direct {@code INVOKESPECIAL} on
 *       {@code ProducerRecord.<init>(Ljava/lang/String;Ljava/lang/Object;)V};</li>
 *   <li>{@code INVOKEDYNAMIC} constructor-reference capture
 *       {@code ProducerRecord::new} bound to a
 *       {@link BiFunction BiFunction&lt;String, V, ProducerRecord&lt;
 *       String, V&gt;&gt;};</li>
 *   <li>{@code INVOKEDYNAMIC} capture bound to a custom 2-arg SAM
 *       ({@code RecordFactory}, {@code TopicValueFactory});</li>
 *   <li>synthetic lambda body containing direct {@code INVOKESPECIAL}
 *       — {@code .map(v -> new ProducerRecord<>(topic, v))} — the
 *       rule walks all methods on the class including synthetic
 *       lambda bodies and fires there.</li>
 * </ol>
 *
 * <h2>Why a null key is a correctness hazard</h2>
 *
 * <p>The key controls three independent producer behaviors: (a) the
 * default partitioner hashes the key, so records with the same key
 * land on the same partition and preserve per-key ordering; null key
 * triggers the sticky partitioner (KIP-480, Kafka 2.4+) which batches
 * records to a partition for the lifetime of a batch then picks a new
 * partition at random — per-key ordering is impossible; (b) log
 * compaction retains only the latest record per key; a null key on a
 * compacted topic is ill-defined (tombstone of nothing); (c) every
 * Streams operation that groups, partitions, or joins by key requires
 * a non-null key — null-keyed records silently break joins.
 */
public final class BadProducerRecordNoKey {

    @FunctionalInterface
    interface RecordFactory<V> {
        ProducerRecord<String, V> create(String topic, V value);
    }

    @FunctionalInterface
    interface TopicValueFactory {
        ProducerRecord<String, String> create(String topic, String value);
    }

    // ===== Direct INVOKESPECIAL =====

    /**
     * MUST FIRE — direct INVOKESPECIAL on the 2-arg constructor.
     * Order event published with no customerId as key: KStream join
     * to customers KTable silently returns null on every record.
     */
    public ProducerRecord<String, String> orderEventNoCustomerId(String orderBody) {
        return new ProducerRecord<>("orders", orderBody);
    }

    /**
     * MUST FIRE — direct INVOKESPECIAL. Payment event published with
     * no accountId as key: per-account state transitions land on
     * sticky-partitioner-chosen partitions; downstream consumer
     * processes partitions in parallel; per-account ordering breaks;
     * non-idempotent state machines produce visible inconsistency
     * (charge before authorization, refund before charge).
     */
    public ProducerRecord<String, String> paymentEventNoAccountId(String eventBody) {
        return new ProducerRecord<>("payments-events", eventBody);
    }

    /**
     * MUST FIRE — direct INVOKESPECIAL in a void send method. User
     * update event published with no userId as key to a topic
     * intended to be compacted: sticky partitioner batches all events
     * to one partition; compaction sees only null keys; either
     * retains all duplicates (no compaction benefit) or deletes
     * everything.
     */
    public void sendUserUpdate(Producer<String, String> producer, String event) {
        producer.send(new ProducerRecord<>("user-updates", event));
    }

    // ===== INVOKEDYNAMIC constructor-reference captures =====

    /**
     * MUST FIRE — {@code ProducerRecord::new} bound to
     * {@link BiFunction}. INVOKEDYNAMIC bsm-args contain a
     * REF_newInvokeSpecial Handle pointing at
     * {@code ProducerRecord.<init>(Ljava/lang/String;Ljava/lang/Object;)V}.
     */
    public BiFunction<String, String, ProducerRecord<String, String>> buildBiFunctionFactory() {
        return ProducerRecord::new;
    }

    /**
     * MUST FIRE — {@code ProducerRecord::new} bound to a custom
     * generic 2-arg SAM whose erased implMethod descriptor matches.
     */
    public <V> RecordFactory<V> buildRecordFactory() {
        return ProducerRecord::new;
    }

    /**
     * MUST FIRE — {@code ProducerRecord::new} bound to a custom
     * non-generic 2-arg SAM. Indy implMethod handle descriptor is
     * exactly {@code (Ljava/lang/String;Ljava/lang/Object;)V}.
     */
    public TopicValueFactory buildTopicValueFactory() {
        return ProducerRecord::new;
    }

    /**
     * MUST FIRE — local BiFunction variable bound to
     * {@code ProducerRecord::new}, then mapped over an input list.
     * The indy site is in this method's bytecode; the synthetic
     * lambda body that calls {@code factory.apply(topic, v)} does NOT
     * contain a direct INVOKESPECIAL on the constructor (it goes
     * through the BiFunction handle), so the only fire is at the
     * indy site itself.
     */
    public List<ProducerRecord<String, String>> mapInputsThroughFactory(
            String topic, List<String> inputs) {
        BiFunction<String, String, ProducerRecord<String, String>> factory = ProducerRecord::new;
        return inputs.stream()
                .map(v -> factory.apply(topic, v))
                .toList();
    }

    /**
     * MUST FIRE — explicit lambda body that invokes
     * {@code new ProducerRecord<>(topic, v)}. The synthetic lambda
     * method's bytecode contains a direct INVOKESPECIAL on the
     * 2-arg constructor — the rule walks all methods on the class
     * including synthetic lambda bodies and fires there.
     */
    public Stream<ProducerRecord<String, String>> streamMapNoKey(
            String topic, List<String> inputs) {
        return inputs.stream().map(v -> new ProducerRecord<>(topic, v));
    }

    public static void main(String[] args) {
        try (KafkaProducer<String, String> p = new KafkaProducer<>(new Properties())) {
            p.send(new BadProducerRecordNoKey().orderEventNoCustomerId("o1"));
        }
    }
}
