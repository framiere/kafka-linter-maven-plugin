package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * RULE: PRODUCER_RECORD_NO_KEY — must NOT fire on ANY method below.
 *
 * <p>Mirror of {@code BadProducerRecordNoKey} that uses the 3-arg
 * {@code ProducerRecord(topic, key, value)} constructor everywhere
 * the BAD class used the unsafe 2-arg {@code (topic, value)}. The
 * 3-arg constructor's descriptor is
 * {@code (Ljava/lang/String;Ljava/lang/Object;Ljava/lang/Object;)V}
 * — strictly different from the 2-arg unsafe descriptor
 * {@code (Ljava/lang/String;Ljava/lang/Object;)V} that the rule
 * predicate matches against — so neither the direct
 * {@code INVOKESPECIAL} sites nor the {@code ProducerRecord::new}
 * {@code INVOKEDYNAMIC} constructor-reference captures (which carry
 * the resolved 3-arg constructor descriptor in their bsm-args
 * {@code REF_newInvokeSpecial} Handle) match the predicate.
 *
 * <p>Every record below carries a domain-meaningful key (customerId,
 * accountId, userId, tenantId) so that (a) the default partitioner
 * hashes the key and per-key ordering is preserved across batches,
 * (b) compacted topics can compact correctly, and (c) downstream
 * stream-table joins find the right side of the join.
 */
public final class GoodProducerRecordNoKey {

    @FunctionalInterface
    interface KeyedRecordFactory<V> {
        ProducerRecord<String, V> create(String topic, String key, V value);
    }

    @FunctionalInterface
    interface TopicKeyValueFactory {
        ProducerRecord<String, String> create(String topic, String key, String value);
    }

    @FunctionalInterface
    interface TriFn<A, B, C, R> {
        R apply(A a, B b, C c);
    }

    // ===== Direct INVOKESPECIAL on the safe 3-arg constructor =====

    /**
     * Order event published WITH customerId as key: KStream join to
     * customers KTable enriches every record correctly because the
     * left side carries the join key.
     */
    public ProducerRecord<String, String> orderEventWithCustomerId(String customerId, String orderBody) {
        return new ProducerRecord<>("orders", customerId, orderBody);
    }

    /**
     * Payment event published WITH accountId as key: per-account
     * state transitions land on the same partition (key-hash
     * partitioner), downstream consumer processes them in-order, the
     * state machine sees a coherent sequence.
     */
    public ProducerRecord<String, String> paymentEventWithAccountId(String accountId, String eventBody) {
        return new ProducerRecord<>("payments-events", accountId, eventBody);
    }

    /**
     * User update event published WITH userId as key to a compacted
     * topic: log compaction retains the latest event per userId, the
     * topic is queryable by userId, the KTable source materializes
     * correctly.
     */
    public void sendUserUpdate(Producer<String, String> producer, String userId, String event) {
        producer.send(new ProducerRecord<>("user-updates", userId, event));
    }

    // ===== INVOKEDYNAMIC constructor-reference captures bound to
    //       3-arg SAMs — implMethod handle desc is
    //       (String, Object, Object)V — does NOT match rule predicate =====

    /**
     * {@code ProducerRecord::new} bound to a generic 3-arg
     * {@code TriFn<String, String, String,
     * ProducerRecord<String, String>>}. Indy bsm-args
     * {@code REF_newInvokeSpecial} Handle carries the 3-arg
     * constructor descriptor.
     */
    public TriFn<String, String, String, ProducerRecord<String, String>> buildTriFnFactory() {
        return ProducerRecord::new;
    }

    /**
     * {@code ProducerRecord::new} bound to a custom generic 3-arg
     * SAM whose erased implMethod descriptor matches the 3-arg
     * constructor.
     */
    public <V> KeyedRecordFactory<V> buildKeyedRecordFactory() {
        return ProducerRecord::new;
    }

    /**
     * {@code ProducerRecord::new} bound to a custom non-generic
     * 3-arg SAM. Indy implMethod handle descriptor is exactly
     * {@code (Ljava/lang/String;Ljava/lang/Object;Ljava/lang/Object;)V}.
     */
    public TopicKeyValueFactory buildTopicKeyValueFactory() {
        return ProducerRecord::new;
    }

    /**
     * Local 3-arg factory bound to {@code ProducerRecord::new},
     * mapped over an input list of (key, value) pairs. The indy
     * site carries the 3-arg constructor descriptor; the synthetic
     * lambda body goes through the factory handle and does NOT
     * contain a direct INVOKESPECIAL on the 2-arg constructor.
     */
    public List<ProducerRecord<String, String>> mapInputsThroughFactory(
            String topic, List<String> inputs, java.util.function.Function<String, String> keyOf) {
        TriFn<String, String, String, ProducerRecord<String, String>> factory = ProducerRecord::new;
        return inputs.stream()
                .map(v -> factory.apply(topic, keyOf.apply(v), v))
                .toList();
    }

    /**
     * Explicit lambda body that invokes the safe 3-arg
     * constructor. The synthetic lambda method's bytecode contains
     * an INVOKESPECIAL on {@code ProducerRecord.<init>(String,
     * Object, Object)V} — descriptor does NOT match the rule
     * predicate.
     */
    public Stream<ProducerRecord<String, String>> streamMapWithKey(
            String topic, List<String> inputs, java.util.function.Function<String, String> keyOf) {
        return inputs.stream().map(v -> new ProducerRecord<>(topic, keyOf.apply(v), v));
    }

    public static void main(String[] args) {
        try (KafkaProducer<String, String> p = new KafkaProducer<>(new Properties())) {
            p.send(new GoodProducerRecordNoKey().orderEventWithCustomerId("c1", "o1"));
        }
    }
}
