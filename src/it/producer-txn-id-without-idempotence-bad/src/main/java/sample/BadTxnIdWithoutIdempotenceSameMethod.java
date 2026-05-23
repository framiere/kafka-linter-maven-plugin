package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * RULE: PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE — class-scoped bytecode
 * rule.
 *
 * The rule fires once per class when BOTH conditions hold ANYWHERE
 * in the class body (across all methods):
 *   (a) some `<configHolder>.put("transactional.id", <ldc-string>)`
 *       call exists — i.e., the producer is configured as a
 *       transactional producer; AND
 *   (b) some `<configHolder>.put("enable.idempotence", "false")`
 *       call exists — i.e., idempotence is EXPLICITLY disabled.
 *
 * Both are checked at the bytecode level by scanning every
 * INVOKEVIRTUAL on a recognized config holder (Properties / Map /
 * HashMap) and matching put/setProperty calls where the key is the
 * LDC-string "transactional.id" or "enable.idempotence" with value
 * "false" (string compare against the LDC value's toString()).
 *
 * The contradiction matters because a transactional producer
 * REQUIRES idempotence — exactly-once semantics depend on the
 * idempotent producer's sequence-number-based dedup, which the
 * transaction coordinator builds on. Modern kafka-clients (3.0+)
 * default enable.idempotence to true and the broker will reject
 * the producer at init time with:
 *   ConfigException: Cannot set a transactional.id without also
 *   enabling idempotence
 * but this fails at RUNTIME — the application has already started,
 * passed health checks, and only discovers the misconfiguration
 * when it tries to initialize the producer. Catching it at lint
 * time saves a deploy-revert cycle.
 *
 * The rule only fires on enable.idempotence=FALSE, not on absence.
 * Modern defaults make idempotence true by default, so the "txn.id
 * set + idempotence absent" combination is fine — the broker will
 * accept it. The rule's contract is "explicitly contradictory
 * configuration", not "missing best-practice annotation".
 *
 * This file exercises the SAME-METHOD shape: both keys are set in
 * the constructor.
 */
public class BadTxnIdWithoutIdempotenceSameMethod {

    @SuppressWarnings({"rawtypes", "unchecked"})
    private final KafkaProducer producer;

    public BadTxnIdWithoutIdempotenceSameMethod() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put("client.id", "bad-txn-id-without-idempotence-same-method");
        props.put("acks", "all");
        props.put("compression.type", "zstd");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        // Both contradictory keys in the same method — the rule fires
        // once per class regardless of how many such put-calls there
        // are. The reported method is the idempotence-false call site.
        props.put("transactional.id", "orders-producer-tx");
        props.put("enable.idempotence", "false");
        this.producer = new KafkaProducer<>(props);
    }
}
