package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Cross-method shape: transactional.id set in one method,
 * enable.idempotence=false set in another method of the SAME class.
 *
 * The rule's two-state class-scoped scan keeps track of the
 * first-seen "transactional.id put" and first-seen
 * "enable.idempotence=false put" across the entire class. When both
 * are found by the end of the method-walk loop, ONE violation is
 * reported — and the message includes the source method of the
 * transactional.id put (the "context" side) and the file/line of the
 * idempotence-false put (the "trigger" side). This is the cleanest
 * presentation because the idempotence-false call is what an
 * operator can actually delete to fix the bug.
 *
 * Real-world incidence: factory pattern where a base "common
 * properties" method sets the kafka-clients defaults, and a
 * specialization method later overrides individual knobs. A
 * pre-3.0-era boilerplate that explicitly sets enable.idempotence=
 * false can survive a transactional refactor — the txn.id is added
 * to the specialization, the idempotence=false is forgotten in the
 * common path.
 */
public class BadTxnIdWithoutIdempotenceCrossMethod {

    @SuppressWarnings({"rawtypes", "unchecked"})
    private final KafkaProducer producer;

    public BadTxnIdWithoutIdempotenceCrossMethod() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put("client.id", "bad-txn-id-without-idempotence-cross-method");
        props.put("acks", "all");
        props.put("compression.type", "zstd");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        applyTxnConfig(props);
        applyLegacyOverrides(props);
        this.producer = new KafkaProducer<>(props);
    }

    /** Configures this producer as transactional — the "txn.id put". */
    private void applyTxnConfig(Properties props) {
        props.put("transactional.id", "orders-producer-tx-cross");
    }

    /**
     * Legacy override path that explicitly disables idempotence —
     * survives across a transactional refactor because it lives in
     * a separate helper method. The rule's cross-method aggregation
     * catches the contradiction even though no single method
     * contains both put-calls.
     */
    private void applyLegacyOverrides(Properties props) {
        props.put("enable.idempotence", "false");
    }
}
