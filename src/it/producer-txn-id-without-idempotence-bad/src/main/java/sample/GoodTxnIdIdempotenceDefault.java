package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Silent: transactional.id is set, enable.idempotence is NOT
 * explicitly set anywhere — falls through to the kafka-clients
 * 3.0+ default (true).
 *
 * The rule does NOT fire on absence. Modern kafka-clients default
 * enable.idempotence to true precisely because the transactional
 * producer needs it, so the absence case is the broker-accepted
 * happy path. Firing on absence would generate noise on every
 * transactional producer that hasn't bothered to set the now-default
 * value explicitly — the rule's contract is explicitly contradictory
 * configuration, not redundant best-practice annotation.
 *
 * This is the rule's negative branch for condition (b): the
 * enable.idempotence-false state stays null, so the violation
 * report doesn't fire.
 */
public class GoodTxnIdIdempotenceDefault {

    @SuppressWarnings({"rawtypes", "unchecked"})
    private final KafkaProducer producer;

    public GoodTxnIdIdempotenceDefault() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put("client.id", "good-txn-id-idempotence-default");
        props.put("acks", "all");
        props.put("compression.type", "zstd");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("transactional.id", "orders-producer-tx-default");
        // enable.idempotence is NOT set — kafka-clients 3.0+ default
        // is true, which is exactly what a transactional producer
        // requires. The rule abstains because there's no explicit
        // "false" to contradict the transactional.id setting.
        this.producer = new KafkaProducer<>(props);
    }
}
