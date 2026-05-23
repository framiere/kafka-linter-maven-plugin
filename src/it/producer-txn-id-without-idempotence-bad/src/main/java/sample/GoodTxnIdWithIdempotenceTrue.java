package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Silent: transactional.id is set AND enable.idempotence is
 * EXPLICITLY set to "true". The rule only fires on the "false"
 * value of enable.idempotence — the rule's contract is
 * contradictory configuration, not absence-of-best-practice.
 *
 * This is the canonical transactional producer shape — both keys
 * present, both consistent, the broker accepts the configuration
 * and exactly-once semantics work end-to-end.
 */
public class GoodTxnIdWithIdempotenceTrue {

    @SuppressWarnings({"rawtypes", "unchecked"})
    private final KafkaProducer producer;

    public GoodTxnIdWithIdempotenceTrue() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put("client.id", "good-txn-id-with-idempotence-true");
        props.put("acks", "all");
        props.put("compression.type", "zstd");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("transactional.id", "orders-producer-tx-good");
        props.put("enable.idempotence", "true");
        this.producer = new KafkaProducer<>(props);
    }
}
