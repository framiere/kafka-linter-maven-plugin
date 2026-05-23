package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Silent: NON-transactional producer that explicitly disables
 * idempotence. The rule's two-key state machine requires BOTH
 * "transactional.id set" AND "enable.idempotence=false" — without
 * transactional.id, the enable.idempotence=false alone is not a
 * contradiction. It's a separate concern (idempotent producer
 * hygiene) that other rules can flag.
 *
 * Real-world incidence: legacy at-most-once producers that
 * explicitly disable idempotence to preserve pre-3.0 semantics
 * (where the default was idempotence=false). These are
 * fire-and-forget pipelines (metrics, logs, fan-out telemetry)
 * where duplicate-write-tolerance is fine.
 *
 * This is the rule's negative branch for condition (a): the
 * transactional.id state stays null, so the violation report
 * doesn't fire.
 */
public class GoodTxnIdAbsentIdempotenceFalse {

    @SuppressWarnings({"rawtypes", "unchecked"})
    private final KafkaProducer producer;

    public GoodTxnIdAbsentIdempotenceFalse() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put("client.id", "good-txn-id-absent-idempotence-false");
        props.put("acks", "1");
        props.put("compression.type", "zstd");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("enable.idempotence", "false");
        this.producer = new KafkaProducer<>(props);
    }
}
