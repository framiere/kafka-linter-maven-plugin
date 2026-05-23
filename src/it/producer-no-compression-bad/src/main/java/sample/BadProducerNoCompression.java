package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Properties;

/**
 * RULE: PRODUCER_NO_COMPRESSION — per-method bytecode scan.
 *
 * The rule fires when a method contains BOTH:
 *   (a) the bytecode `NEW org/apache/kafka/clients/producer/KafkaProducer`
 *       instruction (the JVM-level construction of a KafkaProducer
 *       instance — what `new KafkaProducer<>(props)` compiles to);
 *   (b) AND the method does NOT contain an LDC instruction whose
 *       constant is the string "compression.type".
 *
 * Both conditions are scoped to the SAME method — the rule's heuristic
 * is intentionally dumb and predictable. A method that constructs the
 * producer but builds its Properties via a helper method will NOT fire
 * (documented false-negative — see GoodProducerCompressionInHelper for
 * the cross-method shape that escapes the rule).
 *
 * The LDC check accepts BOTH source forms:
 *   - The string literal "compression.type" (LDC of the literal)
 *   - ProducerConfig.COMPRESSION_TYPE_CONFIG (a compile-time String
 *     constant that javac inlines to the same literal — so the
 *     bytecode is identical to the literal form).
 *
 * This class contains THREE methods that each fire the rule, exercising
 * the three orthogonal "no compression.type literal in the method"
 * shapes. The rule produces one violation PER METHOD that satisfies
 * both conditions (not per `new KafkaProducer` instruction — only the
 * first NEW per method is matched).
 */
public class BadProducerNoCompression {

    /**
     * Shape 1: minimal — Properties built and producer constructed
     * in the same method, no compression knob touched at all.
     *
     * Default `compression.type` is `none` — record batches travel
     * uncompressed to the broker and replicate uncompressed across
     * the ISR. For typical JSON / Avro payloads this is a 3-5×
     * waste on broker storage, cross-AZ network, and tiered-storage
     * egress cost.
     */
    public KafkaProducer<String, String> buildProducerNoCompressionAtAll() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put("client.id", "bad-producer-no-compression");
        props.put("acks", "all");
        props.put("enable.idempotence", "true");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        // FIRES: NEW KafkaProducer present, no LDC of "compression.type" anywhere
        // in this method body.
        return new KafkaProducer<>(props);
    }

    /**
     * Shape 2: HashMap config holder — same gap, different config-map
     * type. The rule checks the producer construction site, not the
     * config-holder type, so HashMap is just as exposed.
     *
     * The .send() inside the same method exercises a producer-touch
     * shape (some teams hoist the producer to a field but then build
     * a throwaway one inside a request handler — that's the
     * PRODUCER_PER_RECORD_ALLOCATION shape, but this rule fires
     * independently of that one).
     */
    public void buildAndSendNoCompression() {
        HashMap<String, Object> cfg = new HashMap<>();
        cfg.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        cfg.put("client.id", "bad-producer-no-compression-send");
        cfg.put("acks", "all");
        cfg.put("enable.idempotence", "true");
        cfg.put("key.serializer", StringSerializer.class.getName());
        cfg.put("value.serializer", StringSerializer.class.getName());
        // FIRES: NEW KafkaProducer present, no "compression.type" LDC.
        KafkaProducer<String, String> p = new KafkaProducer<>(cfg);
        p.send(new ProducerRecord<>("orders", "k", "v"));
        p.close();
    }

    /**
     * Shape 3: "set a different compression-related key but not THE key".
     *
     * The rule's match is exact on the string "compression.type". A
     * related-but-different key like "compression.codec" (the old
     * 0.x-era Kafka key, removed long ago) doesn't satisfy the LDC
     * check. The producer still defaults to `none`.
     *
     * This shape catches the "I configured something with compression
     * in the name, so it must be set" cargo-cult — only the canonical
     * 'compression.type' key controls the producer's compression
     * codec.
     */
    public KafkaProducer<String, String> buildWithWrongCompressionKey() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put("client.id", "bad-producer-wrong-compression-key");
        props.put("acks", "all");
        props.put("enable.idempotence", "true");
        // "compression.codec" is the long-removed 0.x-era key — not
        // "compression.type" — so the LDC scan does not match.
        props.put("compression.codec", "snappy");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        // FIRES: NEW KafkaProducer present, no LDC of "compression.type"
        // (only LDC of "compression.codec" which is a different string).
        return new KafkaProducer<>(props);
    }
}
