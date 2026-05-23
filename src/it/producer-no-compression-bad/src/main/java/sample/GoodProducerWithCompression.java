package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Properties;

/**
 * Silent: producers where the same method that constructs the
 * KafkaProducer also mentions the string "compression.type".
 *
 * The rule's LDC check accepts the literal "compression.type" by
 * any path that puts it on the JVM constant pool in this method:
 *   - direct string literal in `props.put("compression.type", ...)`
 *   - `ProducerConfig.COMPRESSION_TYPE_CONFIG` (a compile-time
 *     constant — javac inlines it, so the bytecode is identical)
 *   - any other LDC of the same string (e.g., reading the key
 *     from a method-local constant declaration)
 */
public class GoodProducerWithCompression {

    /**
     * Shape A: string literal — props.put("compression.type", "zstd").
     * The LDC for the key string lands in this method's constant
     * pool, so the rule's LDC scan matches.
     *
     * zstd is the modern default for Kafka producers — better
     * compression ratio than snappy/lz4, the CPU cost is paid in
     * the background sender thread (not the application hot path).
     */
    public KafkaProducer<String, String> goodLiteralCompressionType() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put("client.id", "good-literal-compression-type");
        props.put("acks", "all");
        props.put("enable.idempotence", "true");
        props.put("compression.type", "zstd");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    /**
     * Shape B: ProducerConfig.COMPRESSION_TYPE_CONFIG.
     *
     * This is the strongly-typed form most teams use. The constant
     * is declared as `public static final String COMPRESSION_TYPE_CONFIG
     * = "compression.type"` — javac inlines compile-time String
     * constants per JLS §13.1, so the bytecode contains an LDC of
     * the literal "compression.type", identical to Shape A.
     *
     * The rule's "this method mentions compression.type" check matches
     * because the LDC scan looks at the JVM constant pool, not the
     * Java source form.
     */
    public KafkaProducer<String, String> goodProducerConfigConstant() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "good-producer-config-constant");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    /**
     * Shape C: HashMap config holder with literal compression.type —
     * same as Shape A but with a different config-map type, to make
     * sure the rule is config-holder-agnostic.
     */
    public KafkaProducer<String, String> goodHashMapWithCompression() {
        HashMap<String, Object> cfg = new HashMap<>();
        cfg.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        cfg.put("client.id", "good-hashmap-with-compression");
        cfg.put("acks", "all");
        cfg.put("enable.idempotence", "true");
        cfg.put("compression.type", "lz4");
        cfg.put("key.serializer", StringSerializer.class.getName());
        cfg.put("value.serializer", StringSerializer.class.getName());
        return new KafkaProducer<>(cfg);
    }
}
