package sample;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 * Control for CLIENT_ID_MISSING.
 *
 * <p>Each method constructs the same client kind as the matching Bad
 * method but explicitly sets {@code "client.id"} in the SAME method.
 * Three different ways to set it are exercised; they are bytecode-
 * equivalent because the {@code *_CONFIG} fields are compile-time
 * constants:
 * <ol>
 *   <li>Producer — {@code "client.id"} as a raw string literal.</li>
 *   <li>Consumer — {@code ConsumerConfig.CLIENT_ID_CONFIG}, which
 *       javac inlines to the literal {@code "client.id"} at the LDC
 *       site. Bytecode-identical to the raw-literal form.</li>
 *   <li>Streams — {@code StreamsConfig.CLIENT_ID_CONFIG}, same
 *       inlining. The rule cannot tell the three forms apart and
 *       does not need to.</li>
 * </ol>
 *
 * <p>The rule's check is: "is there ANY LDC of the constant
 * \"client.id\" anywhere in this method." Each method here passes
 * that check, so each is silent.
 */
public final class GoodClientIdSet {

    public KafkaProducer<String, String> buildProducerWithLiteralString() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put("client.id", "checkout-producer-v3"); // silent: literal "client.id" LDC present
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

    public KafkaConsumer<String, String> buildConsumerWithConfigConstant() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put("group.id", "orders-processor");
        // ConsumerConfig.CLIENT_ID_CONFIG is the String constant "client.id"; javac inlines.
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "orders-consumer-pod-7"); // silent
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(props);
    }

    public KafkaStreams buildStreamsWithStreamsConfigConstant() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put("application.id", "orders-streams-app");
        // StreamsConfig.CLIENT_ID_CONFIG is the String constant "client.id".
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "orders-streams-app-v3"); // silent
        Topology topology = new StreamsBuilder().build();
        return new KafkaStreams(topology, props);
    }

    /** Cross-reference for the per-method scope: same class also has a method that constructs WITHOUT client.id. */
    public KafkaProducer<String, String> buildProducerInDifferentMethodFromTheConfig() {
        // This method does NOT set client.id, so per the rule's per-method scope it WOULD fire on the construction
        // below... EXCEPT we silence it by referencing the literal "client.id" in a comment-free LDC below.
        // The reference is harmless at runtime — it just satisfies the rule's LDC check.
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "fallback-producer"); // silent: ProducerConfig.CLIENT_ID_CONFIG inlines to "client.id"
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }
}
