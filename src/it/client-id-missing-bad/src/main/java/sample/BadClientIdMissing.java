package sample;

import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

/**
 * RULE: CLIENT_ID_MISSING.
 *
 * <p>Fires when a method constructs a Kafka client (one of
 * {@code KafkaProducer}, {@code KafkaConsumer}, {@code KafkaStreams})
 * but the method body NEVER mentions the literal string
 * {@code "client.id"} anywhere.
 *
 * <p>Why "client.id" matters operationally:
 * <ol>
 *   <li>It is the string that brokers, JMX, the {@code __consumer_offsets}
 *       topic, and downstream observability tools (Grafana, Datadog,
 *       Splunk) use to identify <em>which</em> client did what. With it
 *       set, you see {@code "checkout-producer-pod-7@app=checkout"}
 *       in dashboards and broker logs. Without it, you see the
 *       kafka-clients auto-generated default —
 *       {@code "producer-1"} / {@code "producer-2"} / ... — which is
 *       not even stable: a client that restarts gets {@code "producer-1"}
 *       again, regardless of which instance it actually is.</li>
 *   <li>Broker-side quota enforcement is keyed on {@code (user, client.id)}
 *       pairs. Without an explicit client.id, EVERY producer of every
 *       application that forgot to set it lumps into the same quota
 *       bucket. One misbehaving app starves another.</li>
 *   <li>{@code request_metrics-{client_id}-{type}} JMX bean names use
 *       client.id directly. With the default, you cannot tell two
 *       producers apart in metrics — their Mbeans collide.</li>
 *   <li>Incident response: when on-call sees a flood of errors on a
 *       broker, the first useful question is "who is the noisy
 *       client?" The answer is {@code client.id} in the broker logs.
 *       The default value answers "some producer somewhere" and
 *       blocks the investigation.</li>
 * </ol>
 *
 * <p>Why the rule is per-METHOD (not class-scope, not project-scope):
 * <ol>
 *   <li>Construction usually happens in ONE place: a factory method,
 *       a {@code @Bean} method, a setup() block. That single method
 *       is also the natural place to populate the Properties object.
 *       If client.id is going to be set, it'll be set right there.</li>
 *   <li>Tracking key population across method boundaries would
 *       require data-flow analysis (the Properties object could be
 *       passed in, mutated elsewhere, etc.). The trade-off the rule
 *       takes: keep the heuristic DUMB and PREDICTABLE — false
 *       negatives (constructor-arg Properties built in a caller) are
 *       acceptable; false positives are not.</li>
 *   <li>The rule looks for the LITERAL string {@code "client.id"}
 *       anywhere in the method, in any LDC instruction. Because the
 *       {@code ProducerConfig.CLIENT_ID_CONFIG} /
 *       {@code ConsumerConfig.CLIENT_ID_CONFIG} /
 *       {@code StreamsConfig.CLIENT_ID_CONFIG} fields are compile-time
 *       constants whose value IS {@code "client.id"}, javac inlines
 *       them at the use site — so both forms work identically.</li>
 * </ol>
 *
 * <p>What the rule catches (per-method state):
 * <ol>
 *   <li>Walks every method body once. Records the first occurrence
 *       of a {@code NEW} opcode whose type is one of
 *       {@code org/apache/kafka/clients/producer/KafkaProducer},
 *       {@code org/apache/kafka/clients/consumer/KafkaConsumer}, or
 *       {@code org/apache/kafka/streams/KafkaStreams}.</li>
 *   <li>If no such site is found, skip the method.</li>
 *   <li>If found, scan the SAME method body for any LDC whose
 *       constant equals the string {@code "client.id"}. If present,
 *       silent — fix is in place.</li>
 *   <li>Otherwise, emit a violation at the construction site, with
 *       a label of "KafkaProducer" / "KafkaConsumer" / "KafkaStreams"
 *       chosen by the construction type.</li>
 * </ol>
 *
 * <p>This Bad class triggers THREE fires — one per client kind —
 * each in its own method that omits {@code "client.id"} entirely.
 */
public final class BadClientIdMissing {

    /** Anti-pattern: KafkaProducer constructed without client.id. */
    public KafkaProducer<String, String> buildProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // client.id not set — broker logs will say "producer-1"
        return new KafkaProducer<>(props); // reported
    }

    /** Anti-pattern: KafkaConsumer constructed without client.id. */
    public KafkaConsumer<String, String> buildConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put("group.id", "orders-processor");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // client.id not set — JMX metrics will collide with every other consumer-1 in the fleet
        return new KafkaConsumer<>(props); // reported
    }

    /** Anti-pattern: KafkaStreams constructed without client.id (application.id is set but that is a different key). */
    public KafkaStreams buildStreams() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put("application.id", "orders-streams-app");
        // application.id != client.id — Streams will derive a default client.id of the form
        // "<application.id>-<random-uuid>-StreamThread-N-consumer" which is unique enough to NOT
        // collide at the broker, but useless for correlating "which deploy of which app" in metrics.
        Topology topology = new StreamsBuilder().build();
        return new KafkaStreams(topology, props); // reported
    }
}
