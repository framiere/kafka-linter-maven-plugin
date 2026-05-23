package sample;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * RULE: PROPERTIES_MUTATED_AFTER_CTOR.
 *
 * <p>Fires when a method calls {@code put(...)} or {@code setProperty(...)}
 * on a local {@code Properties} / {@code Map} / {@code HashMap} slot
 * AFTER that slot was passed to a {@code new KafkaProducer(...)} or
 * {@code new KafkaConsumer(...)} constructor in the same method.
 *
 * <p>Why this mutation is silently wrong — what the constructor does
 * with the config:
 * <ol>
 *   <li>{@code KafkaProducer(Properties)} / {@code KafkaConsumer(Properties)}
 *       (and the Map overloads) immediately COPY the entries into an
 *       internal {@code ProducerConfig} / {@code ConsumerConfig}, which
 *       extends {@code AbstractConfig}. The copy is performed eagerly,
 *       at construction time, into private final fields. The
 *       client's runtime decisions
 *       (acks, retries, linger.ms, max.in.flight, key.serializer,
 *       group.id, ...) are read from THAT copy and only from that
 *       copy, never from the original Properties.</li>
 *   <li>The original Properties object is not retained — there is
 *       no reference back to it from the client. So a later
 *       {@code props.put("acks", "all")} on the same Properties
 *       object touches NO client state. From the application code's
 *       perspective the mutation succeeds (put returns the previous
 *       value, no exception) — but the live producer continues
 *       using whatever value was in effect at construction time.</li>
 *   <li>The trap: this code COMPILES and RUNS. There is no exception,
 *       no warning at startup, no broker-side error. The
 *       producer simply behaves as if the post-construction tweak
 *       never happened. The bug surfaces as "I set acks=all and we
 *       still lose records on broker shutdown" — and is invisible
 *       in code review because the line that "sets" acks is right
 *       there, just below the new KafkaProducer() line.</li>
 *   <li>The shape that bites: a refactor extracts the producer
 *       construction into a helper, then a programmer adds a
 *       "last-minute" property below the construct call without
 *       realizing the helper already took the snapshot. Or someone
 *       moves the constructor up to make the code "easier to read"
 *       and silently breaks the configuration.</li>
 *   <li>Same trap applies to {@code Map.put} — both the consumer
 *       and producer accept {@code Map<String, Object>} overloads,
 *       and the same eager-copy behavior holds. The rule treats
 *       Properties, Map, and HashMap as equivalent config holders.</li>
 * </ol>
 *
 * <p>What the rule catches (per-method, local-slot tracking):
 * <ol>
 *   <li>For each {@code INVOKESPECIAL <init>} on
 *       {@code KafkaProducer} or {@code KafkaConsumer} whose arg list
 *       is exactly {@code (Properties)} or {@code (Map)}: look at
 *       the previous significant instruction. If it is
 *       {@code ALOAD N}, record N as a FROZEN slot (mapped to the
 *       ctor owner, so the message can name the right client kind).</li>
 *   <li>For each subsequent {@code put} / {@code setProperty} call
 *       whose owner is {@code Properties} / {@code Map} / {@code HashMap}
 *       and whose receiver resolves (via 2-back ALOAD walk) to a
 *       frozen slot — fire.</li>
 *   <li>Conservative receiver resolution: the pre-call stack is
 *       [receiver, key, value]; the rule walks back through
 *       {@code prevSignificant} three times expecting one push per
 *       slot. Non-canonical key/value expressions (e.g., a method
 *       call that produces the key) make the resolution return null
 *       — accepted false negative.</li>
 * </ol>
 *
 * <p>This Bad class triggers FOUR fires across three methods:
 * <ol>
 *   <li>{@code producerThenMutate}: new KafkaProducer + props.put
 *       — the canonical shape.</li>
 *   <li>{@code consumerThenMutate}: new KafkaConsumer +
 *       props.setProperty — exercises the setProperty path.</li>
 *   <li>{@code mapOverloadThenMutate}: uses the {@code Map}
 *       constructor overload + Map.put — exercises the Map config
 *       holder.</li>
 *   <li>{@code twoMutationsOneCtor}: one ctor + two follow-up
 *       put() calls — fires twice on the same method, demonstrating
 *       that the rule reports EVERY use-after-freeze, not just the
 *       first.</li>
 * </ol>
 */
public final class BadPropertiesMutatedAfterCtor {

    /** Anti-pattern: construct, then "tweak" the Properties. The tweak does nothing. */
    public KafkaProducer<String, String> producerThenMutate() {
        Properties props = baseProducerProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        props.put("acks", "all"); // reported — too late, AbstractConfig snapshot already taken
        return producer;
    }

    /** Anti-pattern: setProperty after ctor. Same trap. */
    public KafkaConsumer<String, String> consumerThenMutate() {
        Properties props = baseConsumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        props.setProperty("max.poll.records", "100"); // reported — too late
        return consumer;
    }

    /** Map overload + Map.put. Map config holder is tracked just like Properties. */
    public KafkaProducer<String, String> mapOverloadThenMutate() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        cfg.put("client.id", "bad-map-overload");
        cfg.put("compression.type", "lz4");
        cfg.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        cfg.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(cfg);
        cfg.put("retries", "5"); // reported — too late, even for Map config
        return producer;
    }

    /** Two mutations after one ctor. Rule reports every offending site. */
    public KafkaProducer<String, String> twoMutationsOneCtor() {
        Properties props = baseProducerProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        props.put("acks", "all");         // reported
        props.put("retries", "5");        // reported (separate site)
        return producer;
    }

    private static Properties baseProducerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("client.id", "bad-properties-mutated");
        p.put("compression.type", "lz4");
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return p;
    }

    private static Properties baseConsumerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("client.id", "bad-properties-mutated");
        p.put("group.id", "orders-processor");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return p;
    }
}
