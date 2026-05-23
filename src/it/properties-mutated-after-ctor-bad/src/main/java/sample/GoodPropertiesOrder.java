package sample;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * Control for PROPERTIES_MUTATED_AFTER_CTOR.
 *
 * <p>Three methods, each exercises a different "silent" path:
 *
 * <ol>
 *   <li>{@code allMutationsBeforeCtor} — textbook order. All
 *       {@code put} calls happen BEFORE the ctor. At the moment the
 *       rule encounters the put calls, the slot is not yet in
 *       {@code frozenSlots} (the ctor hasn't been seen yet in linear
 *       scan order). Silent.</li>
 *   <li>{@code mutateFreshPropertiesNotTheFrozenOne} — the method has
 *       TWO Properties slots. Slot A is passed to the ctor and
 *       becomes frozen. Slot B is a separate, fresh Properties used
 *       for a different purpose (e.g., handing to another helper)
 *       and is mutated after the ctor. The mutation is on slot B —
 *       receiverSlotForPutCall resolves to B which is NOT in
 *       frozenSlots. Silent. This is the case the rule's
 *       per-slot map (not "any put after any ctor") is designed
 *       to handle.</li>
 *   <li>{@code mutateMapNotPassedToCtor} — uses a Map for the ctor
 *       and a Properties for something else. Even though both are
 *       config holders, the mutation site's receiver resolves to a
 *       slot that was never recorded as frozen. Silent.</li>
 * </ol>
 */
public final class GoodPropertiesOrder {

    /** All mutations before ctor. Linear-scan friendly: frozenSlots is empty at every put site. */
    public KafkaProducer<String, String> allMutationsBeforeCtor() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092"); // silent: frozenSlots empty
        props.put("client.id", "good-properties-order");                          // silent
        props.put("acks", "all");                                                 // silent
        props.put("compression.type", "lz4");                                     // silent
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props); // ctor LAST — props is frozen but never mutated after this point
    }

    /** Two Properties slots: only the frozen one would fire, mutating the OTHER one is fine. */
    public KafkaConsumer<String, String> mutateFreshPropertiesNotTheFrozenOne() {
        Properties consumerProps = baseConsumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps); // freezes consumerProps slot

        Properties unrelatedProps = new Properties();
        unrelatedProps.put("metric.reporter.config", "value"); // silent: this slot was never passed to a ctor

        return consumer;
    }

    /** Mix Map and Properties: the Map is frozen by the ctor, the Properties is not — mutating Properties is silent. */
    public KafkaProducer<String, String> mutateMapNotPassedToCtor() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        cfg.put("client.id", "good-map-then-properties");
        cfg.put("compression.type", "lz4");
        cfg.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        cfg.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(cfg); // freezes cfg slot

        Properties sideChannel = new Properties();
        sideChannel.put("side.value", "x"); // silent: sideChannel slot never passed to a ctor

        return producer;
    }

    private static Properties baseConsumerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("client.id", "good-properties-order");
        p.put("group.id", "orders-processor");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return p;
    }
}
