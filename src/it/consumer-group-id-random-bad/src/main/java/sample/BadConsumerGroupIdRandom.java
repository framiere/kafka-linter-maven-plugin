package sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * RULE: CONSUMER_GROUP_ID_RANDOM.
 *
 * <p>Fires when {@code group.id} is set to {@code UUID.randomUUID().toString()}
 * via {@code Properties.put}, {@code Properties.setProperty}, or
 * {@code Map.put}.
 *
 * <p>A random per-process group.id has no committed-offset history. Every
 * restart establishes a brand-new consumer group; with
 * {@code auto.offset.reset=latest} (the default) every record between crash
 * and restart is silently skipped; with {@code earliest} the consumer replays
 * the entire topic on every restart. Either way at-least-once is broken.
 */
public final class BadConsumerGroupIdRandom {

    /** Anti-pattern #1: Properties.put with literal key — FIRES. */
    public Properties buildPropertiesPut() {
        Properties props = new Properties();
        props.put("group.id", UUID.randomUUID().toString()); // FIRES
        return props;
    }

    /** Anti-pattern #2: Properties.setProperty — FIRES. */
    public Properties buildPropertiesSetProperty() {
        Properties props = new Properties();
        props.setProperty("group.id", UUID.randomUUID().toString()); // FIRES
        return props;
    }

    /** Anti-pattern #3: ConsumerConfig.GROUP_ID_CONFIG constant — FIRES (javac inlines the constant). */
    public Properties buildPropertiesConsumerConfigConstant() {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString()); // FIRES
        return props;
    }

    /** Anti-pattern #4: HashMap<String,Object> config — FIRES. */
    public Map<String, Object> buildMap() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("group.id", UUID.randomUUID().toString()); // FIRES
        return cfg;
    }
}
