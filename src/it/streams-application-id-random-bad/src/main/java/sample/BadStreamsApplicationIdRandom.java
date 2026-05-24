package sample;

import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * RULE: STREAMS_APPLICATION_ID_RANDOM.
 *
 * <p>Fires when {@code application.id} is set to
 * {@code UUID.randomUUID().toString()} via {@code Properties.put},
 * {@code Properties.setProperty}, or {@code Map.put}.
 *
 * <p>A random per-process application.id destroys every Streams identity at
 * once: the consumer group becomes brand-new (no committed offsets), the
 * state directory becomes empty (full changelog replay), internal topics
 * accrete forever on the broker, and EOS-v2 fencing is broken because the
 * broker has never seen the transactional.id and never bumps the epoch.
 */
public final class BadStreamsApplicationIdRandom {

    /** Anti-pattern #1: Properties.put with literal key — FIRES. */
    public Properties buildPropertiesPut() {
        Properties props = new Properties();
        props.put("application.id", UUID.randomUUID().toString()); // FIRES
        return props;
    }

    /** Anti-pattern #2: Properties.setProperty — FIRES. */
    public Properties buildPropertiesSetProperty() {
        Properties props = new Properties();
        props.setProperty("application.id", UUID.randomUUID().toString()); // FIRES
        return props;
    }

    /** Anti-pattern #3: StreamsConfig.APPLICATION_ID_CONFIG constant — FIRES (javac inlines the constant). */
    public Properties buildPropertiesStreamsConfigConstant() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString()); // FIRES
        return props;
    }

    /** Anti-pattern #4: HashMap<String,Object> config — FIRES. */
    public Map<String, Object> buildMap() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("application.id", UUID.randomUUID().toString()); // FIRES
        return cfg;
    }
}
