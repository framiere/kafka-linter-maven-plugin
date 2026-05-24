package sample;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * RULE: PRODUCER_TRANSACTIONAL_ID_RANDOM.
 *
 * <p>Fires when {@code transactional.id} is set to
 * {@code UUID.randomUUID().toString()} via {@code Properties.put},
 * {@code Properties.setProperty}, or {@code Map.put}.
 *
 * <p>A random per-process transactional.id defeats Kafka's zombie-producer
 * fencing — every restart registers under a brand-new id, the broker has no
 * prior state to bump epoch on, and any zombie producer from the prior
 * incarnation keeps committing under its old id. read_committed consumers
 * downstream see BOTH timelines as legitimate committed transactions.
 */
public final class BadProducerTransactionalIdRandom {

    /** Anti-pattern #1: Properties.put with literal key — FIRES. */
    public Properties buildPropertiesPut() {
        Properties props = new Properties();
        props.put("transactional.id", UUID.randomUUID().toString()); // FIRES
        return props;
    }

    /** Anti-pattern #2: Properties.setProperty — FIRES. */
    public Properties buildPropertiesSetProperty() {
        Properties props = new Properties();
        props.setProperty("transactional.id", UUID.randomUUID().toString()); // FIRES
        return props;
    }

    /** Anti-pattern #3: ProducerConfig.TRANSACTIONAL_ID_CONFIG constant — FIRES (javac inlines the constant). */
    public Properties buildPropertiesProducerConfigConstant() {
        Properties props = new Properties();
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString()); // FIRES
        return props;
    }

    /** Anti-pattern #4: HashMap<String,Object> config — FIRES. */
    public Map<String, Object> buildMap() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("transactional.id", UUID.randomUUID().toString()); // FIRES
        return cfg;
    }
}
