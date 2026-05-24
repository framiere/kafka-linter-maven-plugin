package sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * RULE: CONSUMER_GROUP_INSTANCE_ID_RANDOM.
 *
 * <p>Fires when {@code group.instance.id} is set to {@code UUID.randomUUID().toString()}
 * via {@code Properties.put}, {@code Properties.setProperty}, or {@code Map.put}.
 *
 * <p>{@code group.instance.id} opts a consumer into KIP-345 static membership: a STABLE
 * application-supplied identity that the broker recognises across restarts and uses to
 * SKIP the group rebalance protocol. The strict contract: the SAME instance ID,
 * presented by a restarted consumer within {@code session.timeout.ms}, returns the
 * previous partition assignment unchanged — no rebalance, no offset commit storm, no
 * state-store warm-up.
 *
 * <p>{@code UUID.randomUUID().toString()} produces a NEW string per process: the broker
 * sees a brand-new member, the old instance ID waits out its session as a phantom, and
 * a full rebalance fires on every restart — exactly what static membership exists to
 * eliminate. Net effect: the application pays the protocol cost of static membership
 * (extra coordinator state, typically a longer {@code session.timeout.ms} to absorb
 * deploys) with NONE of the benefit, strictly worse than leaving group.instance.id
 * unset.
 *
 * <p>This Bad class triggers FOUR fires — one per {@code put}/{@code setProperty}
 * shape × key-literal-vs-inlined-constant.
 */
public final class BadConsumerGroupInstanceIdRandom {

    /** Anti-pattern #1: Properties.put with literal key — FIRES. */
    public Properties buildPropertiesPut() {
        Properties props = new Properties();
        props.put("group.instance.id", UUID.randomUUID().toString()); // FIRES
        return props;
    }

    /** Anti-pattern #2: Properties.setProperty — FIRES. */
    public Properties buildPropertiesSetProperty() {
        Properties props = new Properties();
        props.setProperty("group.instance.id", UUID.randomUUID().toString()); // FIRES
        return props;
    }

    /** Anti-pattern #3: ConsumerConfig.GROUP_INSTANCE_ID_CONFIG constant — FIRES (javac inlines the constant). */
    public Properties buildPropertiesConsumerConfigConstant() {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, UUID.randomUUID().toString()); // FIRES
        return props;
    }

    /** Anti-pattern #4: HashMap&lt;String, Object&gt; config — FIRES. */
    public Map<String, Object> buildMap() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("group.instance.id", UUID.randomUUID().toString()); // FIRES
        return cfg;
    }
}
