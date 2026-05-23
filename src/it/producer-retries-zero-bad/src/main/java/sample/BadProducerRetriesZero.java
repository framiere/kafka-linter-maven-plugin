package sample;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * RULE: PRODUCER_RETRIES_ZERO.
 *
 * <p>Fires when a producer-config holder ({@link Properties}, {@link Map},
 * {@link HashMap}) is populated with the literal pair
 * {@code "retries" -> "0"} via {@code put(...)} /
 * {@code setProperty(...)}. The default is {@code Integer.MAX_VALUE},
 * bounded in wall-clock terms by {@code delivery.timeout.ms} (default
 * 2 minutes). Setting {@code retries=0} disables the retry path
 * entirely: ANY retriable error from the broker becomes a permanent
 * send failure the application has to handle itself.
 *
 * <p>What "retriable error" actually covers in routine operation:
 * <ul>
 *   <li>{@code NotLeaderForPartitionException} — happens during every
 *       controlled partition-leader move (rolling restart, broker
 *       replacement, manual reassignment).</li>
 *   <li>{@code NetworkException} — happens any time the producer's
 *       TCP connection to a broker is reset.</li>
 *   <li>{@code UnknownTopicOrPartitionException} — transient during
 *       topic creation propagation, partition expansion.</li>
 *   <li>{@code NotEnoughReplicasException} — transient when
 *       {@code min.insync.replicas} can't be satisfied during a
 *       broker pause.</li>
 *   <li>{@code RequestTimedOutException} — broker GC pause or hung
 *       partition leader.</li>
 * </ul>
 * The default retry behavior absorbs every one of these silently. With
 * {@code retries=0}, all of them surface as send failures.
 *
 * <p>What this looks like in production:
 * <ol>
 *   <li>A team picks {@code retries=0} citing a stale 0.x-era blog
 *       post warning about retries causing duplicates. With
 *       {@code enable.idempotence=true} (the default since 3.0),
 *       retries no longer produce duplicates — the warning is obsolete.</li>
 *   <li>Steady-state: no failures, the change appears benign.</li>
 *   <li>First broker rolling restart: every partition whose leader
 *       moves yields a brief window of {@code NotLeaderForPartition}.
 *       Without retries, those records become send failures.</li>
 *   <li>The application either drops the records (loss) or implements
 *       a hand-rolled retry layer (expensive, error-prone) — both
 *       worse than what the producer would have done by default.</li>
 * </ol>
 *
 * <p>Legitimate uses for {@code retries=0} (very narrow):
 * <ul>
 *   <li>Application has its own outbox/retry layer and treats the
 *       producer as a pure transport — explicit, documented choice.</li>
 *   <li>Test harness measuring raw error-surface behavior.</li>
 *   <li>Anything else: leave the default ({@code Integer.MAX_VALUE})
 *       and bound retries with {@code delivery.timeout.ms}.</li>
 * </ul>
 *
 * <p>Detection: {@code ConfigKeyValueRule.literal} matches a two-LDC
 * pair where the key is the literal {@code "retries"} and the value is
 * the literal {@code "0"}, followed by an INVOKE of {@code put} or
 * {@code setProperty} on {@link Properties}, {@link Map} or
 * {@link HashMap}.
 */
public final class BadProducerRetriesZero {

    private static final String BROKERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092";

    /** Anti-pattern: Properties.setProperty("retries", "0") — FIRES. */
    public Properties propertiesSetProperty() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.setProperty("retries", "0"); // FIRES — no retry on transient errors
        return props;
    }

    /** Anti-pattern: Properties.put("retries", "0") — FIRES. */
    public Properties propertiesPut() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put("retries", "0"); // FIRES — no retry on transient errors
        return props;
    }

    /** Anti-pattern: HashMap.put("retries", "0") — FIRES. */
    public Map<String, Object> mapPut() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        cfg.put("retries", "0"); // FIRES — no retry on transient errors
        return cfg;
    }

    /** Control: retries=Integer.MAX_VALUE (default) — must NOT fire. */
    public Properties controlRetriesDefault() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.setProperty("retries", "2147483647");
        return props;
    }

    /** Control: retries absent (default applies) — must NOT fire. */
    public Properties controlRetriesAbsent() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        return props;
    }
}
