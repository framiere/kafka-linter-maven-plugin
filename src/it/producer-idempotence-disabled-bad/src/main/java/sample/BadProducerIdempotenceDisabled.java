package sample;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * RULE: PRODUCER_IDEMPOTENCE_DISABLED.
 *
 * <p>Fires when a producer-config holder ({@link Properties}, {@link Map},
 * {@link HashMap}) is populated with the literal pair
 * {@code "enable.idempotence" -> "false"} via {@code put(...)} /
 * {@code setProperty(...)}. Since Kafka 3.0, the default is
 * {@code true}, so setting this explicitly to {@code "false"} is an
 * intentional opt-OUT of the producer's at-least-once guarantee.
 *
 * <p>What an idempotent producer gives you (and what disabling it
 * takes away):
 * <ul>
 *   <li><b>De-duplication on retry.</b> Each (producer-id, sequence-number)
 *       tuple is rejected at the broker if it's already been written.
 *       Without idempotence, a retry that succeeds after the original
 *       request also succeeded silently produces a duplicate.</li>
 *   <li><b>Per-partition ordering on retry.</b> The broker enforces
 *       monotonic sequence-number ordering per partition. Without
 *       idempotence, retries with {@code max.in.flight.requests > 1}
 *       can land out of order — record 5 ends up before record 4
 *       on disk.</li>
 *   <li><b>"Effectively-once" within a producer session.</b> No
 *       application-side de-dup logic needed for the producer-to-broker
 *       hop.</li>
 * </ul>
 *
 * <p>What disabling idempotence looks like in production:
 * <ol>
 *   <li>A team disables {@code enable.idempotence} because their app
 *       was running on Kafka 2.x and they "forgot to revisit" after
 *       upgrading to 3.0.</li>
 *   <li>The producer runs fine for months. Steady-state, retries are
 *       rare.</li>
 *   <li>A broker rolling restart triggers a wave of retriable errors
 *       (NotLeaderForPartition, NetworkException). Each retry that
 *       arrives after a successful original write produces a duplicate.</li>
 *   <li>Downstream sees duplicates. The team blames the consumer
 *       ("must be re-reading offsets") or the broker ("must be a Kafka
 *       bug"). It is neither. It is the producer retry path doing
 *       exactly what it documented it does.</li>
 *   <li>Mitigation requires either re-enabling idempotence (right) or
 *       building an application-level dedup layer (wrong, expensive,
 *       lossy).</li>
 * </ol>
 *
 * <p>Trade-off (legitimate but rare): the idempotent producer requires
 * {@code acks=all} and caps {@code max.in.flight.requests.per.connection}
 * at 5. If a benchmark genuinely shows that {@code acks=1} +
 * higher in-flight is required to meet a latency SLO, document the
 * trade-off in code and accept the cost. The trade-off should not be
 * implicit.
 *
 * <p>Detection: {@code ConfigKeyValueRule.literal} matches a two-LDC
 * pair where the key is the literal {@code "enable.idempotence"} and
 * the value is the literal {@code "false"}, followed by an INVOKE of
 * {@code put} or {@code setProperty} on {@link Properties}, {@link Map}
 * or {@link HashMap}.
 */
public final class BadProducerIdempotenceDisabled {

    private static final String BROKERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092";

    /** Anti-pattern: Properties.setProperty("enable.idempotence", "false") — FIRES. */
    public Properties propertiesSetProperty() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.setProperty("enable.idempotence", "false"); // FIRES — opt-out
        return props;
    }

    /** Anti-pattern: Properties.put("enable.idempotence", "false") — FIRES. */
    public Properties propertiesPut() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put("enable.idempotence", "false"); // FIRES — opt-out
        return props;
    }

    /** Anti-pattern: HashMap.put("enable.idempotence", "false") — FIRES. */
    public Map<String, Object> mapPut() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        cfg.put("enable.idempotence", "false"); // FIRES — opt-out
        return cfg;
    }

    /** Control: enable.idempotence=true (default) — must NOT fire. */
    public Properties controlIdempotenceTrue() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.setProperty("enable.idempotence", "true");
        return props;
    }

    /** Control: enable.idempotence absent (default applies) — must NOT fire. */
    public Properties controlIdempotenceAbsent() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        return props;
    }
}
