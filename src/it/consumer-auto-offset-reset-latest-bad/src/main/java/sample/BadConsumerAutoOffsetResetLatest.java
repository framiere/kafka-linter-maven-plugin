package sample;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * RULE: CONSUMER_AUTO_OFFSET_RESET_LATEST.
 *
 * <p>Fires when a consumer-config holder ({@link Properties}, {@link Map},
 * {@link HashMap}) is populated with the literal pair
 * {@code "auto.offset.reset" -> "latest"} via {@code put(...)} /
 * {@code setProperty(...)}. This is the "skip everything before I
 * showed up" reset policy.
 *
 * <p>{@code auto.offset.reset} controls what happens when a consumer
 * group has NO COMMITTED OFFSETS for the partitions it's been assigned
 * — which happens in exactly three real situations:
 * <ul>
 *   <li>The consumer group is BRAND NEW (group.id never seen before
 *       by the broker).</li>
 *   <li>The committed offset has aged out of {@code __consumer_offsets}
 *       (the group has been idle longer than
 *       {@code offsets.retention.minutes}, default 7 days in Kafka 2.0+).</li>
 *   <li>The committed offset has been DELETED by an admin (reset, or
 *       group.id rename, or whole-group drop).</li>
 * </ul>
 * In all three cases, {@code latest} silently skips every record
 * already on the topic. The team has just deployed a brand-new
 * consumer, the topic has 30 days of history, and the consumer reads
 * NOTHING from those 30 days. The first record it picks up is whatever
 * was produced after it joined.
 *
 * <p>What this looks like in production:
 * <ol>
 *   <li>A team deploys a new {@code billing-aggregator} consumer
 *       group reading from the {@code orders} topic.</li>
 *   <li>The {@code orders} topic has 30 days of history, ~50M records.</li>
 *   <li>{@code auto.offset.reset=latest} → the consumer joins, the
 *       coordinator assigns partitions, and the consumer's position
 *       jumps to the high-watermark of each partition. The 50M
 *       historical records are skipped silently.</li>
 *   <li>Discovery shape: weeks later, the aggregation dashboard
 *       reports billing totals that are too low. The team spends
 *       three days hunting for a producer bug before realizing
 *       the consumer simply never read the back-history.</li>
 *   <li>Mitigation requires either (a) committing offsets at the
 *       log-start of each partition and restarting (ugly,
 *       coordinator-dependent), or (b) replaying via a tool like
 *       {@code kafka-consumer-groups.sh --reset-offsets --to-earliest}
 *       (better, but ops cycle takes hours).</li>
 * </ol>
 *
 * <p>Legitimate uses for {@code auto.offset.reset=latest} (narrow):
 * <ul>
 *   <li>Health-check / heartbeat consumer where back-history is
 *       irrelevant — only "are messages flowing right now" matters.</li>
 *   <li>Real-time alerting consumer for events where stale events
 *       are not actionable (e.g. p99 latency anomaly detector).</li>
 *   <li>Anything that reads a windowed, time-bounded stream where
 *       events older than the window are by definition irrelevant.</li>
 * </ul>
 * <p>Recommendation: {@code auto.offset.reset=earliest} for almost
 * every batch-style or accumulator-style consumer; {@code none} for
 * consumers where the absence of committed offsets is itself an
 * operational error that should fail-fast.
 *
 * <p>Detection: {@code ConfigKeyValueRule.literal} matches a two-LDC
 * pair where the key is the literal {@code "auto.offset.reset"} and
 * the value is the literal {@code "latest"}, followed by an INVOKE of
 * {@code put} or {@code setProperty} on {@link Properties}, {@link Map}
 * or {@link HashMap}.
 */
public final class BadConsumerAutoOffsetResetLatest {

    private static final String BROKERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092";

    /** Anti-pattern: Properties.setProperty("auto.offset.reset", "latest") — FIRES. */
    public Properties propertiesSetProperty() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "billing-aggregator");
        props.setProperty("auto.offset.reset", "latest"); // FIRES — skips back-history
        return props;
    }

    /** Anti-pattern: Properties.put("auto.offset.reset", "latest") — FIRES. */
    public Properties propertiesPut() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "billing-aggregator");
        props.put("auto.offset.reset", "latest"); // FIRES — skips back-history
        return props;
    }

    /** Anti-pattern: HashMap.put("auto.offset.reset", "latest") — FIRES. */
    public Map<String, Object> mapPut() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, "billing-aggregator");
        cfg.put("auto.offset.reset", "latest"); // FIRES — skips back-history
        return cfg;
    }

    /** Control: auto.offset.reset=earliest — must NOT fire. */
    public Properties controlEarliest() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "billing-aggregator");
        props.setProperty("auto.offset.reset", "earliest");
        return props;
    }

    /** Control: auto.offset.reset=none (fail-fast) — must NOT fire. */
    public Properties controlNone() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "billing-aggregator");
        props.setProperty("auto.offset.reset", "none");
        return props;
    }
}
