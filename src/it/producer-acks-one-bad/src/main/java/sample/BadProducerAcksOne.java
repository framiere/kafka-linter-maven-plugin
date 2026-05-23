package sample;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * RULE: PRODUCER_ACKS_ONE.
 *
 * <p>Fires when a producer-config holder ({@link Properties}, {@link Map},
 * {@link HashMap}) is populated with the literal pair {@code "acks" -> "1"}
 * via {@code put(...)} / {@code setProperty(...)}. With {@code acks=1},
 * the producer's send completes successfully as soon as the partition
 * LEADER has written the record to its local log — BEFORE the in-sync
 * followers have replicated it.
 *
 * <p>The data-loss window is the time between leader-local-write and
 * follower-replication, multiplied by the failure probability of the
 * leader broker during that window. On a busy cluster with active GC
 * and tail latency in the 100ms range, that window is large enough that
 * losing records on every controlled failover is a routine event:
 * <ul>
 *   <li>Producer sends record R; leader writes R locally and
 *       acknowledges.</li>
 *   <li>Producer's send callback completes successfully — the
 *       application moves on.</li>
 *   <li>Leader broker crashes / pauses / loses ZK session BEFORE
 *       followers fetch R.</li>
 *   <li>Controller promotes a follower to new leader. The new leader
 *       has no record of R. The high-watermark advances past R.</li>
 *   <li>R is silently lost. From the producer's perspective, everything
 *       was fine: the send succeeded.</li>
 * </ul>
 *
 * <p>What this looks like in production:
 * <ol>
 *   <li>A team picks {@code acks=1} because someone said
 *       "{@code acks=all} is too slow" without measuring.</li>
 *   <li>Routine broker rolling restarts (rolling upgrade, node
 *       replacement, configuration change) trigger controlled
 *       leader-elections every few weeks.</li>
 *   <li>Each controlled failover silently drops a small number of
 *       records (single digits to thousands depending on traffic).</li>
 *   <li>Six months later, an audit reconciles upstream-source counts
 *       against downstream-sink counts and discovers ~0.001%
 *       discrepancy — too small to alert on, too consistent to be
 *       random.</li>
 *   <li>The discrepancy is impossible to attribute to any single
 *       incident, because the loss is spread across dozens of
 *       routine restarts.</li>
 * </ol>
 *
 * <p>Legitimate uses for {@code acks=1} (narrow):
 * <ul>
 *   <li>Latency-critical pipeline where end-to-end p99 budget is sub-5ms
 *       AND the downstream system can tolerate the loss rate AND a
 *       deliberate trade-off has been documented.</li>
 *   <li>Logging/metrics pipelines where small losses on broker failover
 *       are acceptable.</li>
 *   <li>Anything in the durability path (orders, payments, audit logs,
 *       outbox patterns): use {@code acks=all} with
 *       {@code min.insync.replicas=2}.</li>
 * </ul>
 *
 * <p>Detection: {@code ConfigKeyValueRule.literal} matches a two-LDC
 * pair where the key is the literal {@code "acks"} and the value is the
 * literal {@code "1"}, followed by an INVOKE of {@code put} or
 * {@code setProperty} on {@link Properties}, {@link Map} or
 * {@link HashMap}.
 */
public final class BadProducerAcksOne {

    private static final String BROKERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092";

    /** Anti-pattern: Properties.setProperty("acks", "1") — FIRES. */
    public Properties propertiesSetProperty() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.setProperty("acks", "1"); // FIRES — leader-only ack
        return props;
    }

    /** Anti-pattern: Properties.put("acks", "1") — FIRES. */
    public Properties propertiesPut() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put("acks", "1"); // FIRES — leader-only ack
        return props;
    }

    /** Anti-pattern: HashMap.put("acks", "1") — FIRES. */
    public Map<String, Object> mapPut() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        cfg.put("acks", "1"); // FIRES — leader-only ack
        return cfg;
    }

    /** Control: acks=all — must NOT fire. */
    public Properties controlAcksAll() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.setProperty("acks", "all");
        return props;
    }
}
