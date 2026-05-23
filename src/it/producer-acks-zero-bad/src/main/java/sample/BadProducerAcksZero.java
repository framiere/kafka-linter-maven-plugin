package sample;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * RULE: PRODUCER_ACKS_ZERO.
 *
 * <p>Fires when a producer-config holder ({@link Properties}, {@link Map},
 * {@link HashMap}) is populated with the literal pair {@code "acks" -> "0"}
 * via {@code put(...)} / {@code setProperty(...)}. This is the "fire and
 * forget" delivery mode — the producer writes the record onto its TCP
 * socket and the {@code send()} callback completes successfully
 * IMMEDIATELY, without ever observing a broker acknowledgement.
 *
 * <p>Every failure mode the network or broker can produce becomes silent
 * data loss:
 * <ul>
 *   <li>Network drop between producer and broker — record never arrives.</li>
 *   <li>Broker GC pause or leader-election window — record arrives at a
 *       broker that can't accept writes, gets discarded silently.</li>
 *   <li>Connection reset mid-flight — buffered records evaporate.</li>
 * </ul>
 * In every case the producer's send callback already fired success. The
 * "messages-sent" metric counts SEND ATTEMPTS, not durable writes. There
 * is NO mechanism by which the application can learn the records were
 * lost.
 *
 * <p>What this looks like in production:
 * <ol>
 *   <li>A team picks {@code acks=0} because they saw a benchmark blog
 *       post comparing {@code acks=all} vs {@code acks=0} throughput. The
 *       benchmark omitted that the missing records were the entire point
 *       of the measurement.</li>
 *   <li>The system runs fine for 11 months. The broker is never
 *       restarted.</li>
 *   <li>On month 12, an AZ outage triggers a cascading leader-election
 *       across 4 brokers over 90 seconds. During those 90 seconds, the
 *       producer continues to {@code send()} and continues to report
 *       success.</li>
 *   <li>Some unknown number of records — likely tens of thousands —
 *       evaporate. They never appear on a topic. The application has no
 *       way to enumerate them.</li>
 *   <li>Discovery shape: weeks later, downstream reports a "gap in the
 *       data between 03:14:12 and 03:15:42". No producer log line
 *       mentions it. No metric flagged it. Nothing was ever logged
 *       because from the producer's point of view, nothing failed.</li>
 * </ol>
 *
 * <p>Legitimate uses for {@code acks=0} (very narrow):
 * <ul>
 *   <li>Metrics shipper publishing low-value telemetry where loss of
 *       0.01% is acceptable AND the throughput delta vs {@code acks=1}
 *       is measurable.</li>
 *   <li>Benchmark harness measuring raw send throughput.</li>
 *   <li>Anything else: {@code acks=all} (Kafka 3.0+ default).</li>
 * </ul>
 *
 * <p>Detection: {@code ConfigKeyValueRule.literal} matches a two-LDC
 * pair where the key is the literal {@code "acks"} and the value is the
 * literal {@code "0"}, immediately followed by an INVOKE of {@code put}
 * or {@code setProperty} on {@link Properties}, {@link Map} or
 * {@link HashMap}. The match is literal-only by design — a value
 * computed at runtime or boxed via {@code Integer.toString(0)} will not
 * fire, to keep false-positives at zero.
 */
public final class BadProducerAcksZero {

    private static final String BROKERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092";

    /** Anti-pattern: Properties.setProperty("acks", "0") — FIRES. */
    public Properties propertiesSetProperty() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.setProperty("acks", "0"); // FIRES — acks=0 fire-and-forget
        return props;
    }

    /** Anti-pattern: Properties.put("acks", "0") — FIRES. */
    public Properties propertiesPut() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put("acks", "0"); // FIRES — acks=0 fire-and-forget
        return props;
    }

    /** Anti-pattern: HashMap.put("acks", "0") — FIRES. */
    public Map<String, Object> mapPut() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        cfg.put("acks", "0"); // FIRES — acks=0 fire-and-forget
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
