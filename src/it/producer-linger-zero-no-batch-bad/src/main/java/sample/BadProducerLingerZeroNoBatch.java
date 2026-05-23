package sample;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * RULE: PRODUCER_LINGER_ZERO_NO_BATCH.
 *
 * <p>Fires when a producer-config holder ({@link Properties}, {@link Map},
 * {@link HashMap}) is populated with the literal pair
 * {@code "linger.ms" -> "0"} via {@code put(...)} / {@code setProperty(...)}.
 *
 * <p>{@code linger.ms} controls how long the producer's accumulator
 * holds records in the per-partition queue before the Sender thread
 * drains them into a {@code ProduceRequest}. The default is 0 — same
 * as the value this rule targets — but the rule deliberately matches
 * the EXPLICIT write, because:
 * <ul>
 *   <li>Code that says {@code linger.ms=0} declares an intent: "send
 *       each record as fast as possible, never coalesce."</li>
 *   <li>That intent is almost always wrong. {@code linger.ms=5} (5ms
 *       hold) routinely batches 10-100× more records per request
 *       on a hot producer for ~5ms of added p50 latency — a trade-off
 *       no one would knowingly refuse.</li>
 *   <li>Code that simply doesn't set the key inherits the default
 *       but signals "I haven't thought about batching" — a different
 *       category of mistake, often caught by an adjacent
 *       {@code linger.ms.absent} rule. This rule targets the
 *       INTENTIONAL choice.</li>
 * </ul>
 *
 * <p>What "no batching" actually costs:
 * <ul>
 *   <li><b>One {@code ProduceRequest} per record.</b> Every record
 *       crosses the wire as its own request. On a 10k-records/sec
 *       producer that's 10k RPC/sec to the broker — versus ~100/sec
 *       if {@code batch.size}-bounded batches form naturally.</li>
 *   <li><b>Broker request-handler thread saturation.</b> The broker
 *       has a fixed {@code num.io.threads} pool. 10k RPC/sec from
 *       one producer leaves no headroom for other clients; tail
 *       latency on the SAME broker degrades for everyone.</li>
 *   <li><b>Compression efficiency collapses.</b> Single-record
 *       batches compress poorly; what would have been 4× under zstd
 *       on a 100-record batch becomes 1.2× on a single-record batch.</li>
 *   <li><b>Network overhead dominates payload.</b> TCP+TLS+Kafka
 *       framing is ~100 bytes per request. A 500-byte record now
 *       pays 25% framing tax.</li>
 * </ul>
 *
 * <p>What this looks like in production:
 * <ol>
 *   <li>A team measures latency on a synthetic single-record
 *       benchmark and concludes "linger.ms=0 is fastest."</li>
 *   <li>Production traffic is bursty (10k req/sec peak). The producer
 *       still emits 10k RPC/sec because each record skips the
 *       accumulator.</li>
 *   <li>Broker IO threads saturate, p99 latency climbs from 5ms to
 *       80ms across all clients.</li>
 *   <li>The "fix" turns out to be {@code linger.ms=5} +
 *       {@code batch.size=131072} — adding 5ms of p50 buys back 60ms
 *       of p99 for everyone on the cluster.</li>
 * </ol>
 *
 * <p>Recommendation: {@code linger.ms=5} to {@code linger.ms=20} for
 * normal pipelines. Higher ({@code 50-100}) for high-volume
 * batch/ingestion pipelines. {@code 0} only when a benchmark shows
 * the latency floor matters more than throughput AND a documented
 * justification exists in the same config file.
 *
 * <p>Detection: {@code ConfigKeyValueRule.literal} matches a two-LDC
 * pair where the key is the literal {@code "linger.ms"} and the value
 * is the literal {@code "0"}, followed by an INVOKE of {@code put} or
 * {@code setProperty} on {@link Properties}, {@link Map} or
 * {@link HashMap}.
 */
public final class BadProducerLingerZeroNoBatch {

    private static final String BROKERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092";

    /** Anti-pattern: Properties.setProperty("linger.ms", "0") — FIRES. */
    public Properties propertiesSetProperty() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.setProperty("linger.ms", "0"); // FIRES — explicit no-batch
        return props;
    }

    /** Anti-pattern: Properties.put("linger.ms", "0") — FIRES. */
    public Properties propertiesPut() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put("linger.ms", "0"); // FIRES — explicit no-batch
        return props;
    }

    /** Anti-pattern: HashMap.put("linger.ms", "0") — FIRES. */
    public Map<String, Object> mapPut() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        cfg.put("linger.ms", "0"); // FIRES — explicit no-batch
        return cfg;
    }

    /** Control: linger.ms=5 — must NOT fire. */
    public Properties controlLingerFive() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.setProperty("linger.ms", "5");
        return props;
    }
}
