package sample;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * RULE: PRODUCER_COMPRESSION_NONE_EXPLICIT.
 *
 * <p>Fires when a producer-config holder ({@link Properties}, {@link Map},
 * {@link HashMap}) is populated with the literal pair
 * {@code "compression.type" -> "none"} via {@code put(...)} /
 * {@code setProperty(...)}.
 *
 * <p>Note that the default for {@code compression.type} on the producer
 * is also {@code "none"} (the broker-side default is
 * {@code "producer"}). The rule deliberately targets EXPLICIT writes
 * of {@code "none"} — code that says "we have considered compression
 * and chosen none" — because that's almost never the right choice:
 *
 * <ul>
 *   <li><b>Wire size.</b> Plain JSON or Avro records compress 3-5× with
 *       zstd/lz4. On a multi-MB/s producer this is the difference
 *       between saturating a 10Gbit NIC and idling at 2Gbit.</li>
 *   <li><b>Broker disk.</b> Uncompressed segments evict from page cache
 *       faster, making tail reads slower for ALL consumers on that
 *       broker (not just this producer's topic).</li>
 *   <li><b>Cross-AZ egress.</b> AWS/GCP charges by the byte. A 4×
 *       compression ratio cuts cross-AZ replication cost by 4×.</li>
 *   <li><b>p99 latency on the broker.</b> Smaller produce requests
 *       fit fewer records per produce-thread iteration, but the
 *       page-cache hit improvement dominates on any realistic
 *       cluster.</li>
 * </ul>
 *
 * <p>What this looks like in production:
 * <ol>
 *   <li>A team copies a producer config from a tutorial that explicitly
 *       set {@code compression.type=none} for "simplicity".</li>
 *   <li>The application ships, traffic ramps, broker disk usage
 *       grows 4× faster than capacity-planning predicted.</li>
 *   <li>SRE adds brokers to expand capacity, expansion takes weeks,
 *       compounds the cross-AZ replication cost.</li>
 *   <li>Eventually someone profiles and notices that toggling to
 *       zstd cuts the broker disk footprint by 75% and the cross-AZ
 *       bill by 75% — work that would have been a one-line config
 *       change at day one.</li>
 * </ol>
 *
 * <p>Trade-off (legitimate but rare): if the records are already
 * compressed at the application layer (e.g. pre-gzipped payload),
 * setting {@code compression.type=none} is correct — double-compression
 * is wasted CPU. Document the assumption in code.
 *
 * <p>Recommendation: {@code compression.type=zstd} for almost all
 * modern workloads (Kafka 2.1+ supports zstd; compression ratio ~25%
 * better than lz4, CPU cost comparable on modern hardware).
 * {@code compression.type=lz4} for latency-critical pipelines where
 * compressor CPU is the bottleneck.
 *
 * <p>Detection: {@code ConfigKeyValueRule.literal} matches a two-LDC
 * pair where the key is the literal {@code "compression.type"} and
 * the value is the literal {@code "none"}, followed by an INVOKE of
 * {@code put} or {@code setProperty} on {@link Properties}, {@link Map}
 * or {@link HashMap}.
 */
public final class BadProducerCompressionNoneExplicit {

    private static final String BROKERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092";

    /** Anti-pattern: Properties.setProperty("compression.type", "none") — FIRES. */
    public Properties propertiesSetProperty() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.setProperty("compression.type", "none"); // FIRES — explicit opt-out
        return props;
    }

    /** Anti-pattern: Properties.put("compression.type", "none") — FIRES. */
    public Properties propertiesPut() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put("compression.type", "none"); // FIRES — explicit opt-out
        return props;
    }

    /** Anti-pattern: HashMap.put("compression.type", "none") — FIRES. */
    public Map<String, Object> mapPut() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        cfg.put("compression.type", "none"); // FIRES — explicit opt-out
        return cfg;
    }

    /** Control: compression.type=zstd — must NOT fire. */
    public Properties controlZstd() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.setProperty("compression.type", "zstd");
        return props;
    }

    /** Control: compression.type=lz4 — must NOT fire. */
    public Properties controlLz4() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.setProperty("compression.type", "lz4");
        return props;
    }
}
