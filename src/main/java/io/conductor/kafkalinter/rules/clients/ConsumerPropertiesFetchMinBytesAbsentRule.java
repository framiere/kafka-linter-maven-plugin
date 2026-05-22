package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Project-scoped rule. Fires on a kafka-clients consumer .properties
 * file that does NOT set {@code fetch.min.bytes}. The kafka-clients
 * default is {@code 1} byte, which is the latency-optimal but
 * throughput-pessimal choice — high-throughput consumers should
 * bump it to 50000+ to amortize per-fetch RPC overhead. INFO
 * severity because the default is workload-dependent.
 *
 * <p>A file is "consumer-shaped" when it sets {@code key.deserializer}
 * OR {@code value.deserializer} as a TOP-LEVEL key. Excludes Connect
 * and Streams configs.
 */
public final class ConsumerPropertiesFetchMinBytesAbsentRule implements ProjectScopedRule {

    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String FETCH_MIN_BYTES = "fetch.min.bytes";

    private final Severity severity;

    public ConsumerPropertiesFetchMinBytesAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_PROPERTIES_FETCH_MIN_BYTES_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String shapeKey = consumerShapeKey(p);
            if (shapeKey == null) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(FETCH_MIN_BYTES))) continue;
            out.add(new Violation(
                    RuleId.CONSUMER_PROPERTIES_FETCH_MIN_BYTES_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + FETCH_MIN_BYTES, 0,
                    "kafka-clients consumer .properties file (detected via top-level `"
                            + shapeKey + "`) does NOT set `" + FETCH_MIN_BYTES
                            + "`. kafka-clients defaults `" + FETCH_MIN_BYTES
                            + "` to `1` byte — the latency-optimal but "
                            + "throughput-pessimal choice. The broker waits "
                            + "up to `fetch.max.wait.ms` (default 500 ms) for "
                            + "`" + FETCH_MIN_BYTES + "` of data to accumulate "
                            + "before responding. With `1`, the broker "
                            + "responds the instant ANY record is available — "
                            + "right for low-latency consumers (monitoring, "
                            + "real-time fraud detection, interactive "
                            + "dashboards), silently wasteful for high-"
                            + "throughput batch consumers (ETL, ML training "
                            + "data, log aggregation, cross-DC replication) "
                            + "where per-fetch RPC overhead dominates and "
                            + "thousands of near-empty fetches per second "
                            + "load the broker's request-handler threads. "
                            + "Fix: set `" + FETCH_MIN_BYTES + "` to a value "
                            + "matched to the workload's latency tolerance "
                            + "(50000 for batch, 1000000 for cross-DC ETL, "
                            + "`1` for low-latency monitoring), OR set it to "
                            + "`1` EXPLICITLY to document that minimum-latency "
                            + "was deliberately chosen."));
        }
        return out;
    }

    private static String consumerShapeKey(Properties p) {
        if (isNonEmpty(p.getProperty(KEY_DESERIALIZER))) return KEY_DESERIALIZER;
        if (isNonEmpty(p.getProperty(VALUE_DESERIALIZER))) return VALUE_DESERIALIZER;
        return null;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
