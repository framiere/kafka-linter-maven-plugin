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
 * file that does NOT set {@code max.poll.records}. The kafka-clients
 * default is {@code 500} — fine for fast processing, dangerous for
 * slow processing where the batch can exceed {@code max.poll.interval.ms}
 * and trigger a rebalance storm. INFO severity because the default is
 * defensible for typical workloads.
 *
 * <p>A file is "consumer-shaped" when it sets {@code key.deserializer}
 * OR {@code value.deserializer} as a TOP-LEVEL key. Excludes Connect
 * and Streams configs.
 */
public final class ConsumerPropertiesMaxPollRecordsAbsentRule implements ProjectScopedRule {

    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String MAX_POLL_RECORDS = "max.poll.records";

    private final Severity severity;

    public ConsumerPropertiesMaxPollRecordsAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_PROPERTIES_MAX_POLL_RECORDS_ABSENT;
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
            if (isNonEmpty(p.getProperty(MAX_POLL_RECORDS))) continue;
            out.add(new Violation(
                    RuleId.CONSUMER_PROPERTIES_MAX_POLL_RECORDS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + MAX_POLL_RECORDS, 0,
                    "kafka-clients consumer .properties file (detected via top-level `"
                            + shapeKey + "`) does NOT set `" + MAX_POLL_RECORDS
                            + "`. kafka-clients defaults `" + MAX_POLL_RECORDS
                            + "` to `500` — the upper bound on records returned by "
                            + "a single `poll()` call. The pairing with "
                            + "`max.poll.interval.ms` (default 300000 ms = 5 min) "
                            + "is critical: if `per-record-cost × max.poll.records "
                            + "> max.poll.interval.ms`, the consumer is evicted "
                            + "from the group mid-batch, the partition rebalances, "
                            + "the new owner re-delivers and may also time out — "
                            + "rebalance storm halts forward progress. The default "
                            + "500 is fine for fast processing (<100 ms per "
                            + "record) but dangerous for slow processing "
                            + "(>600 ms) and wasteful for bulk processing "
                            + "(<1 ms). Rule of thumb: `max.poll.records × "
                            + "p99-per-record-cost < max.poll.interval.ms × 0.5`. "
                            + "Fix: set `" + MAX_POLL_RECORDS + "` to a value "
                            + "matched to per-record processing cost, OR set it "
                            + "to `500` EXPLICITLY to document that the default "
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
