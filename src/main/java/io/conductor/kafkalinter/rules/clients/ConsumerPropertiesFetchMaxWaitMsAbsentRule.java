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
 * Project-scoped rule. Fires on a plain kafka-clients consumer .properties
 * file (identified by top-level {@code key.deserializer} or
 * {@code value.deserializer}) that does NOT set {@code fetch.max.wait.ms}.
 *
 * <p>kafka-clients' {@code ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG} defaults
 * to {@code 500} (500ms). The config is the broker-side LATENCY CEILING for
 * FetchRequest, partnered with {@code fetch.min.bytes} — the broker waits
 * up to {@code fetch.max.wait.ms} for {@code fetch.min.bytes} of data to
 * accumulate before responding.
 *
 * <p>When {@code fetch.min.bytes} is at default 1, the wait is moot
 * (responds instantly). When the operator tunes {@code fetch.min.bytes} UP
 * for throughput, {@code fetch.max.wait.ms} becomes the latency ceiling for
 * off-peak fetches — a 500ms default ceiling can break latency SLAs during
 * quiet periods.
 *
 * <p>INFO severity because the defaults work when fetch.min.bytes is unset;
 * the rule is a documentation nudge prompting the operator to set
 * fetch.min.bytes AND fetch.max.wait.ms TOGETHER.
 *
 * <p>A file is "consumer-shaped" when it sets {@code key.deserializer} OR
 * {@code value.deserializer} as a TOP-LEVEL key. Excludes Connect
 * ({@code connector.class}) and Streams ({@code application.id}).
 */
public final class ConsumerPropertiesFetchMaxWaitMsAbsentRule implements ProjectScopedRule {

    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String FETCH_MAX_WAIT_MS = "fetch.max.wait.ms";

    private final Severity severity;

    public ConsumerPropertiesFetchMaxWaitMsAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_PROPERTIES_FETCH_MAX_WAIT_MS_ABSENT;
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
            if (isNonEmpty(p.getProperty(FETCH_MAX_WAIT_MS))) continue;
            out.add(new Violation(
                    RuleId.CONSUMER_PROPERTIES_FETCH_MAX_WAIT_MS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + FETCH_MAX_WAIT_MS, 0,
                    "kafka-clients consumer .properties file (detected via "
                            + "top-level `" + shapeKey + "`) does NOT set `"
                            + FETCH_MAX_WAIT_MS + "`. kafka-clients "
                            + "defaults `" + FETCH_MAX_WAIT_MS + "` to "
                            + "`500` (500 milliseconds). This is the BROKER-"
                            + "SIDE LATENCY CEILING for FetchRequest, "
                            + "partnered with `fetch.min.bytes`. When a "
                            + "consumer issues a FetchRequest, the broker: "
                            + "(1) accumulates records; (2) if total "
                            + "accumulated bytes >= `fetch.min.bytes`, "
                            + "responds IMMEDIATELY; (3) otherwise waits up "
                            + "to `" + FETCH_MAX_WAIT_MS + "` ms before "
                            + "responding with whatever has accumulated. At "
                            + "defaults (`fetch.min.bytes=1, "
                            + FETCH_MAX_WAIT_MS + "=500`), the broker "
                            + "responds instantly — the 500ms ceiling is "
                            + "moot. The config becomes meaningful WHEN THE "
                            + "OPERATOR TUNES `fetch.min.bytes` UP: e.g., "
                            + "`fetch.min.bytes=65536` means the broker "
                            + "waits for 64 KB; during quiet periods it "
                            + "stalls up to 500ms waiting for data to "
                            + "accumulate. Specific impacts of the absent "
                            + "key: (a) **uncoupled tuning** — operator "
                            + "sets `fetch.min.bytes` without thinking "
                            + "about the latency ceiling; off-peak fetches "
                            + "stall up to 500ms silently; (b) **latency "
                            + "SLA breach during quiet periods** — a "
                            + "consumer that returns in 10ms during peak "
                            + "traffic can suddenly take 500ms during off-"
                            + "peak; (c) **bimodal latency distribution** "
                            + "— p50 is instant, p99 spikes to 500ms; "
                            + "operator confused by the bimodality. Common "
                            + "bug shapes: (1) team tunes "
                            + "`fetch.min.bytes=65536` for throughput; "
                            + "off-peak 3am traffic stalls 500ms; SRE sees "
                            + "p99 spike; (2) fraud-detection consumer with "
                            + "<100ms latency SLA breaks when team adds "
                            + "`fetch.min.bytes=4096`; off-peak fetches "
                            + "miss the SLA; (3) idle admin-event consumer "
                            + "with `fetch.min.bytes=8192` set 'for "
                            + "consistency' stalls 500ms on every poll "
                            + "(8 KB never accumulates between rare "
                            + "events); (4) Spring Boot tutorial says "
                            + "'set fetch-min-size for throughput' but "
                            + "doesn't mention fetch-max-wait; team breaks "
                            + "the SLA following the partial advice. Fix: "
                            + "ONE LINE. Set `" + FETCH_MAX_WAIT_MS
                            + "` paired with the chosen `fetch.min.bytes`. "
                            + "Examples: `fetch.min.bytes=1, "
                            + FETCH_MAX_WAIT_MS + "=10` (low-latency, "
                            + "responsive); `fetch.min.bytes=65536, "
                            + FETCH_MAX_WAIT_MS + "=50` (throughput with "
                            + "50ms latency ceiling); `fetch.min.bytes="
                            + "65536, " + FETCH_MAX_WAIT_MS + "=1000` "
                            + "(batch-oriented, willing to wait 1s for "
                            + "larger batches). Always set both keys "
                            + "together. Sibling rule [[consumer-"
                            + "properties-fetch-min-bytes-absent]] catches "
                            + "the coupled-partner absence."));
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
