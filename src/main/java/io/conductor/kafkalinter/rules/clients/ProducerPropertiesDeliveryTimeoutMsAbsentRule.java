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
 * Project-scoped rule. Fires on a plain kafka-clients producer .properties
 * file that does NOT set {@code delivery.timeout.ms}. The kafka-clients
 * default is {@code 120000} (2 minutes) — defensible for most workloads,
 * but too long for latency-sensitive synchronous paths and too short for
 * bulk pipelines tolerating long broker outages. INFO severity because
 * the default is workable, not broken.
 *
 * <p>A file is "producer-shaped" when it sets {@code key.serializer} OR
 * {@code value.serializer} as a TOP-LEVEL key. Excludes Connect and
 * Streams configs.
 */
public final class ProducerPropertiesDeliveryTimeoutMsAbsentRule implements ProjectScopedRule {

    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String DELIVERY_TIMEOUT_MS = "delivery.timeout.ms";

    private final Severity severity;

    public ProducerPropertiesDeliveryTimeoutMsAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_PROPERTIES_DELIVERY_TIMEOUT_MS_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String shapeKey = producerShapeKey(p);
            if (shapeKey == null) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(DELIVERY_TIMEOUT_MS))) continue;
            out.add(new Violation(
                    RuleId.PRODUCER_PROPERTIES_DELIVERY_TIMEOUT_MS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + DELIVERY_TIMEOUT_MS, 0,
                    "kafka-clients producer .properties file (detected via top-level `"
                            + shapeKey + "`) does NOT set `" + DELIVERY_TIMEOUT_MS
                            + "`. kafka-clients defaults `" + DELIVERY_TIMEOUT_MS
                            + "` to `120000` (2 minutes) — the wall-clock cap on "
                            + "per-record `send()`-to-callback, covering `linger.ms` + "
                            + "`request.timeout.ms` + every retry. The default is "
                            + "defensible for the typical producer but: (a) too long "
                            + "for latency-sensitive synchronous paths (an HTTP "
                            + "handler blocking 120s on Kafka is a service incident); "
                            + "(b) too short for bulk pipelines expecting long broker "
                            + "outages (a nightly batch failing during a 20-min "
                            + "planned failover wastes work that a 1-hour budget "
                            + "would have absorbed). The value should match the "
                            + "application's SLO AND the cluster's expected outage "
                            + "envelope. Fix: set `" + DELIVERY_TIMEOUT_MS
                            + "` to a value matched to the workload, OR set it to "
                            + "`120000` EXPLICITLY to document that the default was "
                            + "deliberately chosen."));
        }
        return out;
    }

    private static String producerShapeKey(Properties p) {
        if (isNonEmpty(p.getProperty(KEY_SERIALIZER))) return KEY_SERIALIZER;
        if (isNonEmpty(p.getProperty(VALUE_SERIALIZER))) return VALUE_SERIALIZER;
        return null;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
