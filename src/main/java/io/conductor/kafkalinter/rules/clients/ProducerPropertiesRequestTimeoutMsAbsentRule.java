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
 * file (identified by top-level {@code key.serializer} or
 * {@code value.serializer}) that does NOT set {@code request.timeout.ms}.
 *
 * <p>kafka-clients' {@code ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG}
 * defaults to {@code 30000} (30 seconds). It is the PER-REQUEST broker-
 * response wait window. Structurally coupled with {@code delivery.timeout.ms}
 * (total retry budget) and {@code linger.ms} (batch wait) via the kafka-
 * clients invariant: {@code delivery.timeout.ms >= linger.ms + request.timeout.ms}.
 *
 * <p>The invariant is enforced at {@code KafkaProducer} construction; default
 * values are used in the check when keys are absent. So setting only
 * {@code delivery.timeout.ms=10000} (without request.timeout.ms) triggers
 * ConfigException because default request.timeout.ms=30000 exceeds 10000.
 *
 * <p>INFO severity because defaults work at default settings; the rule is a
 * documentation nudge prompting the operator to set ALL THREE timeouts
 * (delivery, request, linger) together as a single coupled retry budget.
 *
 * <p>A file is "producer-shaped" when it sets {@code key.serializer} OR
 * {@code value.serializer} as a TOP-LEVEL key. Excludes Connect
 * ({@code connector.class}) and Streams ({@code application.id}).
 */
public final class ProducerPropertiesRequestTimeoutMsAbsentRule implements ProjectScopedRule {

    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String REQUEST_TIMEOUT_MS = "request.timeout.ms";

    private final Severity severity;

    public ProducerPropertiesRequestTimeoutMsAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_PROPERTIES_REQUEST_TIMEOUT_MS_ABSENT;
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
            if (isNonEmpty(p.getProperty(REQUEST_TIMEOUT_MS))) continue;
            out.add(new Violation(
                    RuleId.PRODUCER_PROPERTIES_REQUEST_TIMEOUT_MS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + REQUEST_TIMEOUT_MS, 0,
                    "kafka-clients producer .properties file (detected via "
                            + "top-level `" + shapeKey + "`) does NOT set `"
                            + REQUEST_TIMEOUT_MS + "`. kafka-clients "
                            + "defaults `" + REQUEST_TIMEOUT_MS + "` to "
                            + "`30000` (30 seconds). This is the PER-"
                            + "REQUEST broker-response wait window — how "
                            + "long the producer waits for one in-flight "
                            + "ProduceRequest's ack before declaring it "
                            + "failed and (potentially) retrying. It is "
                            + "structurally coupled with `delivery.timeout."
                            + "ms` (default 120000ms, the TOTAL retry "
                            + "budget across all retries) and `linger.ms` "
                            + "(default 0, the batch-accumulation window) "
                            + "via the kafka-clients invariant: `delivery."
                            + "timeout.ms >= linger.ms + " + REQUEST_TIMEOUT_MS
                            + "`. kafka-clients THROWS `ConfigException` "
                            + "at `KafkaProducer` construction if the "
                            + "invariant fails. The check uses whatever "
                            + "values are in effect — defaults or explicit. "
                            + "So a operator setting only `delivery.timeout."
                            + "ms=10000` (without " + REQUEST_TIMEOUT_MS
                            + ") triggers ConfigException because default "
                            + REQUEST_TIMEOUT_MS + "=30000 exceeds 10000. "
                            + "Specific impacts of the absent key: (a) "
                            + "**ConfigException at startup** — tuning "
                            + "`delivery.timeout.ms` below 30000 without "
                            + "also setting " + REQUEST_TIMEOUT_MS
                            + " crashes the application; (b) **silent "
                            + "retry-arithmetic drift** — tuning "
                            + "`delivery.timeout.ms=3600000` (1 hour) with "
                            + "default " + REQUEST_TIMEOUT_MS + "=30000 "
                            + "allows up to 120 retries — far more than "
                            + "the operator likely intended; (c) **slow-"
                            + "broker case** — tuning " + REQUEST_TIMEOUT_MS
                            + " UP for a known-slow broker without "
                            + "tuning `delivery.timeout.ms` halves the "
                            + "effective retry count. Common bug shapes: "
                            + "(1) team sets `delivery.timeout.ms=10000` "
                            + "for fast-failover, hits ConfigException at "
                            + "startup, has to also set " + REQUEST_TIMEOUT_MS
                            + " below 10000; (2) nightly batch sets "
                            + "`delivery.timeout.ms=3600000`, leaves "
                            + REQUEST_TIMEOUT_MS + " default → silent "
                            + "120-retry budget; (3) cross-region producer "
                            + "bumps " + REQUEST_TIMEOUT_MS + "=90000 to "
                            + "absorb 60s broker acks during link "
                            + "congestion, leaves `delivery.timeout.ms` "
                            + "default 120s → only 1.3 retries fit; (4) "
                            + "Resilience4j layer on top × default "
                            + "producer retries = 450s total budget when "
                            + "operator wanted 30s; (5) Spring Boot multi-"
                            + "producer template, all 4 producers inherit "
                            + "default " + REQUEST_TIMEOUT_MS + ", no per-"
                            + "producer differentiation possible. Fix: ONE "
                            + "LINE. Set `" + REQUEST_TIMEOUT_MS
                            + "` paired with the chosen `delivery.timeout."
                            + "ms` and `linger.ms`, ALWAYS with all three "
                            + "set together so the retry budget breakdown "
                            + "is explicit. Examples: low-latency sync "
                            + "send: `" + REQUEST_TIMEOUT_MS + "=5000, "
                            + "delivery.timeout.ms=10000`; bulk batch: "
                            + "`" + REQUEST_TIMEOUT_MS + "=60000, "
                            + "delivery.timeout.ms=600000` (10 retries); "
                            + "cross-region: `" + REQUEST_TIMEOUT_MS
                            + "=90000, delivery.timeout.ms=360000` (4 "
                            + "retries). Sibling rule [[producer-"
                            + "properties-delivery-timeout-ms-absent]] "
                            + "catches the coupled-partner absence."));
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
