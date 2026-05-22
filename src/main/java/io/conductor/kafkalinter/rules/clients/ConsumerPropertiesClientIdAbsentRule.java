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
 * {@code value.deserializer}) that does NOT set {@code client.id}.
 *
 * <p>kafka-clients' {@code ConsumerConfig.CLIENT_ID_CONFIG} defaults to
 * {@code ""} (empty). When empty, kafka-clients auto-generates
 * {@code consumer-<group.id>-<N>} per JVM. Broker logs,
 * {@code kafka-consumer-groups --describe} output (CLIENT-ID column),
 * per-client-id quotas (KIP-13), and per-instance Grafana boards are all
 * left with this opaque label.
 *
 * <p>INFO severity because the framework does not fail and some frameworks
 * inject the value programmatically (Spring Kafka, Quarkus).
 *
 * <p>A file is "consumer-shaped" when it sets {@code key.deserializer} OR
 * {@code value.deserializer} as a TOP-LEVEL key. Excludes Connect
 * ({@code connector.class}) and Streams ({@code application.id} —
 * Streams' StreamThread derives client.id from application.id).
 */
public final class ConsumerPropertiesClientIdAbsentRule implements ProjectScopedRule {

    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String CLIENT_ID = "client.id";

    private final Severity severity;

    public ConsumerPropertiesClientIdAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_PROPERTIES_CLIENT_ID_ABSENT;
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
            if (isNonEmpty(p.getProperty(CLIENT_ID))) continue;
            out.add(new Violation(
                    RuleId.CONSUMER_PROPERTIES_CLIENT_ID_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + CLIENT_ID, 0,
                    "kafka-clients consumer .properties file (detected via "
                            + "top-level `" + shapeKey + "`) does NOT set `"
                            + CLIENT_ID + "`. kafka-clients defaults `"
                            + CLIENT_ID + "` to `\"\"` (empty); when empty, "
                            + "the constructor auto-generates `consumer-"
                            + "<group.id>-<N>` (or `consumer-<N>` if "
                            + "group.id is absent) from a per-JVM "
                            + "AtomicInteger that RESETS on every restart. "
                            + "When MULTIPLE consumer instances share one "
                            + "`group.id` (the standard consumer-group "
                            + "pattern), they ALL get the same prefix and "
                            + "only differ in the trailing integer — broker-"
                            + "side per-instance identification (which pod "
                            + "committed this offset? which instance is "
                            + "slowest?) is impossible. Specific impacts: "
                            + "(a) `kafka-consumer-groups --describe "
                            + "<group>` displays CLIENT-ID and HOST columns "
                            + "— every row reads `consumer-<group>-N` with "
                            + "no app/pod correlation; (b) KIP-13 consumer "
                            + "quotas (`kafka-configs --add-config "
                            + "'consumer_byte_rate=<x>' --entity-type "
                            + "clients --entity-name <client-id>`) cannot "
                            + "target a specific consumer instance; (c) "
                            + "JMX `kafka.consumer:type=consumer-fetch-"
                            + "manager-metrics,client-id=<x>` dashboards "
                            + "are unstable across restarts because bean "
                            + "init order varies. Common bug shapes: "
                            + "(1) consumer-lag incident — `kafka-"
                            + "consumer-groups --describe` shows 8 rows of "
                            + "`consumer-payments-events-1` through `-8`, "
                            + "no way to map to deployments; SRE has to go "
                            + "pod-by-pod via kubectl; (2) audit log for "
                            + "offset commits is anonymous — SOX auditor "
                            + "flags 'inability to attribute consumption'; "
                            + "(3) CI tests spin up ephemeral groups; "
                            + "broker-side metric cardinality explodes "
                            + "with stale `consumer-test-group-N` labels. "
                            + "Fix: set `" + CLIENT_ID + "=<service-name>-"
                            + "<consumer-role>` (e.g., `payments-fraud-"
                            + "screening`, `orders-export-consumer`). For "
                            + "multi-replica deployments, consider "
                            + "templating the pod-ordinal into client.id "
                            + "(`payments-fraud-${HOSTNAME}`) so each pod "
                            + "is broker-side-distinguishable. Avoid "
                            + "generic values (`my-consumer`, `consumer`) "
                            + "— sibling rule [[kafka-client-id-generic]] "
                            + "catches them."));
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
