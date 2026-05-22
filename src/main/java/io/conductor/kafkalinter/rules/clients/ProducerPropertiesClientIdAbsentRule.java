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
 * {@code value.serializer}) that does NOT set {@code client.id}.
 *
 * <p>kafka-clients' {@code ProducerConfig.CLIENT_ID_CONFIG} defaults to
 * {@code ""} (empty). When empty, kafka-clients auto-generates
 * {@code producer-<N>} per JVM — opaque, unstable across restarts (counter
 * resets), indistinguishable across deployments (every microservice's first
 * producer is {@code producer-1}). Broker request logs, per-client-id
 * quotas (KIP-13), and per-instance Grafana boards are all useless.
 *
 * <p>INFO severity because the framework does not fail and some frameworks
 * inject the value programmatically (Spring Boot's
 * {@code spring.application.name} integration).
 *
 * <p>A file is "producer-shaped" when it sets {@code key.serializer} OR
 * {@code value.serializer} as a TOP-LEVEL key. Excludes Connect
 * ({@code connector.class}) and Streams ({@code application.id} —
 * Streams' StreamThread derives client.id from application.id).
 */
public final class ProducerPropertiesClientIdAbsentRule implements ProjectScopedRule {

    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String CLIENT_ID = "client.id";

    private final Severity severity;

    public ProducerPropertiesClientIdAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_PROPERTIES_CLIENT_ID_ABSENT;
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
            if (isNonEmpty(p.getProperty(CLIENT_ID))) continue;
            out.add(new Violation(
                    RuleId.PRODUCER_PROPERTIES_CLIENT_ID_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + CLIENT_ID, 0,
                    "kafka-clients producer .properties file (detected via "
                            + "top-level `" + shapeKey + "`) does NOT set `"
                            + CLIENT_ID + "`. kafka-clients defaults `"
                            + CLIENT_ID + "` to `\"\"` (empty); when empty, "
                            + "the constructor auto-generates `producer-<N>` "
                            + "from a per-JVM AtomicInteger that RESETS on "
                            + "every restart. Every place where `client.id` "
                            + "shows up downstream — broker request logs, "
                            + "KIP-13 per-client-id quotas (`kafka-configs "
                            + "--entity-type clients --entity-name <client-"
                            + "id>`), broker-side per-client metric labels, "
                            + "JMX `kafka.producer:type=producer-metrics,"
                            + "client-id=<x>` — is left with `producer-N`. "
                            + "Two replicas of the same Deployment both get "
                            + "`producer-1` (each pod is its own JVM); broker "
                            + "sees identical client_id from two source IPs; "
                            + "per-pod investigation is impossible; per-pod "
                            + "quota cannot be applied. Common bug shapes: "
                            + "(1) incident at broker — 'which app caused "
                            + "the produce spike?' answer: unknown, the "
                            + "client.id is `producer-3` which is true of "
                            + "every 3rd-constructed producer across 50 "
                            + "microservices; (2) SRE wants to throttle ONE "
                            + "noisy producer via KIP-13 quota — cannot "
                            + "target because the client.id is auto-gen; "
                            + "(3) per-app Grafana boards on `kafka_server_"
                            + "BrokerTopicMetrics_BytesIn{client_id=<x>}` "
                            + "show stacks of `producer-1`, `producer-2` "
                            + "with no app correlation. Fix: set `"
                            + CLIENT_ID + "=<service-name>-<producer-"
                            + "purpose>` (e.g., `orders-service-event-"
                            + "producer`, `payments-fraud-callback-"
                            + "producer`). Avoid generic values (`my-"
                            + "producer`, `producer`, `client-1`) — those "
                            + "defeat the purpose; the sibling rule "
                            + "[[kafka-client-id-generic]] catches them."));
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
