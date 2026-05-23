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
 * Project-scoped rule. Fires when a Kafka client .properties file
 * (Producer / Consumer / Streams) sets {@code bootstrap.servers} but does NOT
 * set {@code metadata.recovery.strategy}.
 *
 * <p>Per KIP-899 (Apache Kafka 3.7, March 2024), the knob defaults to
 * {@code none} (legacy behavior: cache IPs at bootstrap, never re-resolve).
 * For Kubernetes deployments — where broker pod IPs are ephemeral and rotate
 * on restart, scaling, node-eviction, or rolling upgrades — {@code rebootstrap}
 * is the correct choice: re-resolve {@code bootstrap.servers} via DNS when
 * all cached brokers become unreachable.
 *
 * <p>Excludes Kafka Connect worker configs (presence of {@code connector.class})
 * — Connect has its own restart-on-failure semantics.
 *
 * <p>Detects both the plain key and the Spring Boot prefixed variants for
 * bootstrap-servers ({@code spring.kafka.bootstrap-servers}) and the recovery
 * strategy ({@code spring.kafka.properties.metadata.recovery.strategy}).
 *
 * <p>Emits one INFO violation per offending file.
 */
public final class KafkaClientMetadataRecoveryStrategyAbsentRule implements ProjectScopedRule {

    private static final String BOOTSTRAP_PLAIN = "bootstrap.servers";
    private static final String BOOTSTRAP_SPRING = "spring.kafka.bootstrap-servers";
    private static final String RECOVERY_PLAIN = "metadata.recovery.strategy";
    private static final String RECOVERY_SPRING = "spring.kafka.properties.metadata.recovery.strategy";
    private static final String CONNECTOR_CLASS = "connector.class";

    private final Severity severity;

    public KafkaClientMetadataRecoveryStrategyAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.KAFKA_CLIENT_METADATA_RECOVERY_STRATEGY_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Path file = e.getKey();
            Properties p = e.getValue();

            // Skip Connect worker/connector configs.
            if (trimOrNull(p.getProperty(CONNECTOR_CLASS)) != null) continue;

            String bootstrapPlain = trimOrNull(p.getProperty(BOOTSTRAP_PLAIN));
            String bootstrapSpring = trimOrNull(p.getProperty(BOOTSTRAP_SPRING));
            if (bootstrapPlain == null && bootstrapSpring == null) continue;

            String recoveryPlain = trimOrNull(p.getProperty(RECOVERY_PLAIN));
            String recoverySpring = trimOrNull(p.getProperty(RECOVERY_SPRING));
            if (recoveryPlain != null || recoverySpring != null) continue;

            String bootstrapKey = bootstrapPlain != null ? BOOTSTRAP_PLAIN : BOOTSTRAP_SPRING;

            out.add(new Violation(
                    RuleId.KAFKA_CLIENT_METADATA_RECOVERY_STRATEGY_ABSENT, severity,
                    ctx.relativize(file), "key:" + bootstrapKey, 0,
                    "Kafka client .properties file sets `" + bootstrapKey + "` but does "
                            + "NOT set `metadata.recovery.strategy` (neither plain nor Spring "
                            + "Boot prefixed `spring.kafka.properties.metadata.recovery."
                            + "strategy`). Per KIP-899 (Apache Kafka 3.7, March 2024), the "
                            + "knob defaults to `none` for backward compatibility — meaning "
                            + "the client resolves `bootstrap.servers` ONCE at startup, "
                            + "caches the resulting IP addresses in its internal `Metadata` "
                            + "object, and NEVER re-bootstraps even when ALL broker "
                            + "connections in the cached metadata fail. For Kubernetes "
                            + "deployments — where broker pod IPs rotate on restart, "
                            + "scaling, node-eviction, or rolling upgrades — this is a "
                            + "stuck-client-on-restart trap: when broker pods recycle (e.g., "
                            + "rolling broker upgrade, K8s zone-eviction event, Strimzi "
                            + "config-change reconciliation, `helm uninstall && helm "
                            + "install`), the cached IPs become stale, every connection "
                            + "attempt times out with `Bootstrap broker <stale-ip>:9092 "
                            + "(id: -1 rack: null) disconnected`, and the client stays "
                            + "stuck until manually restarted. Setting `metadata.recovery."
                            + "strategy=rebootstrap` enables KIP-899's recovery mode: when "
                            + "ALL brokers in cached metadata become unreachable, the "
                            + "client RE-RESOLVES `bootstrap.servers` via DNS (fresh "
                            + "lookup against the K8s DNS service) and rebuilds metadata "
                            + "from the newly-resolved IPs. Common bug shapes: (1) "
                            + "Kubernetes rolling broker upgrade — broker StatefulSet pods "
                            + "rotate, each getting new IPs; clients without rebootstrap "
                            + "stuck on the old IPs for hours until restarted. (2) AWS AZ "
                            + "eviction event — K8s recreates broker pod in another AZ "
                            + "with a new IP; client's next rolling-restart cascades the "
                            + "stuck state. (3) Helm cluster recreate — `helm uninstall && "
                            + "helm install` rotates all broker pod IPs; every existing "
                            + "client is stuck. (4) K8s Service IP rotation under some "
                            + "networking configurations (Cilium endpoint-slice churn). "
                            + "(5) Strimzi operator config-change reconciliation triggers "
                            + "chained broker restarts; non-rebootstrap clients survive "
                            + "single restarts but fail under chained restarts. (6) Spring "
                            + "Boot autoconfig default-leak — `KafkaAutoConfiguration` "
                            + "does NOT set this knob; every Spring Boot Kafka app "
                            + "inherits `none` by default. INFO is the right level "
                            + "because: (a) the knob requires Apache Kafka 3.7+ (older "
                            + "clients silently ignore it); (b) stable-IP deployments "
                            + "(bare-metal, VMs with fixed bootstrap IPs, deployments "
                            + "behind a load balancer that handles IP rotation "
                            + "transparently) work fine with the default `none` and "
                            + "`rebootstrap` would just add startup-DNS-flap risk during "
                            + "transient DNS failures. The rule prompts the operator to "
                            + "make the K8s-vs-VM choice explicit. Fix: add `metadata."
                            + "recovery.strategy=rebootstrap` to this properties file. "
                            + "For Spring Boot: `spring.kafka.properties.metadata."
                            + "recovery.strategy=rebootstrap`. For stable-IP non-K8s "
                            + "deployments: set the key explicitly to `none` with a "
                            + "comment documenting the stable-IP rationale — the "
                            + "explicit value disables this rule's trigger. Sibling "
                            + "rules: KAFKA_CLIENT_RACK_PLACEHOLDER (the rack-aware-fetch "
                            + "sibling — both Kubernetes-deployment-hygiene rules), "
                            + "BOOTSTRAP_SERVERS_LOCALHOST_SUSPICIOUS (the bootstrap-"
                            + "target-correctness gate), SR_LATEST_CACHE_TTL_SEC_ABSENT "
                            + "(the SR-cache-permanence-trap with similar 'default-is-"
                            + "wrong-for-modern-deployments' shape)."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
