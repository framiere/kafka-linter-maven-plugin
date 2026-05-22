package io.conductor.kafkalinter.rules.connect;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Project-scoped rule. Fires when any MirrorMaker 2 connector config
 * ({@code connector.class} in the MM2 connector set) has
 * {@code source.cluster.bootstrap.servers} or
 * {@code target.cluster.bootstrap.servers} pointing at a loopback address
 * ({@code localhost}, {@code 127.0.0.1}, {@code 0.0.0.0}).
 *
 * <p>Disjoint from the existing {@code KAFKA_BOOTSTRAP_SERVERS_LOCALHOST}
 * rule, which fires only on the standard Kafka-client {@code bootstrap.servers}
 * key. MM2 uses two separate per-cluster bootstrap keys and is not covered
 * by that rule.
 */
public final class Mm2ClusterBootstrapServersLocalhostRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final Set<String> MM2_CONNECTOR_CLASSES = Set.of(
            "org.apache.kafka.connect.mirror.MirrorSourceConnector",
            "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
            "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector");
    private static final String SOURCE_BOOTSTRAP = "source.cluster.bootstrap.servers";
    private static final String TARGET_BOOTSTRAP = "target.cluster.bootstrap.servers";

    private final Severity severity;

    public Mm2ClusterBootstrapServersLocalhostRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.MM2_CLUSTER_BOOTSTRAP_SERVERS_LOCALHOST;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!MM2_CONNECTOR_CLASSES.contains(connectorClass)) continue;

            String file = ctx.relativize(e.getKey());
            checkKey(out, p, file, connectorClass, SOURCE_BOOTSTRAP, "source");
            checkKey(out, p, file, connectorClass, TARGET_BOOTSTRAP, "target");
        }
        return out;
    }

    private void checkKey(List<Violation> out, Properties p, String file,
                          String connectorClass, String key, String role) {
        String raw = trimOrNull(p.getProperty(key));
        if (raw == null) return;
        String lower = raw.toLowerCase(Locale.ROOT);
        if (!(lower.contains("localhost")
                || lower.contains("127.0.0.1")
                || lower.contains("0.0.0.0"))) {
            return;
        }
        out.add(new Violation(
                RuleId.MM2_CLUSTER_BOOTSTRAP_SERVERS_LOCALHOST, severity,
                file, "key:" + key, 0,
                "MirrorMaker 2 " + role + "-cluster bootstrap servers on "
                        + connectorClass + " (" + key + "=" + raw + ") points "
                        + "at a LOOPBACK address. `localhost`, `127.0.0.1`, and "
                        + "`0.0.0.0` resolve only to the loopback interface of "
                        + "the host where the MM2 connector is running — they "
                        + "cannot reach any Kafka broker on a separate host or "
                        + "in a separate Kubernetes pod. In a standard Connect-"
                        + "cluster deployment, the connector runs on a worker "
                        + "host that is NOT also a Kafka broker; the loopback "
                        + "address does not resolve to any broker; the inner "
                        + "Kafka client's bootstrap (`MetadataRequest`) fails "
                        + "with `Connection refused`; the client enters an "
                        + "indefinite exponential-backoff retry loop; the task "
                        + "holds its Connect-cluster slot but no replication "
                        + "advances. The dominant origin of this misconfig is: "
                        + "(a) dev-to-prod leak — the .properties was written "
                        + "against a local Docker-Compose stack where "
                        + "`localhost:9092` IS reachable, and was checked into "
                        + "git as-is; (b) Helm-chart placeholder default that "
                        + "was never overridden in the production values file; "
                        + "(c) tutorial copy-paste from Confluent's MM2 quick-"
                        + "start (which uses `localhost:9092` throughout) that "
                        + "was only partially updated for production. Fix: "
                        + "replace the loopback address with the actual broker "
                        + "hostnames reachable from the Connect-worker host "
                        + "(e.g., `primary-kafka.internal:9093`). Recommended "
                        + "practice is to externalize via a `ConfigProvider` "
                        + "(`${env:SOURCE_BOOTSTRAP_SERVERS}`) so the file "
                        + "never contains environment-specific hostnames. "
                        + "Detection: case-insensitive substring match for "
                        + "`localhost`/`127.0.0.1`/`0.0.0.0` on the trimmed "
                        + "value. The standard `KAFKA_BOOTSTRAP_SERVERS_LOCALHOST` "
                        + "rule fires only on the `bootstrap.servers` key and "
                        + "does NOT cover MM2's per-cluster bootstrap keys — "
                        + "this rule fills that gap."));
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
