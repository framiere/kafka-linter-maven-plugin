package io.conductor.kafkalinter.rules.connect;

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
 * Project-scoped rule. Fires when a Kafka Connect connector .properties file
 * has BOTH:
 * <ul>
 *   <li>a {@code connector.class} key (the canonical Connect-config fingerprint), AND</li>
 *   <li>{@code consumer.override.group.id} set to any non-blank value.</li>
 * </ul>
 *
 * <p>Connect's framework derives the consumer group.id from the connector's
 * name ({@code connect-<connector-name>}, or the KIP-875 cluster-scoped variant).
 * That group.id is a framework CONTRACT — REST API offset queries, ACL grants,
 * JMX metrics, and exactly-once-source fencing all depend on it. Overriding
 * {@code group.id} via the KIP-458 {@code consumer.override.*} mechanism creates
 * a split-brain: Connect's framework still thinks the group is the framework-
 * derived name, but the actual consumer commits to the override group.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectConsumerOverrideGroupIdRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String GROUP_ID_OVERRIDE = "consumer.override.group.id";

    private final Severity severity;

    public ConnectConsumerOverrideGroupIdRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_CONSUMER_OVERRIDE_GROUP_ID;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;
            String groupIdOverride = p.getProperty(GROUP_ID_OVERRIDE);
            if (!notBlank(groupIdOverride)) continue;

            out.add(new Violation(
                    RuleId.CONNECT_CONSUMER_OVERRIDE_GROUP_ID, severity,
                    ctx.relativize(e.getKey()), "key:" + GROUP_ID_OVERRIDE, 0,
                    "Connect connector " + p.getProperty(CONNECTOR_CLASS)
                            + " sets consumer.override.group.id=" + groupIdOverride.trim()
                            + " — this OVERRIDES Connect's framework-managed consumer group "
                            + "(connect-<connector-name>) and creates a split-brain: REST API "
                            + "offset queries (GET /connectors/{name}/offsets) return data for "
                            + "the framework group, but the actual consumer commits to the "
                            + "override group; ACL grants on the framework group don't apply; "
                            + "JMX metrics correlate against the wrong group identity. There is "
                            + "no legitimate production use case — to take over offsets from an "
                            + "external consumer, use the REST API's PUT /connectors/{name}/offsets "
                            + "(KIP-875, Kafka 3.6+). Remove this override."));
        }
        return out;
    }

    private static boolean notBlank(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
