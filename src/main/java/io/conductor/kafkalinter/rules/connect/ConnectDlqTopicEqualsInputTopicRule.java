package io.conductor.kafkalinter.rules.connect;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Project-scoped rule. Fires when a Kafka Connect sink connector .properties
 * file declares an {@code errors.deadletterqueue.topic.name=<dlq>} whose value
 * is ALSO present in the connector's own {@code topics=} input list.
 *
 * <p>KIP-298 (Kafka 2.0+) introduced the dead-letter-queue. Connect does NOT
 * validate at startup that the DLQ topic is disjoint from the consumed
 * topics; when they collide, every failed record is published to the DLQ,
 * immediately re-polled by the same task, fails again, and re-loops at full
 * broker throughput until disk fills.
 *
 * <p>Out of scope: {@code topics.regex=<pattern>} (regex-membership testing
 * requires broker topic enumeration and is therefore left to a separate
 * runtime-aware check).
 */
public final class ConnectDlqTopicEqualsInputTopicRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String TOPICS = "topics";
    private static final String DLQ_TOPIC = "errors.deadletterqueue.topic.name";

    private final Severity severity;

    public ConnectDlqTopicEqualsInputTopicRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DLQ_TOPIC_EQUALS_INPUT_TOPIC;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;

            String dlq = trimOrNull(p.getProperty(DLQ_TOPIC));
            if (dlq == null) continue;

            Set<String> inputTopics = parseTopics(p.getProperty(TOPICS));
            if (!inputTopics.contains(dlq)) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DLQ_TOPIC_EQUALS_INPUT_TOPIC, severity,
                    ctx.relativize(e.getKey()), "key:" + DLQ_TOPIC, 0,
                    "Connect sink connector " + p.getProperty(CONNECTOR_CLASS)
                            + " has " + DLQ_TOPIC + "=" + dlq
                            + " which is ALSO listed in topics=" + p.getProperty(TOPICS).trim()
                            + " — every record that fails conversion or SMT processing is "
                            + "published to '" + dlq + "', immediately re-polled by this same "
                            + "sink, fails again, is re-published to '" + dlq + "', and so on "
                            + "at full broker throughput until disk fills. Connect performs NO "
                            + "startup validation of DLQ-vs-input disjointness. Rename the DLQ "
                            + "topic to a distinct name (e.g., '" + dlq + ".dlq', '"
                            + p.getProperty(CONNECTOR_CLASS).replaceAll(".*\\.", "").toLowerCase()
                            + ".dlq', or a per-cluster 'connect.dlq')."));
        }
        return out;
    }

    private static Set<String> parseTopics(String raw) {
        if (!notBlank(raw)) return Set.of();
        Set<String> out = new HashSet<>();
        for (String token : raw.split(",")) {
            String t = token.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }

    private static boolean notBlank(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
