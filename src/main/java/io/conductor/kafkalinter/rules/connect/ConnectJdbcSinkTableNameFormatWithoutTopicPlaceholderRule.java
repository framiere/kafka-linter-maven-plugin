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
 * Project-scoped rule. Fires when a Confluent JDBC sink connector consumes from
 * multiple topics (via comma-separated {@code topics=A,B,C} or {@code topics.regex=...})
 * AND sets {@code table.name.format=<literal>} to a value that does NOT contain the
 * {@code ${topic}} placeholder.
 *
 * <p>Without the placeholder, all N topics' records are funneled into a single shared
 * table — schemas collide, downstream queries break.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectJdbcSinkTableNameFormatWithoutTopicPlaceholderRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String JDBC_SINK_CLASS = "io.confluent.connect.jdbc.JdbcSinkConnector";
    private static final String TABLE_NAME_FORMAT = "table.name.format";
    private static final String TOPICS = "topics";
    private static final String TOPICS_REGEX = "topics.regex";
    private static final String TOPIC_PLACEHOLDER = "${topic}";

    private final Severity severity;

    public ConnectJdbcSinkTableNameFormatWithoutTopicPlaceholderRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_JDBC_SINK_TABLE_NAME_FORMAT_WITHOUT_TOPIC_PLACEHOLDER;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (!JDBC_SINK_CLASS.equals(connectorClass)) continue;

            String format = trimOrNull(p.getProperty(TABLE_NAME_FORMAT));
            if (format == null) continue;
            if (format.contains(TOPIC_PLACEHOLDER)) continue;

            int topicCount = countTopics(p.getProperty(TOPICS));
            String topicsRegex = trimOrNull(p.getProperty(TOPICS_REGEX));
            boolean multiTopic = topicCount > 1 || topicsRegex != null;
            if (!multiTopic) continue;

            String multiTopicDesc = topicsRegex != null
                    ? "topics.regex=" + topicsRegex
                    : "topics=" + p.getProperty(TOPICS).trim() + " (" + topicCount + " topics)";

            out.add(new Violation(
                    RuleId.CONNECT_JDBC_SINK_TABLE_NAME_FORMAT_WITHOUT_TOPIC_PLACEHOLDER, severity,
                    ctx.relativize(e.getKey()), "key:" + TABLE_NAME_FORMAT, 0,
                    "Confluent JDBC sink connector (" + JDBC_SINK_CLASS + ") consumes from "
                            + "multiple topics (" + multiTopicDesc + ") but table.name.format="
                            + format + " is a literal string without the `${topic}` placeholder. "
                            + "Result: every record from every topic is routed to a SINGLE shared "
                            + "table named `" + format + "`. Schemas collide; downstream queries "
                            + "that depend on per-source-table separation are wrong; if "
                            + "auto.create=true the connector may fail at startup with a "
                            + "schema-merge type conflict; if auto.evolve=true the connector "
                            + "silently ALTERs the shared table to accommodate every column from "
                            + "every topic, exploding the schema width. Fix: insert the `${topic}` "
                            + "placeholder into the format string (e.g., `table.name.format="
                            + "${topic}` for default-equivalent routing, `table.name.format="
                            + "cdc_${topic}` for per-topic prefix routing, or `table.name.format="
                            + "${topic}_v2` for per-topic suffix routing), OR reduce `topics` to a "
                            + "single topic if the operator's intent was truly 'one shared table'. "
                            + "Sibling rules: CONNECT_JDBC_SINK_UPSERT_OR_UPDATE_WITHOUT_PK "
                            + "(PK-config requirement for upsert/update), "
                            + "CONNECT_JDBC_SINK_DELETE_ENABLED_TRUE_WITHOUT_PK_MODE_RECORD_KEY "
                            + "(delete.enabled requires record_key), "
                            + "CONNECT_JDBC_SINK_AUTO_EVOLVE_TRUE_WITHOUT_AUTO_CREATE_TRUE "
                            + "(schema-evolution + creation requirement)."));
        }
        return out;
    }

    private static int countTopics(String raw) {
        if (raw == null) return 0;
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) return 0;
        int count = 0;
        for (String token : trimmed.split(",")) {
            if (!token.trim().isEmpty()) count++;
        }
        return count;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
