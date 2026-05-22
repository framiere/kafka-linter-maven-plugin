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
 * Project-scoped rule. Fires when a Kafka Connect SINK connector .properties
 * file (identified by {@code connector.class} ending in {@code SinkConnector})
 * declares NEITHER {@code topics=} NOR {@code topics.regex=}.
 *
 * <p>Connect's {@code SinkConnectorConfig} requires EXACTLY ONE of the two
 * keys to be set. Neither set → {@code ConfigException("Must configure one of
 * topics or topics.regex")} at config-parse time; the connector cannot
 * register.
 *
 * <p>Sink-connector detection uses the {@code *SinkConnector} class-name
 * suffix — a near-universal convention across Apache Kafka, Confluent,
 * MirrorMaker, and third-party connectors. Source connectors don't set these
 * keys (they're sink-only configs) and must not fire on this rule.
 *
 * <p>Sibling rule: {@code CONNECT_SINK_TOPICS_AND_TOPICS_REGEX_BOTH_SET} (the
 * inverse — both set, equally invalid).
 */
public final class ConnectSinkTopicsAndTopicsRegexNeitherSetRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String TOPICS = "topics";
    private static final String TOPICS_REGEX = "topics.regex";
    private static final String SINK_CONNECTOR_SUFFIX = "SinkConnector";

    private final Severity severity;

    public ConnectSinkTopicsAndTopicsRegexNeitherSetRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_SINK_TOPICS_AND_TOPICS_REGEX_NEITHER_SET;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = p.getProperty(CONNECTOR_CLASS);
            if (!isNonEmpty(connectorClass)) continue;
            if (!connectorClass.trim().endsWith(SINK_CONNECTOR_SUFFIX)) continue;
            if (isNonEmpty(p.getProperty(TOPICS))) continue;
            if (isNonEmpty(p.getProperty(TOPICS_REGEX))) continue;
            out.add(new Violation(
                    RuleId.CONNECT_SINK_TOPICS_AND_TOPICS_REGEX_NEITHER_SET, severity,
                    ctx.relativize(e.getKey()), "key:" + TOPICS, 0,
                    "Kafka Connect SINK connector " + connectorClass.trim()
                            + " declares NEITHER `" + TOPICS + "` NOR `"
                            + TOPICS_REGEX + "`. Connect's "
                            + "SinkConnectorConfig requires EXACTLY ONE — "
                            + "throws ConfigException at config-parse time: "
                            + "'Must configure one of topics or topics.regex'. "
                            + "The connector cannot register; the worker logs "
                            + "the failure at ERROR level; the connector "
                            + "status goes from CREATED to FAILED. Distributed "
                            + "Connect leaves the failed-status entry in "
                            + "connect-status until DELETE /connectors/<name>. "
                            + "Common trigger: copy-paste from a SOURCE "
                            + "connector template (source connectors don't "
                            + "subscribe to topics), a refactor that deletes "
                            + "the `topics=` line as 'dead config', a Helm "
                            + "template rendering `topics={{ .Values.topics }}` "
                            + "to `topics=` (empty), or a per-environment "
                            + "override that drops the topic list. Fix: set "
                            + "`" + TOPICS + "=<comma-separated-list>` for an "
                            + "explicit subscription (recommended for "
                            + "production), or `" + TOPICS_REGEX
                            + "=<java-regex>` for a dynamic pattern "
                            + "subscription."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
