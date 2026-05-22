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
 * {@code value.deserializer}) that does NOT set {@code group.id}.
 *
 * <p>Without {@code group.id}, the consumer object builds fine but
 * {@code subscribe()}, {@code commitSync()}, and {@code commitAsync()}
 * throw {@code InvalidGroupIdException} at first use. Only legal for
 * {@code assign()}-based consumers with external offset storage — a rare
 * advanced pattern. WARNING severity because that pattern is legitimate;
 * the operator should document the choice with a comment.
 *
 * <p>A file is "consumer-shaped" when it sets {@code key.deserializer} OR
 * {@code value.deserializer} as a TOP-LEVEL key (no framework prefix).
 * Excludes Spring Boot, Quarkus, Connect, and Streams configs.
 */
public final class ConsumerPropertiesGroupIdAbsentRule implements ProjectScopedRule {

    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String GROUP_ID = "group.id";

    private final Severity severity;

    public ConsumerPropertiesGroupIdAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_PROPERTIES_GROUP_ID_ABSENT;
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
            if (isNonEmpty(p.getProperty(GROUP_ID))) continue;
            out.add(new Violation(
                    RuleId.CONSUMER_PROPERTIES_GROUP_ID_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + GROUP_ID, 0,
                    "kafka-clients consumer .properties file (detected via "
                            + "top-level `" + shapeKey + "`) does NOT set `"
                            + GROUP_ID + "`. ConsumerConfig declares "
                            + "`GROUP_ID_CONFIG` with default `null` and "
                            + "importance HIGH. Without it, the consumer "
                            + "object builds fine but `subscribe(topics)`, "
                            + "`commitSync()`, `commitAsync()`, and "
                            + "`committed(partitions)` ALL throw "
                            + "`InvalidGroupIdException: To use the group "
                            + "management or offset commit APIs, you must "
                            + "provide a valid group.id in the consumer "
                            + "configuration` at first call. Failure surfaces "
                            + "at runtime under load — worse than `bootstrap."
                            + "servers` absence (which fails at construction). "
                            + "The ONLY legal usage without `group.id`: "
                            + "`assign(partitions)` (manual partition "
                            + "assignment) + external offset storage. That "
                            + "pattern is rare; the overwhelming majority of "
                            + "consumer .properties files are destined for "
                            + "`subscribe()`-based consumers and need "
                            + "`group.id`. Common bug shape: partial-template "
                            + "copy misses the group.id line; deploy "
                            + "'succeeds'; pod crashes at first subscribe(). "
                            + "Fix: set `" + GROUP_ID + "=<service-name>-"
                            + "<role>` (e.g., `payments-fraud-screening`, "
                            + "`orders-export-to-warehouse`). Name groups by "
                            + "BUSINESS PURPOSE, not technology — meaningful "
                            + "names show up in `__consumer_offsets` and lag "
                            + "dashboards. For assign()-based consumers, "
                            + "document the choice with a comment `# group.id "
                            + "intentionally omitted — uses assign() with "
                            + "external offset storage`."));
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
