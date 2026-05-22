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
 * Project-scoped rule. Fires on a kafka-clients consumer .properties
 * file that does NOT set {@code isolation.level}. The kafka-clients
 * default is {@code read_uncommitted}, which is silently catastrophic
 * for consumers of transactional (EOS) topics — they see records
 * from in-flight and aborted transactions. INFO severity because the
 * default is workload-dependent.
 *
 * <p>A file is "consumer-shaped" when it sets {@code key.deserializer}
 * OR {@code value.deserializer} as a TOP-LEVEL key. Excludes Connect
 * and Streams configs.
 */
public final class ConsumerPropertiesIsolationLevelAbsentRule implements ProjectScopedRule {

    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String ISOLATION_LEVEL = "isolation.level";

    private final Severity severity;

    public ConsumerPropertiesIsolationLevelAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_PROPERTIES_ISOLATION_LEVEL_ABSENT;
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
            if (isNonEmpty(p.getProperty(ISOLATION_LEVEL))) continue;
            out.add(new Violation(
                    RuleId.CONSUMER_PROPERTIES_ISOLATION_LEVEL_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + ISOLATION_LEVEL, 0,
                    "kafka-clients consumer .properties file (detected via top-level `"
                            + shapeKey + "`) does NOT set `" + ISOLATION_LEVEL
                            + "`. kafka-clients defaults `" + ISOLATION_LEVEL
                            + "` to `read_uncommitted` — the consumer sees ALL "
                            + "records up to the high watermark, including "
                            + "records from in-flight (not-yet-committed) and "
                            + "aborted transactions. This is silently "
                            + "catastrophic for consumers of transactional "
                            + "(EOS) topics: aborted records leak downstream, "
                            + "in-flight records are visible before commit, and "
                            + "auto-committed offsets may point past records "
                            + "that subsequent aborts erase from the topic's "
                            + "logical history. Kafka Streams' EOS mode sets "
                            + "`isolation.level=read_committed` automatically; "
                            + "plain kafka-clients consumers MUST opt in. Fix: "
                            + "set `" + ISOLATION_LEVEL + "=read_committed` for "
                            + "consumers of transactional topics (the safe "
                            + "default for EOS-aware pipelines), OR set `"
                            + ISOLATION_LEVEL + "=read_uncommitted` EXPLICITLY "
                            + "to document that uncommitted reads were "
                            + "deliberately chosen (e.g., low-latency monitoring "
                            + "of in-flight transactions)."));
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
