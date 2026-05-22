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
 * {@code value.deserializer}) that does NOT set {@code bootstrap.servers}.
 * The kafka-clients ConsumerConfig REQUIRES this key — the framework throws
 * {@code ConfigException} synchronously at {@code new KafkaConsumer<>(props)}
 * construction, before any record is fetched. ERROR severity because the
 * framework rejects the config deterministically.
 *
 * <p>A file is "consumer-shaped" when it sets {@code key.deserializer} OR
 * {@code value.deserializer} as a TOP-LEVEL key (no framework prefix).
 * Excludes Spring Boot, Quarkus, Connect, and Streams configs.
 */
public final class ConsumerPropertiesBootstrapServersAbsentRule implements ProjectScopedRule {

    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    private final Severity severity;

    public ConsumerPropertiesBootstrapServersAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_PROPERTIES_BOOTSTRAP_SERVERS_ABSENT;
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
            if (isNonEmpty(p.getProperty(BOOTSTRAP_SERVERS))) continue;
            out.add(new Violation(
                    RuleId.CONSUMER_PROPERTIES_BOOTSTRAP_SERVERS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + BOOTSTRAP_SERVERS, 0,
                    "kafka-clients consumer .properties file (detected via "
                            + "top-level `" + shapeKey + "`) does NOT set `"
                            + BOOTSTRAP_SERVERS + "`. ConsumerConfig REQUIRES "
                            + "this key — `ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG` "
                            + "is declared with NO default and validator "
                            + "`NonEmptyListValidator`. `new "
                            + "KafkaConsumer<>(props)` throws ConfigException "
                            + "synchronously: 'Missing required configuration "
                            + "\"bootstrap.servers\" which has no default "
                            + "value'. The consumer fails before the first "
                            + "`poll()` call; in Kubernetes, the pod enters "
                            + "CrashLoopBackOff. Common trigger: partial-"
                            + "template copy, per-environment overlay missing, "
                            + "Helm template rendering `bootstrap.servers={{ "
                            + ".Values.x }}` to empty, env-var indirection "
                            + "where the loader doesn't resolve placeholders, "
                            + "or refactor removing the line as 'externalized'. "
                            + "Fix: set `" + BOOTSTRAP_SERVERS
                            + "=<host>:<port>[,<host>:<port>...]` (a comma-"
                            + "separated list of 2-3 broker addresses for "
                            + "startup redundancy). For env-var-driven "
                            + "configs, use the framework's placeholder "
                            + "syntax and ensure the loader resolves "
                            + "placeholders — plain Properties.load() does "
                            + "NOT."));
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
