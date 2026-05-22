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
 * {@code value.serializer}) that does NOT set {@code bootstrap.servers}.
 * The kafka-clients ProducerConfig REQUIRES this key — the framework throws
 * {@code ConfigException} synchronously at {@code new KafkaProducer<>(props)}
 * construction, before any record is sent. ERROR severity because the
 * framework rejects the config deterministically.
 *
 * <p>A file is "producer-shaped" when it sets {@code key.serializer} OR
 * {@code value.serializer} as a TOP-LEVEL key (no framework prefix).
 * Excludes Spring Boot, Quarkus, Connect, and Streams configs.
 */
public final class ProducerPropertiesBootstrapServersAbsentRule implements ProjectScopedRule {

    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    private final Severity severity;

    public ProducerPropertiesBootstrapServersAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_PROPERTIES_BOOTSTRAP_SERVERS_ABSENT;
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
            if (isNonEmpty(p.getProperty(BOOTSTRAP_SERVERS))) continue;
            out.add(new Violation(
                    RuleId.PRODUCER_PROPERTIES_BOOTSTRAP_SERVERS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + BOOTSTRAP_SERVERS, 0,
                    "kafka-clients producer .properties file (detected via "
                            + "top-level `" + shapeKey + "`) does NOT set `"
                            + BOOTSTRAP_SERVERS + "`. ProducerConfig REQUIRES "
                            + "this key — `ProducerConfig.BOOTSTRAP_SERVERS_CONFIG` "
                            + "is declared with NO default and validator "
                            + "`NonEmptyListValidator`. `new "
                            + "KafkaProducer<>(props)` throws ConfigException "
                            + "synchronously: 'Missing required configuration "
                            + "\"bootstrap.servers\" which has no default "
                            + "value'. The producer fails before the first "
                            + "`send()` call; in Kubernetes, the pod enters "
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

    private static String producerShapeKey(Properties p) {
        if (isNonEmpty(p.getProperty(KEY_SERIALIZER))) return KEY_SERIALIZER;
        if (isNonEmpty(p.getProperty(VALUE_SERIALIZER))) return VALUE_SERIALIZER;
        return null;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
