package io.conductor.kafkalinter.rules.streams;

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
 * Project-scoped rule. Fires on a Kafka Streams .properties file (identified
 * by top-level {@code application.id}) that does NOT set
 * {@code bootstrap.servers}. Kafka Streams REQUIRES this key — the framework
 * throws {@code ConfigException} synchronously at {@code new StreamsConfig(props)}
 * construction, before any topology is built. ERROR severity because the
 * framework rejects the config deterministically.
 *
 * <p>A file is "streams-shaped" when it sets {@code application.id} as a
 * TOP-LEVEL key. Excludes Connect configs.
 */
public final class StreamsPropertiesBootstrapServersAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    private final Severity severity;

    public StreamsPropertiesBootstrapServersAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_BOOTSTRAP_SERVERS_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(BOOTSTRAP_SERVERS))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_BOOTSTRAP_SERVERS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + BOOTSTRAP_SERVERS, 0,
                    "Kafka Streams .properties file (detected via top-level `"
                            + APPLICATION_ID + "`) does NOT set `"
                            + BOOTSTRAP_SERVERS + "`. Streams REQUIRES this "
                            + "key — `StreamsConfig.BOOTSTRAP_SERVERS_CONFIG` "
                            + "is declared with NO default and validator "
                            + "`NonEmptyListValidator`. `new "
                            + "StreamsConfig(props)` throws ConfigException "
                            + "synchronously: 'Missing required configuration "
                            + "\"bootstrap.servers\" which has no default "
                            + "value'. The Streams application crashes at "
                            + "startup before the topology is wired; in "
                            + "Kubernetes, the pod enters CrashLoopBackOff. "
                            + "Common trigger: partial-template copy, per-"
                            + "environment overlay missing, Helm template "
                            + "rendering `bootstrap.servers={{ .Values.x }}` "
                            + "to empty, env-var indirection where the "
                            + "loader doesn't resolve placeholders, or "
                            + "refactor removing the line as 'externalized'. "
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

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
