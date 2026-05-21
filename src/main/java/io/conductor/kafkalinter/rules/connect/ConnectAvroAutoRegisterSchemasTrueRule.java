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
import java.util.Set;

/**
 * Project-scoped rule. Fires when a Kafka Connect connector .properties file
 * uses a Schema-Registry-aware Confluent converter ({@code AvroConverter},
 * {@code JsonSchemaConverter}, or {@code ProtobufConverter}) without
 * explicitly disabling {@code auto.register.schemas}.
 *
 * <p>The Confluent default is {@code auto.register.schemas=true}, which gives
 * connector clients write access to the Schema Registry and lets any schema
 * mismatch silently auto-register a new version, bypassing governance.
 * Confluent documentation explicitly states "we do not recommend using
 * auto schema registration in production."
 *
 * <p>Fires when the value is explicitly {@code true} OR when the key is
 * absent (default-true behavior). Emits one violation per (file,
 * converter-prefix) pair, so both key and value converter can produce
 * separate violations.
 */
public final class ConnectAvroAutoRegisterSchemasTrueRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String KEY_CONVERTER = "key.converter";
    private static final String VALUE_CONVERTER = "value.converter";
    private static final String AUTO_REGISTER_SUFFIX = ".auto.register.schemas";

    private static final Set<String> SR_AWARE_CONVERTERS = Set.of(
            "io.confluent.connect.avro.AvroConverter",
            "io.confluent.connect.json.JsonSchemaConverter",
            "io.confluent.connect.protobuf.ProtobufConverter");

    private final Severity severity;

    public ConnectAvroAutoRegisterSchemasTrueRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_AVRO_AUTO_REGISTER_SCHEMAS_TRUE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;

            checkConverter(p, KEY_CONVERTER, ctx, e.getKey(), out);
            checkConverter(p, VALUE_CONVERTER, ctx, e.getKey(), out);
        }
        return out;
    }

    private void checkConverter(Properties p, String converterKey,
            ProjectContext ctx, Path file, List<Violation> out) {
        String converter = trimOrNull(p.getProperty(converterKey));
        if (converter == null || !SR_AWARE_CONVERTERS.contains(converter)) return;

        String autoRegisterKey = converterKey + AUTO_REGISTER_SUFFIX;
        String raw = p.getProperty(autoRegisterKey);
        String trimmed = raw == null ? null : raw.trim();

        String issue;
        if (trimmed == null || trimmed.isEmpty()) {
            issue = "is not set — the Confluent default is true";
        } else if ("true".equalsIgnoreCase(trimmed)) {
            issue = "is explicitly true";
        } else {
            return;
        }

        out.add(new Violation(
                RuleId.CONNECT_AVRO_AUTO_REGISTER_SCHEMAS_TRUE, severity,
                ctx.relativize(file), "key:" + autoRegisterKey, 0,
                "Connect connector " + p.getProperty(CONNECTOR_CLASS)
                        + " uses Schema-Registry-aware converter " + converter
                        + " for " + converterKey + " but " + autoRegisterKey + " " + issue
                        + ". When auto-registration is enabled, the converter calls "
                        + "SchemaRegistryClient.register() for any new schema it sees, "
                        + "bypassing governance and letting schema typos, refactors, or "
                        + "stale builds silently register new versions. Confluent docs "
                        + "explicitly state 'we do not recommend using auto schema "
                        + "registration in production.' Set " + autoRegisterKey + "=false "
                        + "and manage schema registration through a separate workflow "
                        + "(kafka-schema-registry-maven-plugin in CI, `confluent "
                        + "schema-registry schema create`, Karapace registration step) "
                        + "with peer review on schema diffs."));
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
