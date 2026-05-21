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
 * declares {@code key.converter} or {@code value.converter} as
 * {@code org.apache.kafka.connect.json.JsonConverter} but does NOT explicitly
 * set the corresponding {@code <converter>.schemas.enable} property.
 *
 * <p>JsonConverter defaults to {@code schemas.enable=true}, which requires
 * every record to be an envelope object of the form
 * {@code {"schema": ..., "payload": ...}}. Most upstream Kafka producers emit
 * plain JSON instead, causing every record to throw {@code DataException}.
 *
 * <p>This rule does NOT flag explicit {@code schemas.enable=true} or
 * {@code schemas.enable=false} — only the implicit default-true case.
 * Emits one violation per (file, converter-key) pair.
 */
public final class ConnectJsonConverterSchemasEnableUnsetRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String JSON_CONVERTER = "org.apache.kafka.connect.json.JsonConverter";
    private static final String[] CONVERTER_KEYS = {"key.converter", "value.converter"};

    private final Severity severity;

    public ConnectJsonConverterSchemasEnableUnsetRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_JSON_CONVERTER_SCHEMAS_ENABLE_UNSET;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;

            for (String converterKey : CONVERTER_KEYS) {
                String converterValue = trimOrNull(p.getProperty(converterKey));
                if (!JSON_CONVERTER.equals(converterValue)) continue;

                String schemasEnableKey = converterKey + ".schemas.enable";
                if (notBlank(p.getProperty(schemasEnableKey))) continue;

                out.add(new Violation(
                        RuleId.CONNECT_JSON_CONVERTER_SCHEMAS_ENABLE_UNSET, severity,
                        ctx.relativize(e.getKey()), "key:" + converterKey, 0,
                        "Connect connector " + p.getProperty(CONNECTOR_CLASS)
                                + " declares " + converterKey + "=" + JSON_CONVERTER
                                + " but does NOT explicitly set " + schemasEnableKey
                                + " — JsonConverter defaults to schemas.enable=true, requiring "
                                + "every record to be an envelope object {\"schema\":...,"
                                + "\"payload\":...}. If the upstream producer emits plain JSON "
                                + "(the common case for microservices using JsonSerializer or "
                                + "Kafka Streams), EVERY record throws DataException and either "
                                + "stops the task (errors.tolerance=none) or pollutes the DLQ "
                                + "(errors.tolerance=all). Set " + schemasEnableKey + "=false "
                                + "for plain JSON, or " + schemasEnableKey + "=true for envelope "
                                + "JSON — but make the choice EXPLICIT; do not rely on the "
                                + "default."));
            }
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
