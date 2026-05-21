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
 * has BOTH:
 * <ul>
 *   <li>a {@code connector.class} key (the canonical Connect-config fingerprint), AND</li>
 *   <li>either {@code key.converter} or {@code value.converter} set to a known
 *       Confluent schema-registry converter class
 *       ({@code io.confluent.connect.avro.AvroConverter},
 *       {@code io.confluent.connect.protobuf.ProtobufConverter},
 *       {@code io.confluent.connect.json.JsonSchemaConverter}), AND</li>
 *   <li>the matching {@code <prefix>.schema.registry.url} is missing or blank.</li>
 * </ul>
 *
 * <p>The Confluent registry converter family REQUIRES the
 * {@code schema.registry.url} configuration to construct its
 * {@code CachedSchemaRegistryClient}. Missing it either fails the task at
 * startup with a {@code ConfigException} or silently inherits a worker-level
 * default — which is rarely the registry the connector author intended.
 *
 * <p>Emits one violation per offending {@code key.converter} / {@code value.converter}
 * pair (so a file with the bug on both prefixes emits two violations).
 */
public final class ConnectSchemaRegistryConverterMissingUrlRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";

    private static final Set<String> REGISTRY_CONVERTERS = Set.of(
            "io.confluent.connect.avro.AvroConverter",
            "io.confluent.connect.protobuf.ProtobufConverter",
            "io.confluent.connect.json.JsonSchemaConverter");

    private static final String[] PREFIXES = { "key.converter", "value.converter" };

    private final Severity severity;

    public ConnectSchemaRegistryConverterMissingUrlRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_SCHEMA_REGISTRY_CONVERTER_MISSING_URL;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;

            for (String prefix : PREFIXES) {
                String converterClass = p.getProperty(prefix);
                if (converterClass == null) continue;
                converterClass = converterClass.trim();
                if (!REGISTRY_CONVERTERS.contains(converterClass)) continue;

                String urlKey = prefix + ".schema.registry.url";
                if (notBlank(p.getProperty(urlKey))) continue;

                out.add(new Violation(
                        RuleId.CONNECT_SCHEMA_REGISTRY_CONVERTER_MISSING_URL, severity,
                        ctx.relativize(e.getKey()), "key:" + prefix, 0,
                        "Connect connector " + p.getProperty(CONNECTOR_CLASS)
                                + " sets " + prefix + "=" + converterClass
                                + " (a Confluent schema-registry converter) but does NOT set "
                                + urlKey + " — the converter REQUIRES schema.registry.url to "
                                + "construct its CachedSchemaRegistryClient. The task will either "
                                + "fail at startup with ConfigException, or silently inherit a "
                                + "worker-level default registry URL that is likely the wrong one "
                                + "for this connector's environment. Add " + urlKey
                                + "=<your-schema-registry-url>."));
            }
        }
        return out;
    }

    private static boolean notBlank(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
