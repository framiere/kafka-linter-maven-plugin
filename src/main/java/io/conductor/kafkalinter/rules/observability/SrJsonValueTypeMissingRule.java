package io.conductor.kafkalinter.rules.observability;

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
 * Project-scoped rule. Fires when a properties file wires Confluent's
 * {@code KafkaJsonSchemaDeserializer} (or {@code KafkaJsonSchemaSerde}) onto
 * {@code value.deserializer} / {@code key.deserializer} (or
 * {@code default.value.serde} / {@code default.key.serde}) but does NOT set the
 * matching {@code json.value.type} / {@code json.key.type} FQCN property. The
 * deserializer then returns a {@code LinkedHashMap} instead of the typed POJO,
 * and the consumer's first typed access throws {@code ClassCastException}.
 *
 * <p>Detects both the plain consumer/Streams keys and the Spring Boot prefixed
 * forms ({@code spring.kafka.consumer.value-deserializer},
 * {@code spring.kafka.streams.properties.default.value.serde}, etc.).
 *
 * <p>Emits one violation per matching deserializer key per file.
 */
public final class SrJsonValueTypeMissingRule implements ProjectScopedRule {

    private static final String JSON_DESER_FQCN = "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer";
    private static final String JSON_SERDE_FQCN = "io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde";

    private final Severity severity;

    public SrJsonValueTypeMissingRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SR_JSON_VALUE_TYPE_MISSING;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();

            // Plain consumer keys + plain Streams default-serde keys.
            checkOne(out, ctx, e.getKey(), p, "value.deserializer", "json.value.type", "value");
            checkOne(out, ctx, e.getKey(), p, "key.deserializer", "json.key.type", "key");
            checkOne(out, ctx, e.getKey(), p, "default.value.serde", "json.value.type", "value");
            checkOne(out, ctx, e.getKey(), p, "default.key.serde", "json.key.type", "key");

            // Spring Boot prefix.
            checkOne(out, ctx, e.getKey(), p,
                    "spring.kafka.consumer.value-deserializer",
                    "spring.kafka.properties.json.value.type", "value");
            checkOne(out, ctx, e.getKey(), p,
                    "spring.kafka.consumer.key-deserializer",
                    "spring.kafka.properties.json.key.type", "key");
            checkOne(out, ctx, e.getKey(), p,
                    "spring.kafka.streams.properties.default.value.serde",
                    "spring.kafka.streams.properties.json.value.type", "value");
            checkOne(out, ctx, e.getKey(), p,
                    "spring.kafka.streams.properties.default.key.serde",
                    "spring.kafka.streams.properties.json.key.type", "key");
        }
        return out;
    }

    private void checkOne(List<Violation> out, ProjectContext ctx, Path file, Properties p,
                          String deserKey, String typeKey, String axis) {
        String deser = trimOrNull(p.getProperty(deserKey));
        if (deser == null) return;
        if (!JSON_DESER_FQCN.equals(deser) && !JSON_SERDE_FQCN.equals(deser)) return;

        String typeFqcn = trimOrNull(p.getProperty(typeKey));
        if (typeFqcn != null) return;

        out.add(new Violation(
                RuleId.SR_JSON_VALUE_TYPE_MISSING, severity,
                ctx.relativize(file), "key:" + deserKey, 0,
                deserKey + "=" + deser + " is wired up but " + typeKey + " is unset. The "
                        + "Confluent JSON-Schema deserializer cannot recover the runtime POJO "
                        + "type from its erased generic parameter `<T>`, so without "
                        + typeKey + " it returns a `java.util.LinkedHashMap<String, Object>` "
                        + "instead of the typed POJO. The consumer's first cast on the " + axis
                        + " (`((MyType) record." + axis + "()).getField()`) throws "
                        + "`ClassCastException: class java.util.LinkedHashMap cannot be cast to "
                        + "class com.example.MyType` at runtime — typically deep inside a Streams "
                        + "`mapValues` lambda or a Spring `@KafkaListener` method, far from the "
                        + "deserializer config that caused it. Unit tests using "
                        + "`MockSchemaRegistryClient` pass POJOs directly and miss this entirely; "
                        + "the bug surfaces on the first production poll. Fix: add `" + typeKey
                        + "=com.example.model.MyType` (the FQCN of the runtime POJO) alongside "
                        + "the deserializer/serde config. For Kafka Streams with mixed value "
                        + "types across topics, prefer explicit `KafkaJsonSchemaSerde<T>` "
                        + "instances passed via `Consumed.with(...)` / `Produced.with(...)` "
                        + "rather than relying on `default." + axis + ".serde`. Sibling rules: "
                        + "AVRO_SPECIFIC_READER_MISSING (the Avro counterpart, "
                        + "`specific.avro.reader=true`), SR_PROTOBUF_VALUE_TYPE_MISSING (the "
                        + "Protobuf counterpart, `specific.protobuf." + axis + ".type=<FQCN>`)."));
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
