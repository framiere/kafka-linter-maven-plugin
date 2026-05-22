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
 * {@code KafkaProtobufDeserializer} (or {@code KafkaProtobufSerde}) onto
 * {@code value.deserializer} / {@code key.deserializer} (or
 * {@code default.value.serde} / {@code default.key.serde}) but does NOT set the
 * matching {@code specific.protobuf.value.type} / {@code specific.protobuf.key.type}
 * FQCN property. The deserializer then returns a
 * {@code com.google.protobuf.DynamicMessage} instead of the generated typed
 * class — type safety is lost across every downstream typed access.
 *
 * <p>Detects both the plain consumer/Streams keys and the Spring Boot prefixed
 * forms.
 *
 * <p>Emits one violation per matching deserializer key per file.
 */
public final class SrProtobufValueTypeMissingRule implements ProjectScopedRule {

    private static final String PROTOBUF_DESER_FQCN = "io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer";
    private static final String PROTOBUF_SERDE_FQCN = "io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde";

    private final Severity severity;

    public SrProtobufValueTypeMissingRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SR_PROTOBUF_VALUE_TYPE_MISSING;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();

            // Plain consumer keys + plain Streams default-serde keys.
            checkOne(out, ctx, e.getKey(), p,
                    "value.deserializer", "specific.protobuf.value.type", "value");
            checkOne(out, ctx, e.getKey(), p,
                    "key.deserializer", "specific.protobuf.key.type", "key");
            checkOne(out, ctx, e.getKey(), p,
                    "default.value.serde", "specific.protobuf.value.type", "value");
            checkOne(out, ctx, e.getKey(), p,
                    "default.key.serde", "specific.protobuf.key.type", "key");

            // Spring Boot prefix.
            checkOne(out, ctx, e.getKey(), p,
                    "spring.kafka.consumer.value-deserializer",
                    "spring.kafka.properties.specific.protobuf.value.type", "value");
            checkOne(out, ctx, e.getKey(), p,
                    "spring.kafka.consumer.key-deserializer",
                    "spring.kafka.properties.specific.protobuf.key.type", "key");
            checkOne(out, ctx, e.getKey(), p,
                    "spring.kafka.streams.properties.default.value.serde",
                    "spring.kafka.streams.properties.specific.protobuf.value.type", "value");
            checkOne(out, ctx, e.getKey(), p,
                    "spring.kafka.streams.properties.default.key.serde",
                    "spring.kafka.streams.properties.specific.protobuf.key.type", "key");
        }
        return out;
    }

    private void checkOne(List<Violation> out, ProjectContext ctx, Path file, Properties p,
                          String deserKey, String typeKey, String axis) {
        String deser = trimOrNull(p.getProperty(deserKey));
        if (deser == null) return;
        if (!PROTOBUF_DESER_FQCN.equals(deser) && !PROTOBUF_SERDE_FQCN.equals(deser)) return;

        String typeFqcn = trimOrNull(p.getProperty(typeKey));
        if (typeFqcn != null) return;

        out.add(new Violation(
                RuleId.SR_PROTOBUF_VALUE_TYPE_MISSING, severity,
                ctx.relativize(file), "key:" + deserKey, 0,
                deserKey + "=" + deser + " is wired up but " + typeKey + " is unset. The "
                        + "Confluent Protobuf deserializer cannot recover the generated POJO "
                        + "type from its erased generic parameter `<T extends Message>`, so "
                        + "without " + typeKey + " it returns a "
                        + "`com.google.protobuf.DynamicMessage` instead of your generated "
                        + "Protobuf class. The Object reference survives — no immediate "
                        + "`ClassCastException` — but every typed accessor on the " + axis + " "
                        + "side (`record." + axis + "().getAccountId()`) fails to compile, OR "
                        + "(worse) developers work around it by casting to `DynamicMessage` and "
                        + "reading fields by name string "
                        + "(`((DynamicMessage) record." + axis + "())"
                        + ".getField(descriptor.findFieldByName(\"account_id\"))`), losing all "
                        + "compile-time type safety and creating code that silently breaks on "
                        + "future field renames. Streams topologies that join Protobuf topics "
                        + "end up with `DynamicMessage` on both sides — typed selector functions "
                        + "(`(left, right) -> left.getAccountId() + right.getMerchantId()`) "
                        + "cannot compile. Fix: add `" + typeKey
                        + "=com.example.proto.MyType` (the FQCN of the generated Protobuf "
                        + "class — NOT the `.proto` file's `option java_outer_classname=...` "
                        + "value) alongside the deserializer/serde config. For Kafka Streams "
                        + "with mixed Protobuf types, prefer explicit "
                        + "`KafkaProtobufSerde<T>` instances passed via `Consumed.with(...)` "
                        + "rather than relying on `default." + axis + ".serde`. Sibling rules: "
                        + "AVRO_SPECIFIC_READER_MISSING (the Avro counterpart, "
                        + "`specific.avro.reader=true`), SR_JSON_VALUE_TYPE_MISSING (the "
                        + "JSON Schema counterpart, `json." + axis + ".type=<FQCN>` — ERROR "
                        + "because the failure is immediate ClassCastException)."));
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
