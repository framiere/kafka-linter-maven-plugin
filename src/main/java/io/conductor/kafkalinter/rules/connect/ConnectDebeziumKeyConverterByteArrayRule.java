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
 * Project-scoped rule. Fires when a .properties file declares a Debezium
 * connector (any class under {@code io.debezium.connector.}) AND sets
 * {@code key.converter} to
 * {@code org.apache.kafka.connect.converters.ByteArrayConverter}.
 *
 * <p>Every Debezium connector emits SourceRecord keys as Kafka Connect
 * {@code Struct} instances containing the source row's primary-key columns.
 * {@code ByteArrayConverter#fromConnectData} is a single {@code (byte[]) value}
 * cast — it throws {@code ClassCastException} on the first record because a
 * {@code Struct} cannot be cast to {@code byte[]}. The task fails immediately;
 * no events reach Kafka. There is no legitimate Debezium configuration in
 * which this pairing is correct.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumKeyConverterByteArrayRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEBEZIUM_CLASS_PREFIX = "io.debezium.connector.";
    private static final String KEY_CONVERTER = "key.converter";
    private static final String BYTE_ARRAY_CONVERTER =
            "org.apache.kafka.connect.converters.ByteArrayConverter";

    private final Severity severity;

    public ConnectDebeziumKeyConverterByteArrayRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_KEY_CONVERTER_BYTE_ARRAY;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!connectorClass.startsWith(DEBEZIUM_CLASS_PREFIX)) continue;
            String keyConverter = trimOrNull(p.getProperty(KEY_CONVERTER));
            if (keyConverter == null) continue;
            if (!BYTE_ARRAY_CONVERTER.equals(keyConverter)) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_KEY_CONVERTER_BYTE_ARRAY, severity,
                    ctx.relativize(e.getKey()), "key:" + KEY_CONVERTER, 0,
                    "Debezium connector " + connectorClass
                            + " sets key.converter=org.apache.kafka.connect.converters."
                            + "ByteArrayConverter — this is STRUCTURALLY INVALID. Debezium "
                            + "emits SourceRecord keys as a Kafka Connect Struct containing "
                            + "the source row's primary-key columns; ByteArrayConverter's "
                            + "fromConnectData() is a hard `(byte[]) value` cast with no "
                            + "Struct-handling branch — it throws ClassCastException on the "
                            + "FIRST record. The task transitions to FAILED, no change-events "
                            + "reach Kafka. There is no Debezium configuration, SMT chain, or "
                            + "downstream sink in which ByteArrayConverter is correct for the "
                            + "key position. Replace with a Debezium-compatible converter: "
                            + "io.confluent.connect.avro.AvroConverter (production-default, "
                            + "with Schema Registry), org.apache.kafka.connect.json.JsonConverter "
                            + "(Schema-Registry-less, set schemas.enable explicitly), "
                            + "io.confluent.connect.protobuf.ProtobufConverter, or "
                            + "io.confluent.connect.json.JsonSchemaConverter."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
