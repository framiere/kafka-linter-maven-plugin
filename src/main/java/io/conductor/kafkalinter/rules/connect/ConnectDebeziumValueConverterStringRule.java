package io.conductor.kafkalinter.rules.connect;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * Project-scoped rule. Fires when a Debezium connector
 * ({@code connector.class} starts with {@code io.debezium.connector.})
 * is configured with {@code value.converter=org.apache.kafka.connect.
 * storage.StringConverter}.
 *
 * <p>The StringConverter serializes a Connect Struct via
 * {@code Object.toString()} — for the Debezium envelope (a structured
 * Struct), the output is a Java-debug-format String that no downstream
 * consumer can parse. The CDC pipeline is silently broken: the topic
 * looks like it has data, but the data is unrecoverable text.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumValueConverterStringRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEBEZIUM_CLASS_PREFIX = "io.debezium.connector.";
    private static final String VALUE_CONVERTER = "value.converter";
    private static final String STRING_CONVERTER =
            "org.apache.kafka.connect.storage.stringconverter";

    private final Severity severity;

    public ConnectDebeziumValueConverterStringRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_VALUE_CONVERTER_STRING;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!connectorClass.startsWith(DEBEZIUM_CLASS_PREFIX)) continue;

            String raw = trimOrNull(p.getProperty(VALUE_CONVERTER));
            if (raw == null) continue;
            if (!STRING_CONVERTER.equals(raw.toLowerCase(Locale.ROOT))) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_VALUE_CONVERTER_STRING, severity,
                    ctx.relativize(e.getKey()), "key:" + VALUE_CONVERTER, 0,
                    "Debezium connector has value.converter=" + raw + " — the "
                            + "StringConverter serializes a Connect Struct via "
                            + "Object.toString(). For the Debezium envelope (a "
                            + "structured Struct containing nested before/after/source "
                            + "Struct fields plus op/ts_ms/transaction scalars), "
                            + "Struct.toString() produces a Java-debug-format text "
                            + "like Struct{before=Struct{id=42,email=alice@example.com,"
                            + "...},after=Struct{id=42,email=alice@new.com,...},"
                            + "source=Struct{...},op=u,ts_ms=...} — NOT JSON, NOT Avro, "
                            + "NOT Protobuf; no schema, no grammar (the = and , "
                            + "separators are not escaped if they appear in field "
                            + "values — a customer name like 'Smith, John' would "
                            + "corrupt the output); intended for log debugging, "
                            + "NEVER intended as a wire format. Every downstream "
                            + "consumer fails: Kafka Streams reads the value as a "
                            + "plain String (every map/filter/groupByKey operates "
                            + "on the raw debug text, never on the Struct fields); "
                            + "the JDBC sink's pk.fields=after.id config fails "
                            + "because the sink's value-converter cannot extract "
                            + "after.id from a String — IllegalArgumentException: "
                            + "cannot extract field after.id from value of type "
                            + "String; the Elasticsearch sink similarly cannot "
                            + "extract fields; KSQL CREATE STREAM fails because the "
                            + "schema-inference path cannot register a schema for "
                            + "raw Strings; Mirror Maker 2 replicates the raw "
                            + "Strings, where the same problems repeat. The CDC "
                            + "topic LOOKS like it has data (kafka-console-consumer "
                            + "shows Struct{...} lines), but the data is "
                            + "unrecoverable. Fix: use a structured converter — "
                            + "value.converter=io.confluent.connect.avro."
                            + "AvroConverter (with "
                            + "value.converter.schema.registry.url) for "
                            + "Confluent-platform deployments, value.converter="
                            + "org.apache.kafka.connect.json.JsonConverter (with "
                            + "value.converter.schemas.enable=true for "
                            + "self-describing JSON or false for schemaless JSON) "
                            + "for JSON-based pipelines, or "
                            + "value.converter=io.confluent.connect.protobuf."
                            + "ProtobufConverter for Protobuf-based pipelines. The "
                            + "Debezium envelope structure preserves perfectly "
                            + "through all structured converters. Common origins: "
                            + "(a) 'StringConverter is the simplest, no Schema "
                            + "Registry needed'; (b) copy-paste from a sink "
                            + "connector config that legitimately handles Strings "
                            + "(e.g., file-stream sink); (c) Helm chart with "
                            + "StringConverter default for 'simplicity'; (d) "
                            + "temporary workaround for a Schema Registry outage "
                            + "that was never reverted."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
