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
 * Project-scoped rule. Fires when any Debezium connector
 * ({@code connector.class} starts with {@code io.debezium.connector.})
 * is configured with {@code key.converter=org.apache.kafka.connect.
 * storage.StringConverter}.
 *
 * <p>The Debezium key is a Struct of the source row's primary-key
 * columns. StringConverter calls {@code Object.toString()} on the
 * key Struct, producing a Java-debug-format string like
 * {@code Struct{id=42}}. This silently breaks log-compaction,
 * KTable joins, JDBC-sink pk.fields extraction, and Elasticsearch
 * document-ID inference.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumKeyConverterStringRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEBEZIUM_CLASS_PREFIX = "io.debezium.connector.";
    private static final String KEY_CONVERTER = "key.converter";
    private static final String STRING_CONVERTER =
            "org.apache.kafka.connect.storage.stringconverter";

    private final Severity severity;

    public ConnectDebeziumKeyConverterStringRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_KEY_CONVERTER_STRING;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!connectorClass.startsWith(DEBEZIUM_CLASS_PREFIX)) continue;

            String raw = trimOrNull(p.getProperty(KEY_CONVERTER));
            if (raw == null) continue;
            if (!STRING_CONVERTER.equals(raw.toLowerCase(Locale.ROOT))) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_KEY_CONVERTER_STRING, severity,
                    ctx.relativize(e.getKey()), "key:" + KEY_CONVERTER, 0,
                    "Debezium connector has key.converter=" + raw + " — the "
                            + "StringConverter serializes the primary-key Struct via "
                            + "Object.toString(). For a Debezium key (a Connect Struct "
                            + "of the source row's primary-key columns), Struct.toString() "
                            + "produces a Java-debug-format string like Struct{id=42} for "
                            + "a single-column key or Struct{tenant_id=7,order_id=12345} "
                            + "for a composite key — no schema, no escape grammar, no way "
                            + "for downstream consumers to parse the structure back. This "
                            + "silently breaks four production-critical Kafka behaviors: "
                            + "(a) log-compaction is corrupted — the toString form is "
                            + "field-ORDER-sensitive (Struct{a=1,b=2} and Struct{b=2,a=1} "
                            + "produce different bytes despite representing the same key); "
                            + "the connector's emit-time field iteration order can shift "
                            + "across Connect runtime versions or source-schema metadata "
                            + "refreshes, so the same logical key produces different bytes "
                            + "across restarts; compaction treats them as different keys "
                            + "and retains BOTH, silently breaking the one-record-per-key "
                            + "guarantee; (b) Kafka Streams KTable joins SILENTLY return "
                            + "empty — the String-encoded key bytes do not match any "
                            + "structured-converter peer (an Avro-encoded Struct{id=42} is "
                            + "a different byte sequence than the UTF-8 'Struct{id=42}'), "
                            + "so the join finds zero matches; the downstream enriched "
                            + "topic is empty; (c) the JDBC sink's pk.mode=record_key + "
                            + "pk.fields=id config FAILS — the sink's key-converter "
                            + "deserializes the key as a plain Java String, then tries to "
                            + "call keyStruct.get('id'), throwing IllegalArgumentException: "
                            + "cannot extract field id from value of type String; every "
                            + "record falls into the DLQ; (d) the Elasticsearch sink's "
                            + "key.ignore=false document-ID inference produces garbage — "
                            + "the Elasticsearch _id becomes the literal string "
                            + "'Struct{id=42}' (URL-unsafe characters; index bloat; a "
                            + "primary-key column rename like id -> customer_id silently "
                            + "doubles the document count because every doc gets a new _id "
                            + "and the old docs remain). Fix: use a structured converter — "
                            + "key.converter=io.confluent.connect.avro.AvroConverter "
                            + "(with key.converter.schema.registry.url) for "
                            + "Confluent-platform deployments, key.converter="
                            + "org.apache.kafka.connect.json.JsonConverter (with "
                            + "key.converter.schemas.enable=true for self-describing JSON "
                            + "or false for schemaless JSON) for JSON-based pipelines, or "
                            + "key.converter=io.confluent.connect.protobuf."
                            + "ProtobufConverter for Protobuf-based pipelines. The "
                            + "Debezium key Struct preserves perfectly through all "
                            + "structured converters. Common origins: (a) copy-paste of "
                            + "value-side anti-pattern to key-side ('I just used the same "
                            + "converter for both, for consistency'); (b) Helm chart with "
                            + "StringConverter default for key.converter and "
                            + "value.converter for 'simplicity'; (c) operator set "
                            + "value.converter to AvroConverter but forgot to override "
                            + "key.converter from the worker-level default."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
