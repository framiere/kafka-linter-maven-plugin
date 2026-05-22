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
 * Project-scoped rule. Fires on a Kafka Streams .properties file that
 * does NOT set {@code default.value.serde}. The Streams default is
 * {@code null}; any DSL operator without an explicit Serde resolves
 * the default at topology-build or topology-start time and throws
 * {@code ConfigException} when the default is null. Legitimate only
 * for topologies that pass an explicit Serde to EVERY operator.
 * WARNING severity.
 *
 * <p>A file is "streams-shaped" when it sets {@code application.id}
 * as a TOP-LEVEL key. Excludes Connect configs.
 */
public final class StreamsPropertiesDefaultValueSerdeAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEFAULT_VALUE_SERDE = "default.value.serde";

    private final Severity severity;

    public StreamsPropertiesDefaultValueSerdeAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_DEFAULT_VALUE_SERDE_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(DEFAULT_VALUE_SERDE))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_DEFAULT_VALUE_SERDE_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + DEFAULT_VALUE_SERDE, 0,
                    "Kafka Streams .properties file (detected via top-level `"
                            + APPLICATION_ID + "`) does NOT set `"
                            + DEFAULT_VALUE_SERDE + "`. StreamsConfig "
                            + "declares `DEFAULT_VALUE_SERDE_CLASS_CONFIG` "
                            + "with default `null` and importance MEDIUM. "
                            + "The parse step does NOT reject the absence — "
                            + "the StreamsConfig object is constructed "
                            + "without complaint. The default is consulted "
                            + "at topology-build / topology-start time by "
                            + "EVERY DSL operator that does NOT pass an "
                            + "explicit Serde via `Consumed.with(...)`, "
                            + "`Grouped.with(...)`, `Materialized.with(...)`, "
                            + "`Produced.with(...)`, or `Joined.with(...)`. "
                            + "Affected operators: `stream(topic)`, "
                            + "`table(topic)`, `globalTable(topic)`, "
                            + "`groupByKey()` → `aggregate(...)`, `aggregate("
                            + "...)`, `join(...)`, `to(topic)`, implicit "
                            + "repartition triggers, internal changelog "
                            + "topics. With `null`, the framework throws "
                            + "`ConfigException: Please specify a default "
                            + "value serde through configured `default.value."
                            + "serde` or through Materialized/Consumed/"
                            + "Grouped/Produced/Joined.with(...)`. The "
                            + "exception fires at `Topology.build()` or "
                            + "`KafkaStreams.start()` — production crashes at "
                            + "startup with a clear exception pointing at the "
                            + "missing default. Common bug shape: partial-"
                            + "template copy sets `default.key.serde` but "
                            + "misses `default.value.serde`; deploy crashes; "
                            + "investigation takes 5-15 minutes because the "
                            + "exception is at build time, not parse time. "
                            + "Fix: set `" + DEFAULT_VALUE_SERDE + "=org."
                            + "apache.kafka.common.serialization."
                            + "Serdes$StringSerde` (for string-valued "
                            + "topologies) or `" + DEFAULT_VALUE_SERDE
                            + "=io.confluent.kafka.streams.serdes.avro."
                            + "SpecificAvroSerde` (for Schema Registry / "
                            + "Avro topologies). For multi-typed topologies, "
                            + "set the default to the most-common value type "
                            + "and override per-operator. For all-explicit-"
                            + "Serdes topologies, document the choice with a "
                            + "comment `# " + DEFAULT_VALUE_SERDE
                            + " intentionally omitted — every operator "
                            + "passes explicit Serdes`."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
