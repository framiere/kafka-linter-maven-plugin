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
 * Project-scoped rule. Fires when a Debezium connector
 * ({@code connector.class} starts with {@code io.debezium.connector.}) sets ANY
 * incremental-snapshot tuning key ({@code incremental.snapshot.chunk.size},
 * {@code incremental.snapshot.watermarking.strategy},
 * {@code incremental.snapshot.allow.schema.changes}) but configures NEITHER a
 * signal data-collection ({@code signal.data.collection}) NOR a signal Kafka
 * topic ({@code signal.kafka.topic}).
 *
 * <p>Tuning incremental snapshot without a signal channel means the feature
 * cannot be triggered at runtime — the snapshot will never run.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumIncrementalSnapshotWithoutSignalChannelRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEBEZIUM_CLASS_PREFIX = "io.debezium.connector.";

    private static final String INCREMENTAL_CHUNK_SIZE = "incremental.snapshot.chunk.size";
    private static final String INCREMENTAL_WATERMARKING_STRATEGY = "incremental.snapshot.watermarking.strategy";
    private static final String INCREMENTAL_ALLOW_SCHEMA_CHANGES = "incremental.snapshot.allow.schema.changes";

    private static final String SIGNAL_DATA_COLLECTION = "signal.data.collection";
    private static final String SIGNAL_KAFKA_TOPIC = "signal.kafka.topic";

    private final Severity severity;

    public ConnectDebeziumIncrementalSnapshotWithoutSignalChannelRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_INCREMENTAL_SNAPSHOT_WITHOUT_SIGNAL_CHANNEL;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!connectorClass.startsWith(DEBEZIUM_CLASS_PREFIX)) continue;

            List<String> presentTuning = new ArrayList<>();
            if (trimOrNull(p.getProperty(INCREMENTAL_CHUNK_SIZE)) != null) {
                presentTuning.add(INCREMENTAL_CHUNK_SIZE);
            }
            if (trimOrNull(p.getProperty(INCREMENTAL_WATERMARKING_STRATEGY)) != null) {
                presentTuning.add(INCREMENTAL_WATERMARKING_STRATEGY);
            }
            if (trimOrNull(p.getProperty(INCREMENTAL_ALLOW_SCHEMA_CHANGES)) != null) {
                presentTuning.add(INCREMENTAL_ALLOW_SCHEMA_CHANGES);
            }
            if (presentTuning.isEmpty()) continue;

            String signalDataCollection = trimOrNull(p.getProperty(SIGNAL_DATA_COLLECTION));
            String signalKafkaTopic = trimOrNull(p.getProperty(SIGNAL_KAFKA_TOPIC));
            if (signalDataCollection != null || signalKafkaTopic != null) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_INCREMENTAL_SNAPSHOT_WITHOUT_SIGNAL_CHANNEL, severity,
                    ctx.relativize(e.getKey()), "key:" + presentTuning.get(0), 0,
                    "Debezium connector (" + connectorClass + ") configures incremental-snapshot "
                            + "tuning key(s) " + String.join(", ", presentTuning) + " but defines "
                            + "NEITHER signal.data.collection (database-table signal channel) NOR "
                            + "signal.kafka.topic (Kafka-topic signal channel). Incremental "
                            + "snapshot is an ON-DEMAND feature triggered at runtime by writing an "
                            + "`execute-snapshot` signal to a configured signal channel; without "
                            + "any signal channel the runtime has no entry point to trigger the "
                            + "snapshot, so the chunking / watermarking / schema-change tuning is "
                            + "dead config — the connector starts cleanly and streams normally, "
                            + "but the operator's incremental-snapshot plan SILENTLY never "
                            + "executes. Typical symptoms: an operator inserts a row into the "
                            + "(non-existent) debezium_signal table or writes to the "
                            + "(unconfigured) signal Kafka topic and waits — nothing happens — "
                            + "tasks log nothing — the snapshot for the missing historical rows "
                            + "never runs. Fix: configure a signal channel: either "
                            + "signal.data.collection=<schema>.<table> (Debezium auto-creates "
                            + "AND watches the named table for execute-snapshot rows — Postgres "
                            + "/ MySQL / SQL Server / Oracle / Db2; e.g. "
                            + "signal.data.collection=public.debezium_signal) OR "
                            + "signal.kafka.topic=<topic> with "
                            + "signal.enabled.channels=source,kafka (Debezium consumes from the "
                            + "named Kafka topic for execute-snapshot records — recommended for "
                            + "multi-task / orchestrated deployments). Without one of these, the "
                            + "incremental-snapshot tuning has no effect. Related rule: "
                            + "CONNECT_DEBEZIUM_SNAPSHOT_MODE_NEVER (the inverse failure: "
                            + "skipping the INITIAL snapshot — different mechanism, same shape "
                            + "of 'connector runs but historical rows never emit')."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
