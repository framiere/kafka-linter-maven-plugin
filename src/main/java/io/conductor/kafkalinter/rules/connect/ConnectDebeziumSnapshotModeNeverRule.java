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
import java.util.Set;

/**
 * Project-scoped rule. Fires when ANY Debezium connector
 * (`connector.class` starts with `io.debezium.connector.`) is configured
 * with {@code snapshot.mode=never} (canonical Debezium 2.x name) or its
 * alias {@code snapshot.mode=no_data} (alias accepted in Debezium 1.x/2.x).
 *
 * <p>With these values the connector completely skips the initial
 * snapshot phase: on first start it goes directly to streaming mode and
 * starts capturing changes from the current source-log position. Every
 * row that EXISTED at the moment of connector start is NEVER emitted to
 * Kafka — silent data loss for every downstream consumer.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumSnapshotModeNeverRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEBEZIUM_CLASS_PREFIX = "io.debezium.connector.";
    private static final String SNAPSHOT_MODE = "snapshot.mode";

    private static final Set<String> NEVER_VALUES = Set.of("never", "no_data");

    private final Severity severity;

    public ConnectDebeziumSnapshotModeNeverRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_SNAPSHOT_MODE_NEVER;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!connectorClass.startsWith(DEBEZIUM_CLASS_PREFIX)) continue;

            String raw = trimOrNull(p.getProperty(SNAPSHOT_MODE));
            if (raw == null) continue;
            String value = raw.toLowerCase(Locale.ROOT);
            if (!NEVER_VALUES.contains(value)) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_SNAPSHOT_MODE_NEVER, severity,
                    ctx.relativize(e.getKey()), "key:" + SNAPSHOT_MODE, 0,
                    "Debezium connector has snapshot.mode=" + raw + " — the connector "
                            + "COMPLETELY SKIPS the initial snapshot phase. On first "
                            + "start, instead of issuing SELECT * FROM <table> for every "
                            + "captured table and emitting each existing row as a "
                            + "Kafka record with op=r in the Debezium envelope, the "
                            + "connector goes DIRECTLY to streaming mode and starts "
                            + "capturing changes from the CURRENT source-log position "
                            + "(the source MySQL binlog head / current Postgres WAL LSN "
                            + "/ current Oracle SCN / current DB2 IBMSNAP_COMMITSEQ). "
                            + "Every row that EXISTED in the captured tables at the "
                            + "moment of connector start is NEVER emitted to Kafka. "
                            + "Downstream consumers (Kafka Streams KTables, KSQL "
                            + "TABLEs, JDBC sinks, log-compacted topic consumers, "
                            + "analytics warehouses) are MISSING every pre-existing "
                            + "key — they see only the delta of INSERT/UPDATE/DELETE "
                            + "operations that happened AFTER connector start. The "
                            + "connector runs cleanly, the streaming is healthy, the "
                            + "REST API reports RUNNING — the only symptom is 'we have "
                            + "less data than expected', which typically takes weeks "
                            + "to diagnose. Fix: set snapshot.mode=initial (default — "
                            + "snapshot once then stream, the correct choice for "
                            + "nearly all deployments), snapshot.mode=when_needed "
                            + "(snapshot if no offsets exist, otherwise stream — "
                            + "equivalent to initial on first start), or "
                            + "snapshot.mode=initial_only (snapshot then stop — for "
                            + "bulk-load scenarios with a separate stream-only "
                            + "connector). The 'never' / 'no_data' mode is legitimate "
                            + "ONLY in narrow cases: REPLACING a prior connector that "
                            + "already snapshotted (offsets carried forward, "
                            + "re-snapshot must be avoided), the captured tables are "
                            + "KNOWN-EMPTY at start time, or the historical data is "
                            + "being delivered through a parallel pipeline. In these "
                            + "deliberate cases, suppress this rule per-fixture with "
                            + "a comment documenting WHY 'never' is correct for that "
                            + "connector."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
