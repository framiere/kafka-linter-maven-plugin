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
 * Project-scoped rule. Fires when a Debezium MySQL connector
 * ({@code connector.class=io.debezium.connector.mysql.MySqlConnector}) is
 * configured with {@code snapshot.locking.mode=none}.
 *
 * <p>With {@code none}, Debezium skips the global read lock entirely during
 * the snapshot phase: per-table {@code SELECT * FROM <table>} reads run
 * against a CONCURRENTLY-WRITTEN source database WITHOUT a single
 * point-in-time consistency boundary. Rows from earlier-snapshotted tables
 * reflect state at time T1; rows from later-snapshotted tables reflect
 * state at time T2 (possibly hours later). Foreign-key invariants across
 * tables may be VIOLATED in the snapshot output.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumMysqlSnapshotLockingModeNoneRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String MYSQL_CONNECTOR = "io.debezium.connector.mysql.MySqlConnector";
    private static final String SNAPSHOT_LOCKING_MODE = "snapshot.locking.mode";
    private static final String NONE = "none";

    private final Severity severity;

    public ConnectDebeziumMysqlSnapshotLockingModeNoneRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_MYSQL_SNAPSHOT_LOCKING_MODE_NONE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!MYSQL_CONNECTOR.equals(connectorClass)) continue;

            String raw = trimOrNull(p.getProperty(SNAPSHOT_LOCKING_MODE));
            if (raw == null) continue;
            if (!NONE.equals(raw.toLowerCase(Locale.ROOT))) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_MYSQL_SNAPSHOT_LOCKING_MODE_NONE, severity,
                    ctx.relativize(e.getKey()), "key:" + SNAPSHOT_LOCKING_MODE, 0,
                    "Debezium MySQL connector has snapshot.locking.mode=none — the "
                            + "connector SKIPS the global read lock (FLUSH TABLES WITH "
                            + "READ LOCK) during the initial snapshot phase. Per-table "
                            + "SELECT * FROM <table> reads then run against a "
                            + "CONCURRENTLY-WRITTEN source database WITHOUT a single "
                            + "point-in-time consistency boundary. Rows from "
                            + "earlier-snapshotted tables reflect state at time T1; "
                            + "rows from later-snapshotted tables reflect state at "
                            + "time T2 (possibly many hours later on a multi-billion-row "
                            + "schema). Foreign-key-related rows across tables may "
                            + "DISAGREE — an orders.customer_id captured at T1 may "
                            + "reference a customer row that was DELETED between T1 and "
                            + "T2 (and therefore never appears in the snapshot), "
                            + "producing a downstream KTable with an orders entry that "
                            + "joins to no customer entry. The streaming phase's "
                            + "takeover binlog position is AMBIGUOUSLY-DEFINED (the "
                            + "connector picks a position after the per-table reads "
                            + "complete, but events between snapshot-start and "
                            + "snapshot-end are split between snapshot-output and "
                            + "streaming-output non-deterministically), leading to "
                            + "either DOUBLE-COUNTED rows (snapshot+stream both emit "
                            + "the same row) or MISSED rows (rows inserted-and-deleted "
                            + "in the gap window). Fix: snapshot.locking.mode=minimal "
                            + "(the default — brief FTWRL just long enough to capture "
                            + "the binlog coordinate, then release; requires the "
                            + "connector's MySQL user to have RELOAD privilege), "
                            + "snapshot.locking.mode=extended (FTWRL for the full "
                            + "snapshot duration — perfectly consistent but BLOCKS ALL "
                            + "WRITES on the source for the duration; acceptable only "
                            + "in maintenance windows), or remove the key (default "
                            + "minimal applies). The 'none' mode is legitimate ONLY in "
                            + "narrow cases: a maintenance window where the source DB "
                            + "is known-quiet, OR a managed-MySQL environment where "
                            + "the RELOAD privilege is unavailable AND the operator has "
                            + "explicitly accepted the snapshot-inconsistency trade-off. "
                            + "In these deliberate cases, suppress this rule "
                            + "per-fixture with a comment documenting WHY 'none' is "
                            + "correct for that connector. Common misuse: "
                            + "dev-environment test (no concurrent writes — snapshot "
                            + "appeared consistent) copy-pasted to production (heavy "
                            + "concurrent writes — snapshot is now inconsistent), OR "
                            + "'temporary fix' for an FTWRL permission error that "
                            + "should have been resolved by granting RELOAD privilege "
                            + "instead of skipping the lock."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
