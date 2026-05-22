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
 * Project-scoped rule. Fires when a Debezium Postgres connector
 * (`io.debezium.connector.postgresql.PostgresConnector`) is configured
 * with {@code publication.autocreate.mode=filtered} AND
 * {@code heartbeat.interval.ms} is either absent (default = 0,
 * disabled) or explicitly 0.
 *
 * <p>The combo is the canonical Postgres-CDC WAL-retention bomb: with
 * a filtered publication, the slot only sees the included tables'
 * changes; when those tables are quiet but OTHER tables on the same
 * Postgres server are active, Debezium has no events to ACK; the slot's
 * {@code confirmed_flush_lsn} stays pinned; WAL accumulates
 * indefinitely; the data partition fills; the database crashes.
 * Heartbeats prevent this by periodically advancing the slot's
 * {@code confirmed_flush_lsn} via a no-op LSN-update message.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumPostgresFilteredPublicationHeartbeatDisabledRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String POSTGRES_CONNECTOR = "io.debezium.connector.postgresql.PostgresConnector";
    private static final String PUBLICATION_AUTOCREATE_MODE = "publication.autocreate.mode";
    private static final String FILTERED_MODE = "filtered";
    private static final String HEARTBEAT_INTERVAL_MS = "heartbeat.interval.ms";

    private final Severity severity;

    public ConnectDebeziumPostgresFilteredPublicationHeartbeatDisabledRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_POSTGRES_FILTERED_PUBLICATION_HEARTBEAT_DISABLED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!POSTGRES_CONNECTOR.equals(trimOrNull(p.getProperty(CONNECTOR_CLASS)))) continue;

            String mode = trimOrNull(p.getProperty(PUBLICATION_AUTOCREATE_MODE));
            if (mode == null) continue;
            if (!FILTERED_MODE.equalsIgnoreCase(mode)) continue;

            String heartbeatRaw = trimOrNull(p.getProperty(HEARTBEAT_INTERVAL_MS));
            boolean disabled;
            String source;
            if (heartbeatRaw == null) {
                disabled = true;
                source = "heartbeat.interval.ms is not set (default = 0, heartbeats DISABLED)";
            } else {
                Long parsed = parseLongOrNull(heartbeatRaw);
                if (parsed == null) continue;
                if (parsed != 0L) continue;
                disabled = true;
                source = "heartbeat.interval.ms=0 (heartbeats explicitly DISABLED)";
            }
            if (!disabled) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_POSTGRES_FILTERED_PUBLICATION_HEARTBEAT_DISABLED, severity,
                    ctx.relativize(e.getKey()), "key:" + HEARTBEAT_INTERVAL_MS, 0,
                    "Debezium Postgres connector has publication.autocreate.mode=filtered "
                            + "(server-side publication captures ONLY the connector's filtered "
                            + "tables) AND " + source + " — the canonical Postgres-CDC "
                            + "WAL-retention bomb. When the captured tables are quiet but OTHER "
                            + "tables on the same Postgres server are active, Debezium has no "
                            + "events to ACK; the replication slot's confirmed_flush_lsn stays "
                            + "PINNED; Postgres retains every WAL segment past the slot's "
                            + "restart_lsn; the pg_wal/ directory grows linearly with server "
                            + "activity; the data partition fills; the database crashes with "
                            + "'No space left on device'. Recovery requires DBA-side WAL "
                            + "surgery (data loss) or slot recreation (re-snapshot — hours of "
                            + "locked production tables). The fix is one line: set "
                            + "heartbeat.interval.ms to a positive value (conventionally "
                            + "5000-30000ms). Debezium will then issue periodic LSN advances "
                            + "via STANDBY_STATUS_UPDATE, keeping the slot's "
                            + "confirmed_flush_lsn current even when the captured tables are "
                            + "silent. The cost is negligible (one tiny heartbeat record per "
                            + "interval to a heartbeat topic that nobody reads)."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }

    private static Long parseLongOrNull(String v) {
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
