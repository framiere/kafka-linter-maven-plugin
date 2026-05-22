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
 * Project-scoped rule. Fires when a Debezium connector (any class under
 * {@code io.debezium.connector.}) sets {@code tombstones.on.delete=false}.
 *
 * <p>By default Debezium emits a Kafka tombstone (key with null value) after
 * each row-deletion change event; this tombstone is what log-compaction-aware
 * downstream consumers (compacted topics, Kafka Streams KTable, KSQL TABLE,
 * Confluent JDBC sink with {@code delete.enabled=true}, Mirror Maker 2)
 * require to evict deleted keys from state. Disabling tombstones produces a
 * cascade of silent downstream failures: compacted topics leak disk forever,
 * KTables retain ghost rows for deleted keys, JDBC sinks silently ignore
 * deletes, cross-cluster replication loses delete semantics.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumTombstonesOnDeleteDisabledRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEBEZIUM_CLASS_PREFIX = "io.debezium.connector.";
    private static final String TOMBSTONES_ON_DELETE = "tombstones.on.delete";
    private static final String FALSE_VALUE = "false";

    private final Severity severity;

    public ConnectDebeziumTombstonesOnDeleteDisabledRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_TOMBSTONES_ON_DELETE_DISABLED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!connectorClass.startsWith(DEBEZIUM_CLASS_PREFIX)) continue;
            String value = trimOrNull(p.getProperty(TOMBSTONES_ON_DELETE));
            if (value == null) continue;
            if (!FALSE_VALUE.equalsIgnoreCase(value)) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_TOMBSTONES_ON_DELETE_DISABLED, severity,
                    ctx.relativize(e.getKey()), "key:" + TOMBSTONES_ON_DELETE, 0,
                    "Debezium connector " + connectorClass
                            + " sets tombstones.on.delete=false — SUPPRESSES the Kafka "
                            + "tombstone (key, null value) that downstream log-compaction-aware "
                            + "systems require to evict deleted keys from state. The cascade: "
                            + "(a) compacted topics leak disk forever (the broker's log-cleaner "
                            + "has no signal to compact deleted keys away — the op=d envelope "
                            + "is just another value to it); (b) Kafka Streams KTables retain "
                            + "ghost rows for deleted keys (stream-table joins return "
                            + "logically-incorrect results — e.g., an order is enriched with a "
                            + "GDPR-deleted customer's address); (c) Confluent JDBC sink with "
                            + "delete.enabled=true SILENTLY IGNORES deletes (the sink's delete "
                            + "handler is keyed on tombstones, not on op=d envelope semantics) "
                            + "and the downstream warehouse accumulates ghost rows; "
                            + "(d) Mirror Maker 2 cross-cluster replication of compacted "
                            + "topics loses delete semantics. The fix is to remove the key "
                            + "(default tombstones.on.delete=true) or set it to true. The "
                            + "tombstone is a tiny record and log-compaction reclaims it "
                            + "within delete.retention.ms (default 24h) — there is no "
                            + "throughput or disk cost worth losing delete semantics for."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
