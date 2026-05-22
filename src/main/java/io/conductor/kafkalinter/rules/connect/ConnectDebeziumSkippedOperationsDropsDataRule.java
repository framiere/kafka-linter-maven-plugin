package io.conductor.kafkalinter.rules.connect;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Project-scoped rule. Fires when a Debezium connector
 * ({@code connector.class} starts with {@code io.debezium.connector.})
 * sets {@code skipped.operations} to a CSV value that contains any of
 * {@code c} (CREATE/INSERT), {@code u} (UPDATE), or {@code d} (DELETE).
 *
 * <p>The {@code skipped.operations} config silently drops the listed
 * CDC operation types before they reach Kafka. The safe values are
 * {@code t} (TRUNCATE — Debezium 2.x default; TRUNCATEs have no row
 * payload) and {@code r} (READ — initial-snapshot events). Including
 * {@code c}, {@code u}, or {@code d} drops actual row-change data and
 * has no legitimate CDC use case.
 *
 * <p>Emits one violation per offending file, listing the specific
 * offending operation tokens found.
 */
public final class ConnectDebeziumSkippedOperationsDropsDataRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEBEZIUM_CONNECTOR_CLASS_PREFIX = "io.debezium.connector.";
    private static final String SKIPPED_OPERATIONS = "skipped.operations";
    private static final Set<String> DATA_DROPPING_OPS = Set.of("c", "u", "d");

    private final Severity severity;

    public ConnectDebeziumSkippedOperationsDropsDataRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_SKIPPED_OPERATIONS_DROPS_DATA;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null || !connectorClass.startsWith(DEBEZIUM_CONNECTOR_CLASS_PREFIX)) continue;

            String raw = trimOrNull(p.getProperty(SKIPPED_OPERATIONS));
            if (raw == null) continue;

            Set<String> offending = new LinkedHashSet<>();
            for (String tok : raw.split(",")) {
                String t = tok.trim().toLowerCase(Locale.ROOT);
                if (DATA_DROPPING_OPS.contains(t)) offending.add(t);
            }
            if (offending.isEmpty()) continue;

            String descs = describe(offending);
            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_SKIPPED_OPERATIONS_DROPS_DATA, severity,
                    ctx.relativize(e.getKey()), "key:" + SKIPPED_OPERATIONS, 0,
                    "Debezium connector (" + connectorClass + ") has skipped.operations=" + raw
                            + " which includes data-dropping operation token(s): " + descs
                            + ". The skipped.operations config silently DROPS the listed CDC operation "
                            + "types before they reach Kafka — no log entry, no metric, no DLQ. The safe "
                            + "values are `t` (TRUNCATE — Debezium 2.x default; TRUNCATEs have no row "
                            + "payload and most downstreams cannot model them) and `r` (READ — initial-"
                            + "snapshot events; defensible when the connector is intended only for "
                            + "ongoing change-capture). Including `c` (INSERT), `u` (UPDATE), or `d` "
                            + "(DELETE) is structurally incompatible with the purpose of CDC: an "
                            + "INSERT-skipping connector emits zero records for new source rows; an "
                            + "UPDATE-skipping connector emits stale state forever; a DELETE-skipping "
                            + "connector causes downstream and source to diverge with no signal. "
                            + "Operators who want operation-level filtering should implement it via "
                            + "downstream Single-Message-Transforms (visible in the topology, easy to "
                            + "audit) or consumer-side logic, NOT by configuring the connector to "
                            + "silently drop entire operation classes. Fix: remove the offending "
                            + "operation code(s) from skipped.operations (keep `t` and/or `r` if needed, "
                            + "or remove the line entirely to get the default `t`)."));
        }
        return out;
    }

    private static String describe(Set<String> ops) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String op : ops) {
            if (!first) sb.append(", ");
            first = false;
            switch (op) {
                case "c" -> sb.append("`c` (INSERT)");
                case "u" -> sb.append("`u` (UPDATE)");
                case "d" -> sb.append("`d` (DELETE)");
                default -> sb.append('`').append(op).append('`');
            }
        }
        return sb.toString();
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
