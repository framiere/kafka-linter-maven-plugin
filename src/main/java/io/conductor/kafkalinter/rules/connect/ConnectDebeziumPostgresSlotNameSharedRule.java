package io.conductor.kafkalinter.rules.connect;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Project-scoped rule. Fires when two or more Debezium Postgres
 * connectors declare the same {@code slot.name}.
 *
 * <p>Postgres logical-replication slots are exclusive: only one consumer
 * can attach to a slot at a time. Sharing a slot.name causes one
 * connector to fail-loop on startup, and on failover may cause silent
 * CDC event loss.
 *
 * <p>If {@code slot.name} is absent, Debezium's default ({@code debezium})
 * is used. Connectors that ALL omit slot.name therefore share the default
 * name and are equally susceptible — they are grouped together.
 *
 * <p>Emits one violation per file in a sharing group.
 */
public final class ConnectDebeziumPostgresSlotNameSharedRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String POSTGRES_CONNECTOR = "io.debezium.connector.postgresql.PostgresConnector";
    private static final String SLOT_NAME = "slot.name";
    private static final String DEFAULT_SLOT_NAME = "debezium";

    private final Severity severity;

    public ConnectDebeziumPostgresSlotNameSharedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_POSTGRES_SLOT_NAME_SHARED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        Map<String, List<SlotUsage>> bySlot = new LinkedHashMap<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!POSTGRES_CONNECTOR.equals(trimOrNull(p.getProperty(CONNECTOR_CLASS)))) continue;

            String configured = trimOrNull(p.getProperty(SLOT_NAME));
            String slot = configured != null ? configured : DEFAULT_SLOT_NAME;
            boolean usedDefault = configured == null;

            bySlot.computeIfAbsent(slot, s -> new ArrayList<>())
                    .add(new SlotUsage(e.getKey(), usedDefault, trimOrNull(p.getProperty("name"))));
        }

        List<Violation> out = new ArrayList<>();
        for (Map.Entry<String, List<SlotUsage>> group : bySlot.entrySet()) {
            List<SlotUsage> usages = group.getValue();
            if (usages.size() < 2) continue;

            for (SlotUsage u : usages) {
                StringBuilder peers = new StringBuilder();
                for (SlotUsage other : usages) {
                    if (other == u) continue;
                    if (peers.length() > 0) peers.append(", ");
                    peers.append(ctx.relativize(other.file));
                    if (other.connectorName != null) {
                        peers.append(" (name=").append(other.connectorName).append(")");
                    }
                }
                String source = u.usedDefault
                        ? "slot.name is not set, defaulting to '" + DEFAULT_SLOT_NAME + "'"
                        : "slot.name=" + group.getKey();
                out.add(new Violation(
                        RuleId.CONNECT_DEBEZIUM_POSTGRES_SLOT_NAME_SHARED, severity,
                        ctx.relativize(u.file), "key:" + SLOT_NAME, 0,
                        "Debezium Postgres connector "
                                + (u.connectorName != null ? "(name=" + u.connectorName + ") " : "")
                                + source + " — the same slot name is used by: " + peers
                                + ". Postgres logical-replication slots are EXCLUSIVE: only "
                                + "one consumer may attach at a time. Sharing a slot name "
                                + "causes one connector to fail-loop on startup with "
                                + "'replication slot \"" + group.getKey() + "\" is active "
                                + "for PID <N>', and on failover causes silent CDC event "
                                + "loss as connectors fight over the slot's WAL position. "
                                + "Give each Postgres Debezium connector a unique slot.name, "
                                + "e.g., slot.name=<connector-name> or "
                                + "slot.name=debezium_<database-server-name>."));
            }
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }

    private static final class SlotUsage {
        final Path file;
        final boolean usedDefault;
        final String connectorName;

        SlotUsage(Path file, boolean usedDefault, String connectorName) {
            this.file = file;
            this.usedDefault = usedDefault;
            this.connectorName = connectorName;
        }
    }
}
