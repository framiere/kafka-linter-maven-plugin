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
 * Project-scoped rule. Fires when a Debezium connector
 * ({@code connector.class} starts with {@code io.debezium.connector.})
 * sets {@code event.processing.failure.handling.mode} to {@code skip} or
 * {@code warn}.
 *
 * <p>The {@code skip} and {@code warn} modes are silent-data-loss switches:
 * when Debezium fails to process a single source-database event (parser
 * stumble, schema-cache mismatch, decoder failure, corrupted log entry),
 * the connector advances the source offset past the failing event. The
 * source-database's log-retention mechanism eventually prunes the log
 * segment, and the event is permanently lost.
 *
 * <p>The default {@code fail} value is the correct production setting.
 * The rule fires only on the explicit {@code skip} or {@code warn} values;
 * the absent case is silent (default is correct).
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumEventProcessingFailureHandlingModeSkipOrWarnRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEBEZIUM_CONNECTOR_CLASS_PREFIX = "io.debezium.connector.";
    private static final String EVENT_PROCESSING_FAILURE_HANDLING_MODE = "event.processing.failure.handling.mode";
    private static final Set<String> OFFENDING_VALUES = Set.of("skip", "warn");

    private final Severity severity;

    public ConnectDebeziumEventProcessingFailureHandlingModeSkipOrWarnRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_EVENT_PROCESSING_FAILURE_HANDLING_MODE_SKIP_OR_WARN;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null || !connectorClass.startsWith(DEBEZIUM_CONNECTOR_CLASS_PREFIX)) continue;

            String mode = trimOrNull(p.getProperty(EVENT_PROCESSING_FAILURE_HANDLING_MODE));
            if (mode == null) continue;
            String modeLower = mode.toLowerCase(Locale.ROOT);
            if (!OFFENDING_VALUES.contains(modeLower)) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_EVENT_PROCESSING_FAILURE_HANDLING_MODE_SKIP_OR_WARN, severity,
                    ctx.relativize(e.getKey()), "key:" + EVENT_PROCESSING_FAILURE_HANDLING_MODE, 0,
                    "Debezium connector (" + connectorClass + ") has event.processing.failure.handling.mode="
                            + mode + ". This is a silent-data-loss switch: when Debezium fails to process a "
                            + "single source-database event (DDL parser stumble, schema-cache mismatch, "
                            + "decoder failure, corrupted WAL/binlog/redo-log entry, vendor-specific syntax "
                            + "the parser does not handle), the connector ADVANCES the source offset past "
                            + "the failing event and continues. The source-database's log-retention mechanism "
                            + "(Postgres prunes the WAL once the slot's confirmed_flush_lsn advances, MySQL "
                            + "purges the binlog once the offset advances, Oracle's redo logs roll over) "
                            + "eventually prunes the log segment containing the failing event, and the event "
                            + "is PERMANENTLY LOST. The `warn` mode at least logs the failing event's context "
                            + "(source position, payload, schema metadata) so a forensic reconstruction is "
                            + "possible from old log archives; the `skip` mode drops the event without "
                            + "logging context — there is no record of WHAT was lost. The default `fail` "
                            + "mode is correct for production CDC: stop the connector, page the operator, "
                            + "preserve the failing event at the source (the slot/offset does not advance, "
                            + "so the source log is retained, and the event can be replayed after the parser "
                            + "is patched / schema race is fixed / Debezium is upgraded). For ANY downstream "
                            + "consumer that depends on completeness (audit logs, financial reconciliation, "
                            + "regulatory reporting, search-index updates, materialized views, event-sourcing "
                            + "replay), the data-loss cost of `skip`/`warn` dominates the operational-noise "
                            + "cost of `fail`. Fix: remove this line (default `fail` is correct), or set "
                            + "event.processing.failure.handling.mode=fail explicitly. Repeated event-"
                            + "processing failures should be addressed by patching the underlying issue "
                            + "(upgrade Debezium for parser fixes, fix the upstream schema-evolution race, "
                            + "investigate disk-corruption signals), NOT by dropping the events."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
