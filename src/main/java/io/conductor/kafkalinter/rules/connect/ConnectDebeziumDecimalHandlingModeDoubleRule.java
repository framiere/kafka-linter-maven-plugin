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
 * {@code io.debezium.connector.}) sets
 * {@code decimal.handling.mode=double}.
 *
 * <p>The {@code double} mode encodes DECIMAL/NUMERIC columns as 64-bit
 * IEEE-754 floating-point — irreversibly losing precision beyond ~15-17
 * significant digits and silently rounding every value. For financial,
 * monetary, inventory, regulatory, and audit data — the dominant DECIMAL
 * use cases that drive CDC — this is silent data corruption. The default
 * {@code precise} mode encodes losslessly as Avro decimal logical type;
 * {@code string} mode is the lossless alternative for JSON-only consumers.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDebeziumDecimalHandlingModeDoubleRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DEBEZIUM_CLASS_PREFIX = "io.debezium.connector.";
    private static final String DECIMAL_HANDLING_MODE = "decimal.handling.mode";
    private static final String DOUBLE_MODE = "double";

    private final Severity severity;

    public ConnectDebeziumDecimalHandlingModeDoubleRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DEBEZIUM_DECIMAL_HANDLING_MODE_DOUBLE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!connectorClass.startsWith(DEBEZIUM_CLASS_PREFIX)) continue;
            String mode = trimOrNull(p.getProperty(DECIMAL_HANDLING_MODE));
            if (mode == null) continue;
            if (!DOUBLE_MODE.equalsIgnoreCase(mode)) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DEBEZIUM_DECIMAL_HANDLING_MODE_DOUBLE, severity,
                    ctx.relativize(e.getKey()), "key:" + DECIMAL_HANDLING_MODE, 0,
                    "Debezium connector " + connectorClass
                            + " sets decimal.handling.mode=double — every DECIMAL/NUMERIC "
                            + "column is silently rounded to the nearest 64-bit IEEE-754 "
                            + "double at the connector, before the record reaches Kafka. "
                            + "Values with more than ~15-17 significant decimal digits "
                            + "lose precision irreversibly; even shorter values can drift "
                            + "because not every decimal fraction has an exact binary "
                            + "representation (e.g., 0.1 is stored as 0.1000000000000000055...). "
                            + "For monetary/financial/inventory/regulatory/audit data — the "
                            + "dominant DECIMAL use cases driving CDC — this is silent data "
                            + "corruption: a ledger value 99999999999999.99 becomes "
                            + "99999999999999.984, a price 0.1234 becomes "
                            + "0.12340000000000001, sum-aggregations drift, audits fail "
                            + "weeks-to-months later. Set decimal.handling.mode=precise "
                            + "(default — encodes as Avro `bytes` + logicalType:decimal, "
                            + "preserves every digit via BigDecimal) or "
                            + "decimal.handling.mode=string (encodes as the literal "
                            + "string representation — lossless and JSON-consumer-friendly)."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
