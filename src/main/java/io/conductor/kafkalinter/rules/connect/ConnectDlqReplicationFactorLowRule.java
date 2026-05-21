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
 * Project-scoped rule. Fires when a Kafka Connect connector .properties file
 * has ALL of:
 * <ul>
 *   <li>a {@code connector.class} key (the canonical Connect-config fingerprint), AND</li>
 *   <li>{@code errors.deadletterqueue.topic.name} set (DLQ machinery engaged), AND</li>
 *   <li>{@code errors.deadletterqueue.topic.replication.factor} set explicitly to a value
 *       less than 3.</li>
 * </ul>
 *
 * <p>The DLQ is the recovery path for un-processable records; making it less
 * durable than data topics inverts the durability hierarchy. A single-replica
 * DLQ becomes unavailable during any broker outage that contains its leader —
 * exactly when the operator most needs it.
 *
 * <p>Does NOT fire when the replication factor is unset (the framework default
 * is 3, sourced from broker's default.replication.factor; detecting that
 * broker-side default would require cluster-side inspection beyond the
 * .properties contract).
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectDlqReplicationFactorLowRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String DLQ_TOPIC = "errors.deadletterqueue.topic.name";
    private static final String DLQ_RF = "errors.deadletterqueue.topic.replication.factor";

    private static final int MIN_PRODUCTION_RF = 3;

    private final Severity severity;

    public ConnectDlqReplicationFactorLowRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_DLQ_REPLICATION_FACTOR_LOW;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;
            if (!notBlank(p.getProperty(DLQ_TOPIC))) continue;

            String rfRaw = p.getProperty(DLQ_RF);
            if (rfRaw == null) continue;
            String rf = rfRaw.trim();
            if (rf.isEmpty()) continue;

            int rfValue;
            try {
                rfValue = Integer.parseInt(rf);
            } catch (NumberFormatException nfe) {
                continue;
            }
            if (rfValue >= MIN_PRODUCTION_RF) continue;

            out.add(new Violation(
                    RuleId.CONNECT_DLQ_REPLICATION_FACTOR_LOW, severity,
                    ctx.relativize(e.getKey()), "key:" + DLQ_RF, 0,
                    "Connect connector " + p.getProperty(CONNECTOR_CLASS)
                            + " has DLQ topic " + p.getProperty(DLQ_TOPIC).trim()
                            + " configured with " + DLQ_RF + "=" + rfValue
                            + " — the DLQ is the recovery path for un-processable records "
                            + "and a single-replica (or RF<3) DLQ becomes unavailable during "
                            + "any broker outage that contains its leader. The DLQ should be "
                            + "MORE durable than data topics, not less. Set " + DLQ_RF + "=3 "
                            + "(or to your cluster's data-topic replication factor)."));
        }
        return out;
    }

    private static boolean notBlank(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
