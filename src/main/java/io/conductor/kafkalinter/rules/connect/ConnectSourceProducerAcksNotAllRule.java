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
 * has BOTH:
 * <ul>
 *   <li>a {@code connector.class} key (the canonical Connect-config fingerprint), AND</li>
 *   <li>{@code producer.override.acks} set to any value other than {@code all} or {@code -1}
 *       (case-insensitive).</li>
 * </ul>
 *
 * <p>Connect's {@code WorkerSourceTask} commits source offsets ONLY after the
 * internal producer ACKs the record. With {@code acks=all} (the Kafka 3.0+ default),
 * an ACK means 'durably replicated to all in-sync replicas.' Downgrading to
 * {@code acks=1} or {@code acks=0} via KIP-458's {@code producer.override.*}
 * mechanism silently breaks Connect's at-least-once contract: source offsets
 * advance past records that were leader-only-ACKed (and lost on failover) or
 * fire-and-forgotten.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectSourceProducerAcksNotAllRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String PRODUCER_ACKS_OVERRIDE = "producer.override.acks";

    private final Severity severity;

    public ConnectSourceProducerAcksNotAllRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_SOURCE_PRODUCER_ACKS_NOT_ALL;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;
            String acks = p.getProperty(PRODUCER_ACKS_OVERRIDE);
            if (acks == null) continue;
            String v = acks.trim();
            if (v.isEmpty()) continue;
            if ("all".equalsIgnoreCase(v) || "-1".equals(v)) continue;

            out.add(new Violation(
                    RuleId.CONNECT_SOURCE_PRODUCER_ACKS_NOT_ALL, severity,
                    ctx.relativize(e.getKey()), "key:" + PRODUCER_ACKS_OVERRIDE, 0,
                    "Connect connector " + p.getProperty(CONNECTOR_CLASS)
                            + " sets producer.override.acks=" + v
                            + " — this DOWNGRADES the durability of Connect's source-task producer "
                            + "below at-least-once. acks=1 means the leader broker ACKs without "
                            + "waiting for in-sync replicas; a leader failover before replication "
                            + "loses the record AND Connect has already committed its source-offset. "
                            + "acks=0 means fire-and-forget — broker outages are silent. KIP-679 "
                            + "(Kafka 3.0) made acks=all the producer default for exactly this "
                            + "reason. Remove this override; producer.override.acks=all is the "
                            + "only correct value."));
        }
        return out;
    }

    private static boolean notBlank(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
