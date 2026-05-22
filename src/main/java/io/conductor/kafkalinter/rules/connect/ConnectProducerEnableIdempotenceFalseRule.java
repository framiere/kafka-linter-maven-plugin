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
 *   <li>{@code producer.override.enable.idempotence} set to {@code false}
 *       (case-insensitive).</li>
 * </ul>
 *
 * <p>The Kafka producer's idempotent-write protocol (KIP-98, Kafka 0.11+)
 * prevents duplicate-on-retry by tagging every {@code ProduceRequest} with a
 * producer-id + monotonic sequence-number; the broker dedupes silently.
 * KIP-679 (Kafka 3.0) flipped the producer's default to {@code true}.
 * Connect's {@code WorkerSourceTask} commits source offsets ONLY after the
 * internal producer ACKs, so without idempotence every retry-after-ACK-timeout
 * appends a duplicate record on the broker while the source-offset advances
 * as if a single record had been written. Connect's exactly-once-source
 * (KIP-618, Kafka 3.3+) REQUIRES {@code enable.idempotence=true}; explicit
 * {@code false} blocks EOS-source adoption and silently breaks at-least-once.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectProducerEnableIdempotenceFalseRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String IDEMPOTENCE_OVERRIDE = "producer.override.enable.idempotence";

    private final Severity severity;

    public ConnectProducerEnableIdempotenceFalseRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_PRODUCER_ENABLE_IDEMPOTENCE_FALSE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;
            String idempotence = p.getProperty(IDEMPOTENCE_OVERRIDE);
            if (idempotence == null) continue;
            String v = idempotence.trim();
            if (v.isEmpty()) continue;
            if (!"false".equalsIgnoreCase(v)) continue;

            out.add(new Violation(
                    RuleId.CONNECT_PRODUCER_ENABLE_IDEMPOTENCE_FALSE, severity,
                    ctx.relativize(e.getKey()), "key:" + IDEMPOTENCE_OVERRIDE, 0,
                    "Connect connector " + p.getProperty(CONNECTOR_CLASS)
                            + " sets producer.override.enable.idempotence=false — this DISABLES "
                            + "the producer's idempotent-write protocol (KIP-98). Connect's "
                            + "automatic retry logic (default retries=Integer.MAX_VALUE, "
                            + "delivery.timeout.ms=120000) will then silently produce DUPLICATE "
                            + "records on every retry-after-ACK-timeout (network blip, leader "
                            + "re-election, broker GC pause): the broker has already written the "
                            + "record but its ACK was lost; without producer-id/sequence-number "
                            + "metadata, the broker can't dedupe the retry. Connect's source-task "
                            + "offset commit happens AFTER the producer ACK, so the source-offset "
                            + "reflects ONE record while the topic log contains TWO. Connect "
                            + "exactly-once-source support (KIP-618, Kafka 3.3+) REQUIRES "
                            + "enable.idempotence=true; with the override set to false, the "
                            + "framework refuses to start the connector under "
                            + "exactly.once.source.support=enabled. KIP-679 (Kafka 3.0) flipped "
                            + "the producer default to true precisely because this protocol has "
                            + "negligible overhead and major correctness benefit. Remove this "
                            + "override (the default of true is safe), or set "
                            + "producer.override.enable.idempotence=true explicitly."));
        }
        return out;
    }

    private static boolean notBlank(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
