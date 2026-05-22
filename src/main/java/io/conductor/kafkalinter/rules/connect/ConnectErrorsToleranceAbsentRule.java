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
 * Project-scoped rule. Fires on a Kafka Connect connector .properties file
 * (identified by top-level {@code connector.class}) that does NOT set
 * {@code errors.tolerance}.
 *
 * <p>Connect's {@code ConnectorConfig.ERRORS_TOLERANCE_CONFIG} declares
 * {@code errors.tolerance} with {@code ConfigDef.Type.STRING}, default
 * {@code "none"}, validator {@code ["none", "all"]}. With {@code none}, the
 * first per-record processing exception transitions the task to FAILED state
 * and halts the entire connector. The expected production posture is
 * {@code errors.tolerance=all} paired with a DLQ stanza.
 *
 * <p>WARNING severity because the default {@code none} halts the connector
 * on the first bad record — visibly bad operational behavior — but for some
 * connectors {@code none} IS the correct posture. The plugin's nudge is to
 * surface the missing knob: "choose one explicitly."
 *
 * <p>Complementary to {@link ConnectErrorsToleranceAllNoDlqRule} which fires
 * on {@code errors.tolerance=all} + missing DLQ.
 */
public final class ConnectErrorsToleranceAbsentRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String ERRORS_TOLERANCE = "errors.tolerance";

    private final Severity severity;

    public ConnectErrorsToleranceAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_ERRORS_TOLERANCE_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(ERRORS_TOLERANCE))) continue;
            out.add(new Violation(
                    RuleId.CONNECT_ERRORS_TOLERANCE_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + ERRORS_TOLERANCE, 0,
                    "Kafka Connect connector .properties (connector.class="
                            + p.getProperty(CONNECTOR_CLASS) + ") does NOT "
                            + "set `" + ERRORS_TOLERANCE + "`. Connect "
                            + "defaults `" + ERRORS_TOLERANCE + "` to "
                            + "`none` — meaning ANY exception thrown in the "
                            + "per-record processing pipeline (key/value "
                            + "Converter, single-message-transform chain, "
                            + "SinkTask.put for sinks or Producer.send for "
                            + "sources) TRANSITIONS THE TASK TO FAILED state "
                            + "and halts the entire connector. The first "
                            + "malformed record, schema-evolution mishap, "
                            + "or transient downstream-write failure stops "
                            + "the pipeline indefinitely until an operator "
                            + "manually restarts via the REST API. Worse, "
                            + "Connect resumes from the LAST COMMITTED offset "
                            + "on restart — so if the bad record is still at "
                            + "that offset, the task fails again immediately "
                            + "on the same record (hard fail-loop). Specific "
                            + "impacts of the absent key: (a) **single bad "
                            + "record halts the pipeline.** Upstream emits "
                            + "one null-field record; JDBC sink's "
                            + "value-mapping throws NPE; default `none` "
                            + "halts the task; operator pager fires; on-call "
                            + "must manually skip the offset or fix upstream "
                            + "before restart — 30+ minutes downtime. (b) "
                            + "**transient downstream errors become "
                            + "outages.** S3 sink hits one transient 5xx "
                            + "(documented 1-in-10000 S3 error rate); "
                            + "ConnectException halts the task; operator "
                            + "pager fires at 3am for what was a 50ms "
                            + "transient that already resolved. (c) **MM2 "
                            + "replication-topology halt on one oversized "
                            + "record.** One destination-broker-rejected "
                            + "record halts entire source-target replication "
                            + "pair; downstream consumers see lag spike. (d) "
                            + "**Debezium CDC pipeline freeze on schema-"
                            + "evolution window.** Mid-deploy schema "
                            + "evolution produces brief mixed-schema records; "
                            + "value-converter chokes; CDC topic stops "
                            + "receiving events; entire downstream CDC chain "
                            + "freezes. Common bug shapes: (1) JDBC sink "
                            + "halted by single null-field record — 30+ min "
                            + "downtime; (2) S3 sink halted by transient 5xx "
                            + "— 3am pager for resolved transient; (3) MM2 "
                            + "replication halt on single oversized record "
                            + "— full topology stops; (4) Debezium CDC "
                            + "freeze on schema-evolution window — pipeline "
                            + "stalls mid-deploy; (5) operator-template "
                            + "missing the knob propagates org-wide — 47 "
                            + "connectors with same fail-fast posture, "
                            + "ticket-category dominates on-call load; (6) "
                            + "resource-locking ledger sink — `none` IS "
                            + "correct here, but should be EXPLICIT to "
                            + "document the deliberate choice. Fix: ONE LINE "
                            + "per connector. Production default for "
                            + "high-volume sinks/sources: `errors.tolerance"
                            + "=all` PAIRED with the DLQ stanza (`errors."
                            + "deadletterqueue.topic.name=<dlq>`, `errors."
                            + "deadletterqueue.topic.replication.factor=3`, "
                            + "`errors.deadletterqueue.context.headers."
                            + "enable=true`, `errors.log.enable=true`, "
                            + "`errors.log.include.messages=true`). OR "
                            + "explicit `errors.tolerance=none` to document "
                            + "the deliberate fail-fast posture for "
                            + "connectors where any drop is catastrophic "
                            + "(financial reconciliation, ledger writes). "
                            + "Sibling rule [[connect-errors-tolerance-all-"
                            + "no-dlq]] fires the COMPLEMENTARY case: "
                            + "`errors.tolerance=all` SET but DLQ MISSING; "
                            + "the two rules together enforce the full "
                            + "KIP-298 contract."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
