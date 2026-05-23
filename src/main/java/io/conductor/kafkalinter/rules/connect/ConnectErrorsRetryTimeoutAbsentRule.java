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
 * Project-scoped rule. Fires on a Kafka Connect connector .properties
 * file (identified by top-level {@code connector.class}) that does
 * NOT set {@code errors.retry.timeout}.
 *
 * <p>Connect defaults {@code errors.retry.timeout} to {@code 0}
 * (declared in {@code ConnectorConfig.ERRORS_RETRY_TIMEOUT_CONFIG},
 * {@code ConfigDef.Type.LONG}). With value {@code 0}, the framework's
 * {@code RetryWithToleranceOperator} does NOT retry retriable
 * failures AT ALL — the first transient broker/network/downstream
 * error skips straight to the {@code errors.tolerance} decision
 * (FAIL the task under {@code none}, route to DLQ under {@code all}).
 *
 * <p>KIP-298 (Apache Kafka 2.0, July 2018) introduced this knob
 * alongside {@code errors.tolerance} specifically so retriable errors
 * could be handled gracefully — the retry-with-tolerance contract is
 * TWO-STAGED: first RETRY for up to {@code errors.retry.timeout} ms
 * with exponential backoff; ONLY after exhausting the retry budget
 * does the {@code errors.tolerance} policy apply.
 *
 * <p>INFO severity because the default {@code 0} is defensible for
 * fail-fast workloads (financial reconciliation, ledger writes), the
 * right value is workload-dependent (60 s for typical sinks, 5 min
 * for MM2-style replication, -1 for never-drop-a-record), and the
 * linter can't infer the workload character from .properties alone.
 *
 * <p>Sibling rules: {@code CONNECT_ERRORS_TOLERANCE_ABSENT} (the
 * tolerance-policy companion), {@code CONNECT_ERRORS_TOLERANCE_ALL_NO_LOG}
 * (the observability companion). Together these rules cover the full
 * KIP-298 contract: tolerance policy + retry budget + DLQ channel +
 * log channel.
 */
public final class ConnectErrorsRetryTimeoutAbsentRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String ERRORS_RETRY_TIMEOUT = "errors.retry.timeout";

    private final Severity severity;

    public ConnectErrorsRetryTimeoutAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_ERRORS_RETRY_TIMEOUT_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(ERRORS_RETRY_TIMEOUT))) continue;
            out.add(new Violation(
                    RuleId.CONNECT_ERRORS_RETRY_TIMEOUT_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + ERRORS_RETRY_TIMEOUT, 0,
                    "Kafka Connect connector .properties file (detected via top-"
                            + "level `" + CONNECTOR_CLASS + "`) does NOT set `"
                            + ERRORS_RETRY_TIMEOUT + "`. Connect defaults `"
                            + ERRORS_RETRY_TIMEOUT + "` to `0` — the framework's "
                            + "`RetryWithToleranceOperator` performs NO retries "
                            + "on retriable failures. The first transient "
                            + "broker-side error, downstream-system blip, "
                            + "network jitter, or schema-registry timeout is "
                            + "treated as a TERMINAL failure: with `errors."
                            + "tolerance=none` (the default) the task "
                            + "transitions to FAILED; with `errors.tolerance="
                            + "all` the record is routed to the DLQ (or "
                            + "skipped) immediately, with NO retry attempted. "
                            + "KIP-298 (Apache Kafka 2.0, July 2018) introduced "
                            + "this knob alongside `errors.tolerance` so "
                            + "retriable errors could be handled gracefully — "
                            + "the retry-with-tolerance contract is TWO-STAGED: "
                            + "first RETRY for up to `" + ERRORS_RETRY_TIMEOUT
                            + "` milliseconds with exponential backoff (capped "
                            + "per-attempt by `errors.retry.delay.max.ms`, "
                            + "default 60000); ONLY after exhausting the retry "
                            + "budget does the `errors.tolerance` policy apply "
                            + "(FAIL the task or route to DLQ). With `"
                            + ERRORS_RETRY_TIMEOUT + "=0` (the default), the "
                            + "first stage is skipped entirely. Specific bug "
                            + "shapes: (a) **S3 sink, single transient 5xx "
                            + "fills DLQ** — S3 returns a 1-in-10000 transient "
                            + "503 SlowDown; with `" + ERRORS_RETRY_TIMEOUT
                            + "=0`, the record routes to DLQ; over a week the "
                            + "DLQ accumulates thousands of records that would "
                            + "have succeeded on the SECOND try; setting `"
                            + ERRORS_RETRY_TIMEOUT + "=60000` (60 s) absorbs "
                            + "S3's typical sub-second recovery. (b) **JDBC "
                            + "sink, connection-pool exhaustion mass-DLQs** — "
                            + "HikariCP throws ConnectionTimeoutException "
                            + "during a DB backup window; with `"
                            + ERRORS_RETRY_TIMEOUT + "=0` every record routes "
                            + "to DLQ; setting `" + ERRORS_RETRY_TIMEOUT
                            + "=60000` absorbs the 30-second backup window. "
                            + "(c) **Elasticsearch sink, bulk-request 429** — "
                            + "ES returns HTTP 429 (Too Many Requests) under "
                            + "ingest pressure; without retry budget every "
                            + "429 is terminal; setting `" + ERRORS_RETRY_TIMEOUT
                            + "=120000` (2 min) lets the connector self-"
                            + "throttle via exponential backoff. (d) **HTTP "
                            + "sink, downstream maintenance window** — a 30-"
                            + "second API maintenance window kills the "
                            + "connector; setting `" + ERRORS_RETRY_TIMEOUT
                            + "=60000` waits it out. (e) **Schema-registry "
                            + "intermittent failure** — a 10-second schema-"
                            + "registry deploy fails every record at the "
                            + "converter stage; setting `" + ERRORS_RETRY_TIMEOUT
                            + "=30000` covers the deploy window. (f) **MM2 "
                            + "replication with rolling broker restart** — "
                            + "destination cluster's rolling restart "
                            + "intermittently fails producer requests; setting "
                            + "`" + ERRORS_RETRY_TIMEOUT + "=300000` (5 min) "
                            + "absorbs the full rolling-restart window. (g) "
                            + "**Financial-reconciliation sink — fail-fast IS "
                            + "right** — operator deliberately wants `"
                            + ERRORS_RETRY_TIMEOUT + "=0` so retriable errors "
                            + "halt the task and page on-call IMMEDIATELY; in "
                            + "this case set the value EXPLICITLY (`"
                            + ERRORS_RETRY_TIMEOUT + "=0`) to document the "
                            + "deliberate choice. Fix: ONE LINE. Production "
                            + "default for retriable-failure-tolerant sinks: `"
                            + ERRORS_RETRY_TIMEOUT + "=60000` (60 s — typical "
                            + "7-10 retry attempts with exponential backoff). "
                            + "For never-drop-a-record connectors (financial, "
                            + "ledger): `" + ERRORS_RETRY_TIMEOUT + "=-1` "
                            + "(infinite retries — requires paired health-check "
                            + "monitoring to detect non-retriable errors "
                            + "disguised as retriable). For deliberate fail-"
                            + "fast: explicit `" + ERRORS_RETRY_TIMEOUT + "=0` "
                            + "documents the choice. Sibling rules [[connect-"
                            + "errors-tolerance-absent]] (the tolerance-policy "
                            + "companion), [[connect-errors-tolerance-all-no-"
                            + "log]] (the observability companion), [[connect-"
                            + "errors-tolerance-all-no-dlq]] (the DLQ-presence "
                            + "companion). Together these four rules cover "
                            + "the full KIP-298 contract: tolerance policy + "
                            + "retry budget + DLQ channel + log channel."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
