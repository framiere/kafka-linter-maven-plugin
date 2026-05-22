package io.conductor.kafkalinter.rules.clients;

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
 * Project-scoped rule. Fires on a plain kafka-clients consumer .properties
 * file (identified by top-level {@code key.deserializer} or
 * {@code value.deserializer}) that does NOT set {@code request.timeout.ms}.
 *
 * <p>kafka-clients' {@code ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG}
 * defaults to {@code 30000} (30 seconds). It is the per-request client-side
 * wait window for every broker call (Fetch, JoinGroup, SyncGroup,
 * OffsetCommit, OffsetFetch, FindCoordinator, ListOffsets, Heartbeat,
 * LeaveGroup, Metadata).
 *
 * <p>Critical coupling: kafka-clients 3.x raised the default
 * {@code session.timeout.ms} to 45000ms (KIP-735) WITHOUT raising the
 * default {@code request.timeout.ms}. Every consumer on full defaults in
 * 3.x now silently violates the informal guideline
 * {@code request.timeout.ms > session.timeout.ms} — manifests as a
 * client-side give-up-then-rejoin loop during rebalance.
 *
 * <p>INFO severity because (a) the violation is silent under defaults
 * during steady-state, (b) the rebalance-loop pathology only manifests
 * when a rebalance occurs, (c) the rule is a documentation nudge prompting
 * the operator to set request.timeout.ms paired with session.timeout.ms.
 *
 * <p>A file is "consumer-shaped" when it sets {@code key.deserializer} OR
 * {@code value.deserializer} as a TOP-LEVEL key. Excludes Connect
 * ({@code connector.class}) and Streams ({@code application.id}).
 */
public final class ConsumerPropertiesRequestTimeoutMsAbsentRule implements ProjectScopedRule {

    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String REQUEST_TIMEOUT_MS = "request.timeout.ms";

    private final Severity severity;

    public ConsumerPropertiesRequestTimeoutMsAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_PROPERTIES_REQUEST_TIMEOUT_MS_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String shapeKey = consumerShapeKey(p);
            if (shapeKey == null) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(REQUEST_TIMEOUT_MS))) continue;
            out.add(new Violation(
                    RuleId.CONSUMER_PROPERTIES_REQUEST_TIMEOUT_MS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + REQUEST_TIMEOUT_MS, 0,
                    "kafka-clients consumer .properties file (detected via "
                            + "top-level `" + shapeKey + "`) does NOT set `"
                            + REQUEST_TIMEOUT_MS + "`. kafka-clients "
                            + "defaults `" + REQUEST_TIMEOUT_MS + "` to "
                            + "`30000` (30 seconds). This is the PER-"
                            + "REQUEST client-side wait window for EVERY "
                            + "broker call the consumer makes: Fetch, "
                            + "JoinGroup, SyncGroup, OffsetCommit, "
                            + "OffsetFetch, FindCoordinator, ListOffsets, "
                            + "Heartbeat, LeaveGroup, Metadata. It is "
                            + "structurally COUPLED with `session.timeout."
                            + "ms` (default 45000ms in 3.x via KIP-735, "
                            + "previously 10000ms in 2.x) via the informal "
                            + "kafka-clients guideline: " + REQUEST_TIMEOUT_MS
                            + " > session.timeout.ms. The broker holds a "
                            + "JoinGroup or SyncGroup request OPEN for up "
                            + "to session.timeout.ms during rebalance; if "
                            + REQUEST_TIMEOUT_MS + " is shorter, the "
                            + "consumer's own client gives up on JoinGroup "
                            + "BEFORE the broker finishes the rebalance, "
                            + "re-issues JoinGroup, broker treats it as a "
                            + "fresh join and restarts the rebalance — an "
                            + "infinite re-join loop that prevents the "
                            + "consumer from ever stabilizing. KIP-735 "
                            + "silently broke this: kafka-clients 3.x "
                            + "default " + REQUEST_TIMEOUT_MS + "=30000 < "
                            + "default session.timeout.ms=45000. Every "
                            + "consumer on full defaults in 3.x is now in "
                            + "a silently-broken regime during rebalances. "
                            + "Specific impacts of the absent key: (a) "
                            + "**KIP-735 default-violation rebalance loop "
                            + "— invisible on steady state, broken on "
                            + "rebalance.** Consumer joins, leaves, or "
                            + "partition reassigned → JoinGroup is issued; "
                            + "broker holds it OPEN for up to 45s waiting "
                            + "for all group members; client times out at "
                            + "30s, cancels, re-issues; broker restarts "
                            + "the rebalance protocol; consumer is stuck. "
                            + "Symptoms: repeated `(Re-)joining group` "
                            + "every 30s; `last-rebalance-seconds-ago` "
                            + "resets every 30s; `records-consumed-rate` "
                            + "= 0 during the loop; (b) **Fetch storm — "
                            + REQUEST_TIMEOUT_MS + " < fetch.max.wait.ms "
                            + "after the operator tunes fetch.max.wait.ms "
                            + "UP for batching.** Operator sets `fetch."
                            + "max.wait.ms=60000` to allow large fetches; "
                            + "leaves " + REQUEST_TIMEOUT_MS + " at "
                            + "default 30000. The broker holds Fetch OPEN "
                            + "for up to 60s; client gives up at 30s and "
                            + "re-issues; the broker treats it as a new "
                            + "FetchRequest, starts another 60s wait, "
                            + "cycle repeats. Consumer's `fetch-rate` "
                            + "high but `records-consumed-rate` near zero. "
                            + "(c) **OffsetCommit timeout during partition "
                            + "migration causes duplicate processing.** "
                            + "During a broker restart / controller "
                            + "failover, an OffsetCommit can take up to "
                            + "35s while the broker recovers leadership; "
                            + "default " + REQUEST_TIMEOUT_MS + "=30000 "
                            + "times it out; the commit silently does not "
                            + "land; on next rebalance the consumer "
                            + "re-processes the records in the missing-"
                            + "commit window — silent at-most-once → "
                            + "at-least-once-with-duplicates downgrade. "
                            + "Common bug shapes: (1) Spring Boot consumer "
                            + "on kafka-clients 3.x with default " + REQUEST_TIMEOUT_MS
                            + " — rebalance-loop pathology silently "
                            + "present in nearly every microservice in "
                            + "production today; (2) Operator tunes fetch."
                            + "max.wait.ms UP without thinking about "
                            + REQUEST_TIMEOUT_MS + " — fetch storm; (3) "
                            + "Operator tunes session.timeout.ms UP to "
                            + "120000 for slow consumers, leaves "
                            + REQUEST_TIMEOUT_MS + " at default — "
                            + "rebalance loop, worse than KIP-735 default "
                            + "case; (4) Kafka Connect internal vs "
                            + "external consumer — operator can't tune "
                            + REQUEST_TIMEOUT_MS + " separately for the "
                            + "two consumer types in Connect's worker "
                            + "config; (5) Multi-cluster consumer pool "
                            + "where one cluster is slower — default "
                            + REQUEST_TIMEOUT_MS + " breaks on the slow "
                            + "cluster's rebalances but works on the fast "
                            + "one. Fix: ONE LINE. Set `" + REQUEST_TIMEOUT_MS
                            + "` STRICTLY GREATER than `session.timeout."
                            + "ms`, ALWAYS with both set together so the "
                            + "rebalance budget is explicit. Convention: "
                            + REQUEST_TIMEOUT_MS + " = session.timeout.ms "
                            + "+ 15000 (15s margin for JoinGroup completion "
                            + "after broker-side timeout). Examples: low-"
                            + "latency consumer: `session.timeout.ms="
                            + "10000, " + REQUEST_TIMEOUT_MS + "=20000`; "
                            + "robust consumer: `session.timeout.ms=45000, "
                            + REQUEST_TIMEOUT_MS + "=60000`; long-"
                            + "rebalance large group: `session.timeout.ms="
                            + "120000, " + REQUEST_TIMEOUT_MS + "=180000`. "
                            + "Sibling rule [[consumer-properties-session-"
                            + "timeout-ms-absent]] catches the coupled-"
                            + "partner absence."));
        }
        return out;
    }

    private static String consumerShapeKey(Properties p) {
        if (isNonEmpty(p.getProperty(KEY_DESERIALIZER))) return KEY_DESERIALIZER;
        if (isNonEmpty(p.getProperty(VALUE_DESERIALIZER))) return VALUE_DESERIALIZER;
        return null;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
