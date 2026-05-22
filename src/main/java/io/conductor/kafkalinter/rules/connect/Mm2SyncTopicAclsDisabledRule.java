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
 * Project-scoped rule. Fires when a MirrorMaker 2 source-connector config
 * ({@code connector.class=org.apache.kafka.connect.mirror.MirrorSourceConnector})
 * EXPLICITLY sets {@code sync.topic.acls.enabled=false}. The default is
 * {@code true}; opting out leaves the target-cluster replicated topic with
 * no source-derived ACLs — the topic's effective security posture becomes
 * whatever the target broker's default and pre-existing wildcard ACLs say
 * (either a data leak when {@code allow.everyone.if.no.acl.found=true} or
 * a failover-blocker when {@code =false}).
 *
 * <p>Only {@code MirrorSourceConnector} reads this key.
 */
public final class Mm2SyncTopicAclsDisabledRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String MIRROR_SOURCE_CONNECTOR =
            "org.apache.kafka.connect.mirror.MirrorSourceConnector";
    private static final String SYNC_TOPIC_ACLS_ENABLED = "sync.topic.acls.enabled";

    private final Severity severity;

    public Mm2SyncTopicAclsDisabledRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.MM2_SYNC_TOPIC_ACLS_DISABLED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!MIRROR_SOURCE_CONNECTOR.equals(connectorClass)) continue;

            String raw = trimOrNull(p.getProperty(SYNC_TOPIC_ACLS_ENABLED));
            if (raw == null) continue;
            if (!"false".equalsIgnoreCase(raw)) continue;

            String file = ctx.relativize(e.getKey());
            out.add(new Violation(
                    RuleId.MM2_SYNC_TOPIC_ACLS_DISABLED, severity,
                    file, "key:" + SYNC_TOPIC_ACLS_ENABLED, 0,
                    "MirrorMaker 2 source connector (" + connectorClass + ") has "
                            + SYNC_TOPIC_ACLS_ENABLED + "=" + raw + " — this OPTS OUT "
                            + "of topic-ACL synchronization between the source cluster and "
                            + "the target cluster. The default value is `true` because the "
                            + "dominant MM2 use case is DR replication, where target-cluster "
                            + "replicated topics MUST be accessible to the same principals "
                            + "that could read the source topics. ACLs are part of the "
                            + "topic's SECURITY CONTRACT — the set of principals authorized "
                            + "to read or write the topic. Two failure shapes: (1) DATA LEAK "
                            + "ON THE TARGET — when the target cluster's broker config has "
                            + "`allow.everyone.if.no.acl.found=true` (a common legacy "
                            + "default), the replicated topic on the target has NO ACLs and "
                            + "is therefore readable by EVERY authenticated principal "
                            + "(including data-analytics, marketing, BI, and other teams "
                            + "that had no source-cluster access). A PII or financial-data "
                            + "topic that was tightly ACL'd on the source becomes wide-open "
                            + "on the target — a GDPR / SOX / PCI compliance violation. (2) "
                            + "DR FAILOVER BLOCKED — when the target cluster's broker config "
                            + "has `allow.everyone.if.no.acl.found=false` (the production-"
                            + "recommended default), the replicated topic has NO ACLs and "
                            + "NO principal is authorized; during a primary→DR cutover, the "
                            + "consumer applications' service-account principals get "
                            + "`TopicAuthorizationException` on every poll; the failover is "
                            + "blocked until an operator manually applies the missing ACLs "
                            + "on the target (a 30+ minute scramble during an active "
                            + "incident). Fix: remove the explicit `=false` (the default "
                            + "`=true` is the production-recommended setting) AND ensure the "
                            + "MM2 connector's principal has ALTER permissions on the target "
                            + "cluster's ACLs (without ALTER, the sync fails with "
                            + "ClusterAuthorizationException — which is the most common "
                            + "reason operators disable the sync, treating the symptom "
                            + "rather than the cause). If the operator deliberately wants "
                            + "per-cluster security divergence (e.g., target is a sanitized "
                            + "analytics cluster with separately-managed ACLs), document the "
                            + "intent and suppress this rule via per-rule severity override "
                            + "(`<MM2_SYNC_TOPIC_ACLS_DISABLED>OFF</MM2_SYNC_TOPIC_ACLS_DISABLED>` "
                            + "in the plugin config). Default severity is WARNING (not "
                            + "ERROR) because the legitimate per-cluster-security-divergence "
                            + "use case exists. NOTE: enabling `sync.topic.acls.enabled=true` "
                            + "does NOT retroactively fix already-replicated topics that "
                            + "lack ACLs — MM2 syncs ACLs going forward on the next "
                            + "`sync.topic.acls.interval.seconds` cycle (default 600 s), but "
                            + "topics that were created during the `=false` window will be "
                            + "back-filled on that next sync cycle. Verify the back-fill "
                            + "completed before relying on the sync."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
