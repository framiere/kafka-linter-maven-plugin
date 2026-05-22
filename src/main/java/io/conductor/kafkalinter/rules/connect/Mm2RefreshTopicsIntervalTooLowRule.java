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
import java.util.Set;

/**
 * Project-scoped rule. Fires when any MirrorMaker 2 connector config
 * ({@code connector.class} in
 * {@code {MirrorSourceConnector, MirrorCheckpointConnector, MirrorHeartbeatConnector}})
 * sets {@code refresh.topics.interval.seconds} to an integer LESS THAN
 * {@value #THRESHOLD_SECONDS}. The MM2 ConfigDef default is 600 (10 min); the
 * threshold is the plugin's pragmatic line between 'aggressive but defensible'
 * and 'almost certainly broker-thrash'.
 */
public final class Mm2RefreshTopicsIntervalTooLowRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final Set<String> MM2_CONNECTOR_CLASSES = Set.of(
            "org.apache.kafka.connect.mirror.MirrorSourceConnector",
            "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
            "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector");
    private static final String REFRESH_TOPICS_INTERVAL_SECONDS = "refresh.topics.interval.seconds";
    private static final int THRESHOLD_SECONDS = 60;

    private final Severity severity;

    public Mm2RefreshTopicsIntervalTooLowRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.MM2_REFRESH_TOPICS_INTERVAL_TOO_LOW;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!MM2_CONNECTOR_CLASSES.contains(connectorClass)) continue;

            String raw = trimOrNull(p.getProperty(REFRESH_TOPICS_INTERVAL_SECONDS));
            if (raw == null) continue;

            int value;
            try {
                value = Integer.parseInt(raw);
            } catch (NumberFormatException nfe) {
                continue;
            }
            if (value >= THRESHOLD_SECONDS) continue;

            String file = ctx.relativize(e.getKey());
            out.add(new Violation(
                    RuleId.MM2_REFRESH_TOPICS_INTERVAL_TOO_LOW, severity,
                    file, "key:" + REFRESH_TOPICS_INTERVAL_SECONDS, 0,
                    "MirrorMaker 2 connector (" + connectorClass + ") has "
                            + REFRESH_TOPICS_INTERVAL_SECONDS + "=" + value
                            + " — below the plugin's broker-thrash threshold of "
                            + THRESHOLD_SECONDS + " seconds (the MM2 ConfigDef default "
                            + "is 600 s = 10 minutes). `refresh.topics.interval.seconds` "
                            + "controls how often the connector performs a FULL TOPIC "
                            + "METADATA REFRESH against the source cluster — calling "
                            + "`AdminClient.listTopics()`, `AdminClient.describeTopics()` "
                            + "for every topic matching the connector's `topics`/"
                            + "`topics.exclude` regex, plus `describeConfigs()` (when "
                            + "`sync.topic.configs.enabled=true`, default) and "
                            + "`describeAcls()` (when `sync.topic.acls.enabled=true`, "
                            + "default). On a source cluster with N topics, ONE refresh "
                            + "issues O(N) admin RPCs to the source-cluster CONTROLLER "
                            + "(a single broker — metadata operations are controller-"
                            + "routed). With N=5000 topics and an interval of " + value
                            + " s, that is roughly " + (15000 / Math.max(1, value))
                            + "+ admin-RPCs-per-second to the controller from THIS "
                            + "connector alone — multiplied across MM2 connector "
                            + "instances and multi-source topologies. The controller's "
                            + "single-threaded event loop saturates; legitimate "
                            + "`CreateTopics`/`AlterConfigs` operations time out; in "
                            + "pathological cases the controller fails its session-"
                            + "timeout heartbeat with ZooKeeper/KRaft and triggers an "
                            + "unnecessary controller failover. The typical wrong "
                            + "value (10 s or 30 s) is almost always: (a) tutorial "
                            + "copy-paste where the tutorial author set a low value "
                            + "'so the demo reacts quickly when you create a topic' "
                            + "(tutorial cluster had 3 topics; production has 5000); "
                            + "(b) debug-session leftover where the operator lowered "
                            + "the interval to confirm a topic-regex was matching and "
                            + "never reverted; (c) confusion with consumer-side "
                            + "`metadata.max.age.ms` (which is unrelated and would not "
                            + "affect MM2 anyway); (d) misunderstanding that this knob "
                            + "affects RECORD replication lag (it does not — record "
                            + "replication runs continuously via the Connect poll loop). "
                            + "Fix: remove the explicit value (the default 600 s is "
                            + "production-recommended) or set to a value >= "
                            + THRESHOLD_SECONDS + " s if there is a deliberate reason. "
                            + "Detection: trim, integer parse of "
                            + "`refresh.topics.interval.seconds`; comparison against "
                            + "the threshold; exact class match against the MM2 "
                            + "connector class set. The rule does NOT fire when the "
                            + "key is absent (default 600 applies) or when the value "
                            + "fails to parse as an integer."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
