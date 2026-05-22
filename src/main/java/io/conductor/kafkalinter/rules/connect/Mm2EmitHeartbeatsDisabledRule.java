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
 * Project-scoped rule. Fires when a MirrorMaker 2 heartbeat-connector config
 * ({@code connector.class=org.apache.kafka.connect.mirror.MirrorHeartbeatConnector})
 * EXPLICITLY sets {@code emit.heartbeats.enabled=false}. This is a
 * NULL-CONFIGURATION — the connector's only job is to emit heartbeats; with
 * emission disabled, the connector occupies a Connect-cluster task slot and
 * does nothing useful. If heartbeats are not wanted, the connector simply
 * should not be deployed.
 */
public final class Mm2EmitHeartbeatsDisabledRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String MIRROR_HEARTBEAT_CONNECTOR =
            "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector";
    private static final String EMIT_HEARTBEATS_ENABLED = "emit.heartbeats.enabled";

    private final Severity severity;

    public Mm2EmitHeartbeatsDisabledRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.MM2_EMIT_HEARTBEATS_DISABLED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!MIRROR_HEARTBEAT_CONNECTOR.equals(connectorClass)) continue;

            String raw = trimOrNull(p.getProperty(EMIT_HEARTBEATS_ENABLED));
            if (raw == null) continue;
            if (!"false".equalsIgnoreCase(raw)) continue;

            String file = ctx.relativize(e.getKey());
            out.add(new Violation(
                    RuleId.MM2_EMIT_HEARTBEATS_DISABLED, severity,
                    file, "key:" + EMIT_HEARTBEATS_ENABLED, 0,
                    "MirrorMaker 2 heartbeat connector (" + connectorClass + ") has "
                            + EMIT_HEARTBEATS_ENABLED + "=" + raw + " — this is a NULL "
                            + "CONFIGURATION. The entire purpose of MirrorHeartbeatConnector "
                            + "is to periodically write small records (one per "
                            + "`heartbeats.topic.heartbeat.interval.seconds`, default 1 s) "
                            + "to the `heartbeats` topic on the target cluster so that "
                            + "downstream observers — other MM2 connectors that emit "
                            + "checkpoints, monitoring systems that query the heartbeats "
                            + "topic's last-record timestamp to confirm replication is "
                            + "alive, replication-lag dashboards that measure source-vs-"
                            + "target heartbeat-timestamp delta — can confirm the "
                            + "replication pipeline is healthy. With "
                            + "`emit.heartbeats.enabled=false`, the connector starts, "
                            + "claims its Connect-cluster task slot, holds its rebalance "
                            + "lease, generates Connect-cluster coordination overhead, but "
                            + "PRODUCES ZERO RECORDS. The heartbeats topic is empty (or "
                            + "frozen at its previous last-offset if the connector was "
                            + "previously emitting). Every downstream observability check "
                            + "that depends on heartbeats is dark; the operator sees 'MM2 "
                            + "deployed' on the dashboard but the most important liveness "
                            + "signal is missing. The observability gap is invisible in "
                            + "steady-state operation (no alarms fire because no monitoring "
                            + "exists that distinguishes 'connector emits no records' from "
                            + "'connector is broken'); it manifests only during an actual "
                            + "replication-stoppage incident or DR drill — exactly the "
                            + "moments when the heartbeats substrate is most needed. There "
                            + "is NO legitimate production use case for this combination. "
                            + "Fix: either remove the explicit `=false` (the default "
                            + "`=true` is correct), OR remove the MirrorHeartbeatConnector "
                            + "deployment entirely if heartbeats are not wanted. If the "
                            + "concern is heartbeats-topic record volume (the default 1-s "
                            + "interval produces 86 400 records/day), the CORRECT knob is "
                            + "`heartbeats.topic.heartbeat.interval.seconds=60` (1440 "
                            + "records/day — 60x less storage, still enough for "
                            + "monitoring). Do NOT disable emission entirely."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
