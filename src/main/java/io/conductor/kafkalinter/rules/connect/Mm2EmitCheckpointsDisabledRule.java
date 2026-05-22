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
 * Project-scoped rule. Fires when a MirrorMaker 2 checkpoint-connector config
 * ({@code connector.class=org.apache.kafka.connect.mirror.MirrorCheckpointConnector})
 * EXPLICITLY sets {@code emit.checkpoints.enabled=false}. This opts out of
 * the canonical failover-recovery substrate — the
 * {@code <source-alias>.checkpoints.internal} topic that the official
 * {@code RemoteClusterUtils.translateOffsets()} API consumes during
 * primary→DR cutover.
 *
 * <p>Default severity WARNING because the alternative path
 * ({@code sync.group.offsets.enabled=true}, which writes translated
 * offsets directly to the target's {@code __consumer_offsets}) is
 * legitimate. The rule still fires to prompt explicit operator choice.
 */
public final class Mm2EmitCheckpointsDisabledRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String MIRROR_CHECKPOINT_CONNECTOR =
            "org.apache.kafka.connect.mirror.MirrorCheckpointConnector";
    private static final String EMIT_CHECKPOINTS_ENABLED = "emit.checkpoints.enabled";
    private static final String SYNC_GROUP_OFFSETS_ENABLED = "sync.group.offsets.enabled";

    private final Severity severity;

    public Mm2EmitCheckpointsDisabledRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.MM2_EMIT_CHECKPOINTS_DISABLED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!MIRROR_CHECKPOINT_CONNECTOR.equals(connectorClass)) continue;

            String raw = trimOrNull(p.getProperty(EMIT_CHECKPOINTS_ENABLED));
            if (raw == null) continue;
            if (!"false".equalsIgnoreCase(raw)) continue;

            String syncRaw = trimOrNull(p.getProperty(SYNC_GROUP_OFFSETS_ENABLED));
            boolean syncGroupOffsetsExplicitlyTrue = "true".equalsIgnoreCase(syncRaw);

            String file = ctx.relativize(e.getKey());
            String alternativePathNote = syncGroupOffsetsExplicitlyTrue
                    ? "An alternative failover-recovery path IS active here — "
                    + "`sync.group.offsets.enabled=true` is set in the same file. "
                    + "That path writes translated offsets directly to the target's "
                    + "`__consumer_offsets` topic, giving transparent consumer "
                    + "failover without application-side translation code. This "
                    + "rule still fires to prompt EXPLICIT, DOCUMENTED choice: if "
                    + "the operator deliberately uses the sync-group-offsets-only "
                    + "path (e.g., to reduce target-cluster topic count by skipping "
                    + "the checkpoints internal topic), suppress this rule via "
                    + "`<MM2_EMIT_CHECKPOINTS_DISABLED>OFF</MM2_EMIT_CHECKPOINTS_DISABLED>` "
                    + "in the plugin config WITH a comment documenting the "
                    + "rationale. NOTE: audit tooling and DR-readiness dashboards "
                    + "that depend on `RemoteClusterUtils.translateOffsets()` or "
                    + "direct reads of `<source-alias>.checkpoints.internal` WILL "
                    + "break with this configuration; verify your operational "
                    + "tooling does not depend on the checkpoints topic before "
                    + "suppressing."
                    : "NO ALTERNATIVE failover-recovery path is active — "
                    + "`sync.group.offsets.enabled` is absent (default `false`) or "
                    + "explicitly `false`. With BOTH paths off, MirrorCheckpointConnector "
                    + "does NOTHING failover-useful. During a primary→DR cutover, "
                    + "consumers connecting to the DR cluster have no translated "
                    + "offsets available anywhere; they must restart from `earliest` "
                    + "(replaying every retained record — at-least-once double-billing "
                    + "for non-idempotent processing) or `latest` (skipping every "
                    + "record between the source-cluster's last commit and the failover "
                    + "moment — silent data loss). This is the CATASTROPHIC "
                    + "dual-disable case. Fix: set `emit.checkpoints.enabled=true` "
                    + "(restore the default), AND/OR set "
                    + "`sync.group.offsets.enabled=true` (enable the alternative "
                    + "path). Production MM2 deployments should have AT LEAST ONE of "
                    + "these paths active.";

            out.add(new Violation(
                    RuleId.MM2_EMIT_CHECKPOINTS_DISABLED, severity,
                    file, "key:" + EMIT_CHECKPOINTS_ENABLED, 0,
                    "MirrorMaker 2 checkpoint connector (" + connectorClass + ") has "
                            + EMIT_CHECKPOINTS_ENABLED + "=" + raw + " — this OPTS OUT of "
                            + "the canonical failover-recovery substrate. Normally, "
                            + "MirrorCheckpointConnector periodically (every "
                            + "`emit.checkpoints.interval.seconds`, default 60 s) reads the "
                            + "source-cluster's `__consumer_offsets` topic, translates each "
                            + "source-offset into the corresponding target-offset using the "
                            + "offset-sync table maintained by MirrorSourceConnector, and "
                            + "writes the translated offsets to "
                            + "`<source-alias>.checkpoints.internal` on the target. During "
                            + "a primary→DR cutover, consumer applications (or operator "
                            + "tooling) call `RemoteClusterUtils.translateOffsets()` to "
                            + "read this topic and discover the target-cluster offsets to "
                            + "resume from. With `emit.checkpoints.enabled=false`, the "
                            + "periodic write is not scheduled; the checkpoints topic is "
                            + "never updated (empty or stale). " + alternativePathNote
                            + " The rule defaults to WARNING (not ERROR) because the "
                            + "sync-group-offsets-only path is legitimate when explicitly "
                            + "chosen. The detection: trim, case-insensitive compare of "
                            + "`emit.checkpoints.enabled` to `false`; only fires on the "
                            + "exact `MirrorCheckpointConnector` class. The rule does NOT "
                            + "fire on `MirrorSourceConnector` or "
                            + "`MirrorHeartbeatConnector`."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
