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
 * Project-scoped rule. Fires when a Confluent cloud-storage sink
 * connector (S3 / HDFS / GCS) declares {@code rotate.interval.ms} set
 * to a positive value less than 60_000 ms (1 minute).
 *
 * <p>{@code rotate.interval.ms} is the EVENT-TIME flush trigger: when
 * the gap between the first buffered record's timestamp and the latest
 * record's timestamp exceeds this value, the buffer is flushed. Sub-
 * minute event-time rotation produces a storm of small objects in the
 * underlying object store (same pathology as {@link
 * ConnectStorageSinkPartitionDurationMsTooLowRule} and {@link
 * ConnectS3SinkFlushSizeTooSmallRule}, surfaced through a different
 * dial).
 *
 * <p>The rule does NOT fire on absent / zero / negative values
 * (Confluent's default is {@code rotate.interval.ms=-1} = disabled).
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectStorageSinkRotateIntervalMsTooLowRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String ROTATE_INTERVAL_MS = "rotate.interval.ms";
    private static final long THRESHOLD_MS = 60_000L;

    private static final Set<String> STORAGE_SINK_CLASSES = Set.of(
            "io.confluent.connect.s3.S3SinkConnector",
            "io.confluent.connect.hdfs.HdfsSinkConnector",
            "io.confluent.connect.hdfs3.Hdfs3SinkConnector",
            "io.confluent.connect.gcs.GcsSinkConnector");

    private final Severity severity;

    public ConnectStorageSinkRotateIntervalMsTooLowRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_STORAGE_SINK_ROTATE_INTERVAL_MS_TOO_LOW;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null || !STORAGE_SINK_CLASSES.contains(connectorClass)) continue;

            Long rotateIntervalMs = tryParseLong(trimOrNull(p.getProperty(ROTATE_INTERVAL_MS)));
            if (rotateIntervalMs == null || rotateIntervalMs <= 0L || rotateIntervalMs >= THRESHOLD_MS) continue;

            out.add(new Violation(
                    RuleId.CONNECT_STORAGE_SINK_ROTATE_INTERVAL_MS_TOO_LOW, severity,
                    ctx.relativize(e.getKey()), "key:" + ROTATE_INTERVAL_MS, 0,
                    "Connect storage sink " + connectorClass
                            + " declares rotate.interval.ms=" + rotateIntervalMs
                            + " ms — BELOW the " + THRESHOLD_MS + " ms (1-minute) "
                            + "plugin threshold. rotate.interval.ms is the EVENT-"
                            + "TIME flush trigger (one of three OR'd triggers "
                            + "alongside flush.size and rotate.schedule.interval."
                            + "ms); when the gap between the first buffered "
                            + "record's timestamp and the latest record's "
                            + "timestamp exceeds this value, the buffer is "
                            + "flushed — and EVERY flush produces EXACTLY ONE "
                            + "object in the underlying object store. Sub-minute "
                            + "event-time rotation produces a STORM OF SMALL "
                            + "OBJECTS: at realistic production scale (500 topic-"
                            + "partitions × 4 tasks × ~10 partition-keys = 20K "
                            + "active buffers × >= 1 flush/min = >= 28.8M "
                            + "objects/day) — saturating S3 PUT-rate per-prefix "
                            + "limits (3500 PUTs/sec/prefix), inflating S3 LIST "
                            + "cost, regressing downstream query-engine planners "
                            + "(per-file metadata read overhead), and amplifying "
                            + "S3 lifecycle-rule iteration time. The Confluent "
                            + "default is rotate.interval.ms=-1 (EVENT-TIME "
                            + "ROTATION DISABLED — only flush.size and rotate."
                            + "schedule.interval.ms apply). Typical wrong-value "
                            + "origins: (a) 'real-time data lake' misconception "
                            + "(operator confuses freshness with per-flush "
                            + "granularity); (b) confusion with rotate.schedule."
                            + "interval.ms (operator wants 'wall-clock flush "
                            + "every minute' but reaches for the event-time "
                            + "dial); (c) debug-mode setting (5s or 10s) never "
                            + "reverted; (d) tutorial copy-paste at scale "
                            + "(tutorial used 10s on a tiny cluster); (e) backfill "
                            + "job with event-time skew (backfilled records "
                            + "spanning months trigger thousands of event-time "
                            + "rotations per partition). Fix: (1) raise rotate."
                            + "interval.ms to 300_000 (5min) or higher; (2) "
                            + "remove the key entirely to inherit the default -1 "
                            + "(disabled — only flush.size and rotate.schedule."
                            + "interval.ms apply); (3) if sub-minute freshness "
                            + "is genuinely required, use rotate.schedule."
                            + "interval.ms (wall-clock) instead, accept the "
                            + "object-storm cost as a known tradeoff, and "
                            + "suppress this rule with OFF in the .properties "
                            + "documenting the rationale."));
        }
        return out;
    }

    private static Long tryParseLong(String s) {
        if (s == null) return null;
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
