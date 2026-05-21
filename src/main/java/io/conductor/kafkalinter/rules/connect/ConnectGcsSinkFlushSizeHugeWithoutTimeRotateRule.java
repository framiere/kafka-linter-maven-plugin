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
 * Project-scoped rule. Fires when the Confluent GCS sink connector
 * declares a very large {@code flush.size} (≥100000) but no time-based
 * rotation safety net.
 *
 * <p>The GCS sink uses the same buffering model as the S3 and HDFS
 * sinks: per-(TopicPartition, encoded-partition-key) heap buffer,
 * flushed on any of flush.size / rotate.interval.ms /
 * rotate.schedule.interval.ms. Without a time rotation safety valve,
 * a large flush.size leads to unbounded heap growth and worker OOM,
 * plus GCS-specific abandoned-resumable-upload quota exhaustion.
 *
 * <p>Identical detection logic to
 * {@link ConnectS3SinkFlushSizeHugeWithoutTimeRotateRule} and
 * {@link ConnectHdfsSinkFlushSizeHugeWithoutTimeRotateRule}.
 */
public final class ConnectGcsSinkFlushSizeHugeWithoutTimeRotateRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String GCS_SINK_CLASS = "io.confluent.connect.gcs.GcsSinkConnector";
    private static final String FLUSH_SIZE = "flush.size";
    private static final String ROTATE_INTERVAL_MS = "rotate.interval.ms";
    private static final String ROTATE_SCHEDULE_INTERVAL_MS = "rotate.schedule.interval.ms";
    private static final int FLUSH_SIZE_THRESHOLD = 100_000;

    private final Severity severity;

    public ConnectGcsSinkFlushSizeHugeWithoutTimeRotateRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_GCS_SINK_FLUSH_SIZE_HUGE_WITHOUT_TIME_ROTATE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!GCS_SINK_CLASS.equals(trimOrNull(p.getProperty(CONNECTOR_CLASS)))) continue;

            Integer flushSize = tryParseInt(trimOrNull(p.getProperty(FLUSH_SIZE)));
            if (flushSize == null || flushSize < FLUSH_SIZE_THRESHOLD) continue;

            if (hasPositiveLong(p, ROTATE_INTERVAL_MS)) continue;
            if (hasPositiveLong(p, ROTATE_SCHEDULE_INTERVAL_MS)) continue;

            out.add(new Violation(
                    RuleId.CONNECT_GCS_SINK_FLUSH_SIZE_HUGE_WITHOUT_TIME_ROTATE, severity,
                    ctx.relativize(e.getKey()), "key:" + FLUSH_SIZE, 0,
                    "Connect GCS sink connector has flush.size=" + flushSize
                            + " (>= " + FLUSH_SIZE_THRESHOLD + ") AND no time-based "
                            + "rotation set (rotate.interval.ms and "
                            + "rotate.schedule.interval.ms are both missing/zero/negative). "
                            + "The GCS sink buffers records in worker HEAP per "
                            + "(TopicPartition, encoded-partition-key) until flush.size "
                            + "records have accumulated, then issues a single GCS upload. "
                            + "With no time-rotation safety net, the buffer grows unbounded "
                            + "(1M records × 1KB = 1GB heap per partition per task) and the "
                            + "worker OOMs before the flush ever happens, taking down ALL "
                            + "connectors on that worker. Additional GCS-specific risk: "
                            + "the in-flight resumable-upload session token is abandoned on "
                            + "worker crash and counts against the project's GCS API quota "
                            + "for ~24h, potentially blocking ALL further uploads in the "
                            + "project. Either reduce flush.size below " + FLUSH_SIZE_THRESHOLD
                            + ", OR add rotate.interval.ms=<event-time-ms> (e.g., 300000 "
                            + "for 5 min) or rotate.schedule.interval.ms=<wall-clock-ms> "
                            + "(e.g., 60000 for 1 min) to bound buffer growth in time."));
        }
        return out;
    }

    private static boolean hasPositiveLong(Properties p, String key) {
        Long v = tryParseLong(trimOrNull(p.getProperty(key)));
        return v != null && v > 0L;
    }

    private static Integer tryParseInt(String s) {
        if (s == null) return null;
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException ignored) {
            return null;
        }
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
