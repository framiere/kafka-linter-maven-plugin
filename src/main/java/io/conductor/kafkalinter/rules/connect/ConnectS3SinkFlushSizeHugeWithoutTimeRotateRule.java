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
 * Project-scoped rule. Fires when a Kafka Connect S3 sink connector
 * declares a very large {@code flush.size} (≥100000) but no time-based
 * rotation safety net.
 *
 * <p>The S3 sink buffers records in worker heap (per-partition
 * ByteArrayOutputStream) until {@code flush.size} records have
 * accumulated. Without {@code rotate.interval.ms} or
 * {@code rotate.schedule.interval.ms} set with a positive value, the
 * buffer grows unbounded under realistic load and the worker OOMs
 * before the flush ever happens.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectS3SinkFlushSizeHugeWithoutTimeRotateRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String S3_SINK_CLASS = "io.confluent.connect.s3.S3SinkConnector";
    private static final String FLUSH_SIZE = "flush.size";
    private static final String ROTATE_INTERVAL_MS = "rotate.interval.ms";
    private static final String ROTATE_SCHEDULE_INTERVAL_MS = "rotate.schedule.interval.ms";
    private static final int FLUSH_SIZE_THRESHOLD = 100_000;

    private final Severity severity;

    public ConnectS3SinkFlushSizeHugeWithoutTimeRotateRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_S3_SINK_FLUSH_SIZE_HUGE_WITHOUT_TIME_ROTATE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!S3_SINK_CLASS.equals(trimOrNull(p.getProperty(CONNECTOR_CLASS)))) continue;

            Integer flushSize = tryParseInt(trimOrNull(p.getProperty(FLUSH_SIZE)));
            if (flushSize == null || flushSize < FLUSH_SIZE_THRESHOLD) continue;

            if (hasPositiveLong(p, ROTATE_INTERVAL_MS)) continue;
            if (hasPositiveLong(p, ROTATE_SCHEDULE_INTERVAL_MS)) continue;

            out.add(new Violation(
                    RuleId.CONNECT_S3_SINK_FLUSH_SIZE_HUGE_WITHOUT_TIME_ROTATE, severity,
                    ctx.relativize(e.getKey()), "key:" + FLUSH_SIZE, 0,
                    "Connect S3 sink connector has flush.size=" + flushSize
                            + " (>= " + FLUSH_SIZE_THRESHOLD + ") AND no time-based "
                            + "rotation set (rotate.interval.ms and "
                            + "rotate.schedule.interval.ms are both missing/zero/negative). "
                            + "The S3 sink buffers records in worker HEAP until flush.size "
                            + "records have accumulated per partition; with no time-rotation "
                            + "safety net, the buffer grows unbounded under realistic load "
                            + "(1M records × 1KB = 1GB heap per partition per task) and the "
                            + "worker OOMs before the flush ever happens, taking down ALL "
                            + "connectors on that worker. Either reduce flush.size below "
                            + FLUSH_SIZE_THRESHOLD + ", OR add rotate.interval.ms=<event-"
                            + "time-ms> (e.g., 300000 for 5 min) or "
                            + "rotate.schedule.interval.ms=<wall-clock-ms> (e.g., 60000 for "
                            + "1 min) to bound buffer growth in time."));
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
