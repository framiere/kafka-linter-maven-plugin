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
 * Project-scoped rule. Fires when a Confluent HDFS sink connector
 * (HDFS 3.x {@code Hdfs3SinkConnector} or legacy 2.x {@code HdfsSinkConnector})
 * declares a very large {@code flush.size} (≥100000) but no time-based
 * rotation safety net.
 *
 * <p>The HDFS sink buffers records in worker heap per
 * (TopicPartition, encoded-partition-key) until {@code flush.size}
 * records have accumulated; only then does the temporary HDFS file
 * become visible to downstream Hive/Spark/Trino. Without
 * {@code rotate.interval.ms} or {@code rotate.schedule.interval.ms},
 * the buffer grows unbounded under realistic load and the Connect
 * worker OOMs before flush ever fires.
 *
 * <p>Identical detection logic to
 * {@link ConnectS3SinkFlushSizeHugeWithoutTimeRotateRule} — the two
 * sinks share buffering semantics.
 */
public final class ConnectHdfsSinkFlushSizeHugeWithoutTimeRotateRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final Set<String> HDFS_SINK_CLASSES = Set.of(
            "io.confluent.connect.hdfs3.Hdfs3SinkConnector",
            "io.confluent.connect.hdfs.HdfsSinkConnector");
    private static final String FLUSH_SIZE = "flush.size";
    private static final String ROTATE_INTERVAL_MS = "rotate.interval.ms";
    private static final String ROTATE_SCHEDULE_INTERVAL_MS = "rotate.schedule.interval.ms";
    private static final int FLUSH_SIZE_THRESHOLD = 100_000;

    private final Severity severity;

    public ConnectHdfsSinkFlushSizeHugeWithoutTimeRotateRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_HDFS_SINK_FLUSH_SIZE_HUGE_WITHOUT_TIME_ROTATE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null || !HDFS_SINK_CLASSES.contains(connectorClass)) continue;

            Integer flushSize = tryParseInt(trimOrNull(p.getProperty(FLUSH_SIZE)));
            if (flushSize == null || flushSize < FLUSH_SIZE_THRESHOLD) continue;

            if (hasPositiveLong(p, ROTATE_INTERVAL_MS)) continue;
            if (hasPositiveLong(p, ROTATE_SCHEDULE_INTERVAL_MS)) continue;

            out.add(new Violation(
                    RuleId.CONNECT_HDFS_SINK_FLUSH_SIZE_HUGE_WITHOUT_TIME_ROTATE, severity,
                    ctx.relativize(e.getKey()), "key:" + FLUSH_SIZE, 0,
                    "Connect HDFS sink connector (" + connectorClass + ") has flush.size="
                            + flushSize + " (>= " + FLUSH_SIZE_THRESHOLD + ") AND no "
                            + "time-based rotation set (rotate.interval.ms and "
                            + "rotate.schedule.interval.ms are both missing/zero/negative). "
                            + "The HDFS sink buffers records in worker HEAP per "
                            + "(TopicPartition, encoded-partition-key) until flush.size "
                            + "records have accumulated, then commits the +tmp/ file to "
                            + "its final HDFS path (visible to Hive/Spark/Trino). With no "
                            + "time-rotation safety net, the buffer grows unbounded (1M "
                            + "records × 1KB = 1GB heap per partition per task) and the "
                            + "worker OOMs before the flush ever happens, taking down ALL "
                            + "connectors on that worker. Additional HDFS-specific risk: "
                            + "the temporary file holds an open HDFS lease for the entire "
                            + "buffering duration; on worker crash, the lease must wait "
                            + "for NameNode recovery (default ~1h) before the next start "
                            + "can open the same path. Either reduce flush.size below "
                            + FLUSH_SIZE_THRESHOLD + ", OR add rotate.interval.ms=<event-"
                            + "time-ms> (e.g., 300000 for 5 min) or "
                            + "rotate.schedule.interval.ms=<wall-clock-ms> (e.g., 60000 "
                            + "for 1 min) to bound buffer growth in time."));
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
