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
 * Project-scoped rule. Fires when a Confluent S3 sink connector
 * ({@code io.confluent.connect.s3.S3SinkConnector}) declares
 * {@code flush.size<100} — every flush trigger produces a near-empty
 * S3 object, leading to an S3 object storm at production scale.
 *
 * <p>The Confluent default {@code flush.size=1000} is a deliberate
 * compromise: small enough to keep heap bounded, large enough that each
 * object holds ~1MB of records under typical 1KB/record workloads. Sub-
 * 100-record flushes have no legitimate production use case — typical
 * origins are debug-mode settings never reverted, conflation with
 * {@code rotate.interval.ms}, and tutorial copy-paste.
 *
 * <p>This is the symmetric pathology of {@link
 * ConnectS3SinkFlushSizeHugeWithoutTimeRotateRule} (heap OOM): too small
 * causes object storm, too large causes worker OOM.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectS3SinkFlushSizeTooSmallRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS_KEY = "connector.class";
    private static final String S3_SINK_CONNECTOR_CLASS =
            "io.confluent.connect.s3.S3SinkConnector";
    private static final String FLUSH_SIZE_KEY = "flush.size";
    private static final int THRESHOLD = 100;

    private final Severity severity;

    public ConnectS3SinkFlushSizeTooSmallRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_S3_SINK_FLUSH_SIZE_TOO_SMALL;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS_KEY));
            if (!S3_SINK_CONNECTOR_CLASS.equals(connectorClass)) continue;

            Integer flushSize = tryParseInt(trimOrNull(p.getProperty(FLUSH_SIZE_KEY)));
            if (flushSize == null || flushSize <= 0 || flushSize >= THRESHOLD) continue;

            out.add(new Violation(
                    RuleId.CONNECT_S3_SINK_FLUSH_SIZE_TOO_SMALL, severity,
                    ctx.relativize(e.getKey()), "key:" + FLUSH_SIZE_KEY, 0,
                    "Confluent S3 sink connector declares flush.size=" + flushSize
                            + " — BELOW the plugin's threshold of " + THRESHOLD
                            + ". The S3 sink writes ONE S3 object per "
                            + "(partition, encoded-partition-key) buffer flush; "
                            + "with flush.size<" + THRESHOLD + " each object holds "
                            + "at most a few dozen records (~10s-100s of KB for "
                            + "typical 1-10KB records) which is FAR below the "
                            + "size at which downstream query engines (Athena, "
                            + "Trino, Presto, BigQuery, Spark) plan efficiently. "
                            + "At production scale (500 topic-partitions × 4 "
                            + "tasks × ~10 partition-keys = 20K active buffers), "
                            + "a low flush.size produces millions of tiny "
                            + "objects per day — saturating S3 PUT-rate per-"
                            + "prefix limits (3500 PUTs/sec/prefix), inflating "
                            + "S3 LIST cost, regressing downstream query-engine "
                            + "planners (per-file metadata read overhead), and "
                            + "amplifying S3 lifecycle-rule iteration time. The "
                            + "Confluent default is flush.size=1000. flush.size<"
                            + THRESHOLD + " has NO legitimate production use "
                            + "case — typical origins are (a) debug-mode "
                            + "setting ('I want to see records flush "
                            + "immediately') never reverted; (b) conflation "
                            + "with rotate.interval.ms (which controls "
                            + "freshness, NOT object size); (c) 'real-time "
                            + "data lake' misconception (smaller does NOT make "
                            + "data fresher); (d) tutorial copy-paste at scale "
                            + "(tutorial used 10 on a 3-partition cluster). "
                            + "Fix: raise flush.size to at least 1000 (the "
                            + "Confluent default), or — if freshness matters "
                            + "more than throughput — pair the larger "
                            + "flush.size with `rotate.interval.ms=<event-"
                            + "time-ms>` (typically 300000=5min) or "
                            + "`rotate.schedule.interval.ms=<wall-clock-ms>` "
                            + "(typically 60000=1min) which bound flush "
                            + "latency without crippling per-object size."));
        }
        return out;
    }

    private static Integer tryParseInt(String s) {
        if (s == null) return null;
        try {
            return Integer.parseInt(s);
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
