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
 * connector (S3 / HDFS / GCS) is configured with the base
 * {@code TimeBasedPartitioner} AND {@code partition.duration.ms} set to
 * a value less than 60_000 ms (1 minute).
 *
 * <p>Sub-minute bucket widths produce a storm of small objects in the
 * underlying object store, punishing S3 PUT-rate per-prefix limits,
 * downstream query engines (Athena/Trino per-file overhead), and
 * lifecycle-rule iteration.
 *
 * <p>The rule excludes {@code DailyPartitioner} and {@code HourlyPartitioner}
 * because those subclasses HARDCODE their durations and IGNORE any
 * explicit {@code partition.duration.ms} in the .properties file.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectStorageSinkPartitionDurationMsTooLowRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String PARTITIONER_CLASS = "partitioner.class";
    private static final String PARTITION_DURATION_MS = "partition.duration.ms";
    private static final String TIME_BASED_PARTITIONER =
            "io.confluent.connect.storage.partitioner.TimeBasedPartitioner";
    private static final long THRESHOLD_MS = 60_000L;

    private static final Set<String> STORAGE_SINK_CLASSES = Set.of(
            "io.confluent.connect.s3.S3SinkConnector",
            "io.confluent.connect.hdfs.HdfsSinkConnector",
            "io.confluent.connect.hdfs3.Hdfs3SinkConnector",
            "io.confluent.connect.gcs.GcsSinkConnector");

    private final Severity severity;

    public ConnectStorageSinkPartitionDurationMsTooLowRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_STORAGE_SINK_PARTITION_DURATION_MS_TOO_LOW;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null || !STORAGE_SINK_CLASSES.contains(connectorClass)) continue;

            String partitionerClass = trimOrNull(p.getProperty(PARTITIONER_CLASS));
            if (!TIME_BASED_PARTITIONER.equals(partitionerClass)) continue;

            Long durationMs = tryParseLong(trimOrNull(p.getProperty(PARTITION_DURATION_MS)));
            if (durationMs == null || durationMs <= 0L || durationMs >= THRESHOLD_MS) continue;

            out.add(new Violation(
                    RuleId.CONNECT_STORAGE_SINK_PARTITION_DURATION_MS_TOO_LOW, severity,
                    ctx.relativize(e.getKey()), "key:" + PARTITION_DURATION_MS, 0,
                    "Connect storage sink " + connectorClass + " uses base "
                            + "TimeBasedPartitioner with partition.duration.ms=" + durationMs
                            + " ms — BELOW the " + THRESHOLD_MS + " ms (1-minute) plugin "
                            + "threshold. partition.duration.ms is the WIDTH of each "
                            + "time bucket the partitioner creates; sub-minute bucket "
                            + "widths produce a STORM OF SMALL OBJECTS in the "
                            + "underlying object store. Concrete consequences: (1) S3 "
                            + "PUT-rate per-prefix limit is 3500 PUTs/sec — sub-minute "
                            + "buckets across many topic-partitions can saturate it, "
                            + "producing HTTP 503 `SlowDown` errors and SDK retry storms; "
                            + "(2) S3 LIST cost (priced per 1000 keys) grows linearly "
                            + "with object count — Athena/Trino queries scanning the "
                            + "prefix become expensive; (3) downstream query engines "
                            + "(Athena, Trino, Presto, BigQuery, Spark) optimize for "
                            + "fewer larger objects — thousands of small objects per "
                            + "query cause severe planner regression (per-file "
                            + "metadata read, per-file open/close overhead); (4) HDFS "
                            + "NameNode metadata pressure (one inode per file) crowds "
                            + "out other workloads at scale; (5) S3 lifecycle rules "
                            + "(delete-after-N-days, transition-to-Glacier) iterate "
                            + "per-object — millions of small objects amplify "
                            + "lifecycle-rule processing time. The Confluent ConfigDef "
                            + "default is 86_400_000 ms (24 hours); typical correct "
                            + "values are 3_600_000 (1 hour) for hourly, 86_400_000 "
                            + "(24 hours) for daily. Typical wrong-value origins: (a) "
                            + "confusion with `rotate.interval.ms` or "
                            + "`rotate.schedule.interval.ms` (those control flush "
                            + "freshness; partition.duration.ms controls bucket "
                            + "WIDTH); (b) debug-mode setting (1s or 10s) never "
                            + "reverted; (c) 'real-time data lake' misconception — "
                            + "smaller buckets do NOT make data fresher (that's "
                            + "rotate.interval.ms's job); (d) tutorial copy-paste at "
                            + "scale (tutorial used 60s on a tiny cluster). Fix: set "
                            + "partition.duration.ms to a value appropriate for the "
                            + "downstream query pattern (3_600_000 = 1h is a reasonable "
                            + "middle ground), or remove the explicit key to inherit "
                            + "the 24-hour default. NOTE: this rule does NOT fire on "
                            + "DailyPartitioner or HourlyPartitioner — those subclasses "
                            + "HARDCODE their durations and IGNORE the explicit key."));
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
