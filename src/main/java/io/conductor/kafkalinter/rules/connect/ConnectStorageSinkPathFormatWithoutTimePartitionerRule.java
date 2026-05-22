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
 * Project-scoped rule. Fires when a Confluent storage-sink connector
 * (S3/HDFS/HDFS3/GCS) sets {@code path.format} (a TimeBasedPartitioner-only
 * configuration) while {@code partitioner.class} is unset (defaults to
 * {@code DefaultPartitioner}) or set to a non-time-based partitioner
 * ({@code DefaultPartitioner} / {@code FieldPartitioner}).
 *
 * <p>{@code path.format} is read exclusively by
 * {@code TimeBasedPartitioner#configure} (and subclasses
 * {@code DailyPartitioner} / {@code HourlyPartitioner}); the non-time-based
 * partitioners IGNORE it. The framework does not cross-validate keys against
 * {@code partitioner.class}, so the misconfiguration is entirely silent at
 * startup. Files land at default {@code partition=<N>} paths while the
 * operator's downstream tooling expects {@code year=}/{@code month=}/{@code day=}
 * directories and finds none.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectStorageSinkPathFormatWithoutTimePartitionerRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String PARTITIONER_CLASS = "partitioner.class";
    private static final String PATH_FORMAT = "path.format";

    private static final Set<String> STORAGE_SINK_CLASSES = Set.of(
            "io.confluent.connect.s3.S3SinkConnector",
            "io.confluent.connect.hdfs.HdfsSinkConnector",
            "io.confluent.connect.hdfs3.Hdfs3SinkConnector",
            "io.confluent.connect.gcs.GcsSinkConnector");

    private static final Set<String> NON_TIME_BASED_PARTITIONERS = Set.of(
            "io.confluent.connect.storage.partitioner.DefaultPartitioner",
            "io.confluent.connect.storage.partitioner.FieldPartitioner");

    private final Severity severity;

    public ConnectStorageSinkPathFormatWithoutTimePartitionerRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_STORAGE_SINK_PATH_FORMAT_WITHOUT_TIME_PARTITIONER;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null || !STORAGE_SINK_CLASSES.contains(connectorClass)) continue;

            String pathFormat = trimOrNull(p.getProperty(PATH_FORMAT));
            if (pathFormat == null) continue;

            String partitionerClass = trimOrNull(p.getProperty(PARTITIONER_CLASS));
            // Default is DefaultPartitioner — non-time-based — so absent triggers.
            boolean isNonTimeBased =
                    partitionerClass == null || NON_TIME_BASED_PARTITIONERS.contains(partitionerClass);
            if (!isNonTimeBased) continue;

            String observed = partitionerClass == null ? "<unset, defaults to DefaultPartitioner>" : partitionerClass;
            out.add(new Violation(
                    RuleId.CONNECT_STORAGE_SINK_PATH_FORMAT_WITHOUT_TIME_PARTITIONER, severity,
                    ctx.relativize(e.getKey()), "key:" + PATH_FORMAT, 0,
                    "Connect storage sink " + connectorClass + " sets path.format=" + pathFormat
                            + " but partitioner.class=" + observed + " — path.format is read ONLY "
                            + "by TimeBasedPartitioner (and its DailyPartitioner / HourlyPartitioner "
                            + "subclasses). DefaultPartitioner and FieldPartitioner IGNORE this key "
                            + "entirely: the framework loads the string, validates its type, but the "
                            + "partitioner never calls config.get(PATH_FORMAT_CONFIG). The connector "
                            + "starts cleanly, processes records cleanly, and writes files to default "
                            + "partition=<N> paths while downstream Hive/Athena/Glue tooling looks for "
                            + "year=*/month=*/day=* directories and finds nothing. There is NO startup "
                            + "warning, NO config-validation error, NO log line — the misconfiguration "
                            + "is entirely silent. Fix: either set "
                            + "partitioner.class=io.confluent.connect.storage.partitioner.TimeBasedPartitioner "
                            + "(or DailyPartitioner / HourlyPartitioner) to match the path.format intent, "
                            + "OR delete the path.format line if non-time-based partitioning was actually "
                            + "intended."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
