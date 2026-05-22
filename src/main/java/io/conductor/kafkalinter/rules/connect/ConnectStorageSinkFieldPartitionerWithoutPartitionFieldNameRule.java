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
 * (S3/GCS/HDFS/HDFS3) sets {@code partitioner.class=io.confluent.connect.storage.partitioner.FieldPartitioner}
 * but does NOT set {@code partition.field.name} (absent, empty, or whitespace).
 *
 * <p>The FieldPartitioner requires {@code partition.field.name} (no default) —
 * without it, the connector either fails at startup with ConfigException
 * (Confluent Platform 7.0+) or writes files to broken partition paths
 * ({@code <topic>/null=null/<file>}) in earlier versions.
 *
 * <p>Emits one violation per offending file.
 */
public final class ConnectStorageSinkFieldPartitionerWithoutPartitionFieldNameRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String PARTITIONER_CLASS = "partitioner.class";
    private static final String PARTITION_FIELD_NAME = "partition.field.name";
    private static final String FIELD_PARTITIONER_CLASS =
            "io.confluent.connect.storage.partitioner.FieldPartitioner";

    private static final Set<String> STORAGE_SINK_CLASSES = Set.of(
            "io.confluent.connect.s3.S3SinkConnector",
            "io.confluent.connect.gcs.GcsSinkConnector",
            "io.confluent.connect.hdfs.HdfsSinkConnector",
            "io.confluent.connect.hdfs3.Hdfs3SinkConnector");

    private final Severity severity;

    public ConnectStorageSinkFieldPartitionerWithoutPartitionFieldNameRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_STORAGE_SINK_FIELD_PARTITIONER_WITHOUT_PARTITION_FIELD_NAME;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null || !STORAGE_SINK_CLASSES.contains(connectorClass)) continue;

            String partitionerClass = trimOrNull(p.getProperty(PARTITIONER_CLASS));
            if (!FIELD_PARTITIONER_CLASS.equals(partitionerClass)) continue;

            String fieldName = trimOrNull(p.getProperty(PARTITION_FIELD_NAME));
            if (fieldName != null) continue;

            out.add(new Violation(
                    RuleId.CONNECT_STORAGE_SINK_FIELD_PARTITIONER_WITHOUT_PARTITION_FIELD_NAME, severity,
                    ctx.relativize(e.getKey()), "key:" + PARTITIONER_CLASS, 0,
                    "Connect storage sink " + connectorClass + " sets partitioner.class="
                            + FIELD_PARTITIONER_CLASS + " but partition.field.name is absent or "
                            + "empty. FieldPartitioner uses partition.field.name (no default) to "
                            + "identify the record-value field whose value becomes the partition "
                            + "key in the output path (`<topic>/<field>=<value>/<file>`). Without "
                            + "it, the connector either fails at startup with `ConfigException: "
                            + "Partition field name(s) should be specified for FieldPartitioner` "
                            + "(Confluent Platform 7.0+), throws DataException on the first "
                            + "record (Confluent 6.x), or writes files to literal "
                            + "`<topic>/null=null/<file>` paths that downstream Hive/Athena/Glue "
                            + "tooling cannot query (older versions). Fix: either set "
                            + "partition.field.name=<field> with a field name that exists in the "
                            + "record's value Struct (or a comma-separated list for multi-level "
                            + "partitioning like `partition.field.name=region,product_category`), "
                            + "OR change partitioner.class to a different strategy "
                            + "(DefaultPartitioner for Kafka-partition-id-based; TimeBasedPartitioner / "
                            + "DailyPartitioner / HourlyPartitioner for time-based) if field-based "
                            + "partitioning was not actually intended. Sibling rules: "
                            + "CONNECT_STORAGE_SINK_PATH_FORMAT_WITHOUT_TIME_PARTITIONER (inverse "
                            + "anti-pattern — TimeBasedPartitioner-only config set when a "
                            + "non-time partitioner is selected, silent), "
                            + "CONNECT_STORAGE_SINK_TIMESTAMP_EXTRACTOR_WALLCLOCK "
                            + "(TimeBasedPartitioner-related anti-pattern)."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
