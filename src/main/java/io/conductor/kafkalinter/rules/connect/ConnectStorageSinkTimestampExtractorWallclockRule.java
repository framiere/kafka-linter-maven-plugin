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
 * connector (S3 / HDFS / GCS) uses a time-based partitioner
 * ({@code TimeBasedPartitioner}, {@code DailyPartitioner}, or
 * {@code HourlyPartitioner}) without setting {@code timestamp.extractor}
 * — the framework default is {@code Wallclock}, which partitions by
 * Connect's processing time rather than the record's event-time.
 *
 * <p>Detection:
 * <ol>
 *   <li>connector.class matches one of the four Confluent storage sink
 *       classes (S3, HDFS, HDFS3, GCS);</li>
 *   <li>partitioner.class matches one of the three time-based
 *       partitioners (TimeBased, Daily, Hourly);</li>
 *   <li>timestamp.extractor is absent OR equals {@code Wallclock}
 *       (case-sensitive, the class-name suffix used by the framework).</li>
 * </ol>
 *
 * <p>Emits one ERROR-severity violation per offending file.
 */
public final class ConnectStorageSinkTimestampExtractorWallclockRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String PARTITIONER_CLASS = "partitioner.class";
    private static final String TIMESTAMP_EXTRACTOR = "timestamp.extractor";

    private static final Set<String> STORAGE_SINK_CLASSES = Set.of(
            "io.confluent.connect.s3.S3SinkConnector",
            "io.confluent.connect.hdfs.HdfsSinkConnector",
            "io.confluent.connect.hdfs3.Hdfs3SinkConnector",
            "io.confluent.connect.gcs.GcsSinkConnector");

    private static final Set<String> TIME_BASED_PARTITIONERS = Set.of(
            "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
            "io.confluent.connect.storage.partitioner.DailyPartitioner",
            "io.confluent.connect.storage.partitioner.HourlyPartitioner");

    private final Severity severity;

    public ConnectStorageSinkTimestampExtractorWallclockRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_STORAGE_SINK_TIMESTAMP_EXTRACTOR_WALLCLOCK;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null || !STORAGE_SINK_CLASSES.contains(connectorClass)) continue;

            String partitionerClass = trimOrNull(p.getProperty(PARTITIONER_CLASS));
            if (partitionerClass == null || !TIME_BASED_PARTITIONERS.contains(partitionerClass)) continue;

            String extractor = trimOrNull(p.getProperty(TIMESTAMP_EXTRACTOR));
            if (extractor != null && !"Wallclock".equals(extractor)) continue;

            String observed = extractor == null ? "<unset>" : extractor;
            String keyForLoc = extractor == null ? "key:" + PARTITIONER_CLASS : "key:" + TIMESTAMP_EXTRACTOR;
            out.add(new Violation(
                    RuleId.CONNECT_STORAGE_SINK_TIMESTAMP_EXTRACTOR_WALLCLOCK, severity,
                    ctx.relativize(e.getKey()), keyForLoc, 0,
                    "Connect storage sink " + connectorClass + " uses time-based partitioner "
                            + partitionerClass + " with timestamp.extractor=" + observed
                            + " — the kafka-connect-storage-common DEFAULT is Wallclock, which "
                            + "partitions records by `System.currentTimeMillis()` AT PROCESSING "
                            + "TIME, NOT by the record's event-time. On replay (consumer reset, "
                            + "worker restart), records spanning days/weeks all land in TODAY's "
                            + "partition — silently corrupting time-partitioned data lakes. "
                            + "Similarly, GC pauses, schema-registry slowness, queue stalls all "
                            + "cause records to bunch into the partition for the wall-clock "
                            + "moment the stall ENDED, not when the events actually occurred. "
                            + "Set timestamp.extractor=Record (uses the Kafka record's timestamp "
                            + "metadata — defaults to producer-side CreateTime, usually correct) "
                            + "or timestamp.extractor=RecordField with timestamp.field=<your-"
                            + "event-time-field> (extracts a domain field from the record value "
                            + "— the gold standard for event-time partitioning)."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
