package sample;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;

/**
 * RULE: ADMIN_LIST_OFFSETS_NO_OPTIONS.
 *
 * Fires when {@code Admin.listOffsets(Map<TopicPartition, OffsetSpec>)} is
 * called without a {@code ListOffsetsOptions} argument. Two silent
 * defaults compound:
 *
 * <ol>
 *   <li>Isolation level defaults to {@code READ_UNCOMMITTED} — the
 *       returned "latest" offset is the broker's high-watermark (HWM),
 *       which INCLUDES records from in-flight (potentially-aborted)
 *       transactions. A {@code read_committed} consumer cannot ever read
 *       past the last stable offset (LSO), which is strictly less than the
 *       HWM during open transactions. Result: monitoring tools that
 *       compute {@code lag = listOffsets.latest - consumerCommit} report
 *       PHANTOM lag of (HWM - LSO) records on every transactional topic.</li>
 *   <li>Request timeout inherits the AdminClient default (~30 s), short
 *       for the per-broker fan-out this operation does.</li>
 * </ol>
 *
 * <p>Historical compatibility note: {@code listOffsets} predates the
 * transactional producer API by an entire release. The default was set
 * when there was literally nothing uncommitted to read FROM. With EOS-v2
 * the default Streams behavior and transactional producers now common in
 * ETL pipelines, the default is a present-day footgun.
 *
 * <p>Failure mode (consumer-lag monitor pages oncall on phantom lag):
 * <ol>
 *   <li>A Streams app with {@code processing.guarantee=exactly_once_v2}
 *       consumes from a transactional input topic.</li>
 *   <li>The lag monitor calls
 *       {@code admin.listOffsets(partitions, OffsetSpec.latest())} (no
 *       options) and subtracts the consumer-group's committed offset.</li>
 *   <li>EOS transactions commit every 100 ms; during each open window
 *       HWM - LSO ≈ 10K records per partition × 200 partitions ≈ 2 million
 *       records of phantom 'lag'.</li>
 *   <li>Grafana alert: "consumer lag > 1 million records, paging oncall".
 *       Six false pages in one week before someone reads the AdminClient
 *       docs.</li>
 * </ol>
 *
 * <p>Fix: {@code new ListOffsetsOptions(IsolationLevel.READ_COMMITTED)
 * .timeoutMs(60_000)} (isolation level is a constructor argument, not a
 * fluent setter) to match what every {@code read_committed} consumer (and
 * every EOS Streams app) will actually see.
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code (Ljava/util/Map;)Lorg/apache/kafka/clients/admin/ListOffsetsResult;}
 *   <li>With options: {@code (Ljava/util/Map;Lorg/apache/kafka/clients/admin/ListOffsetsOptions;)Lorg/apache/kafka/clients/admin/ListOffsetsResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/ListOffsetsOptions;}
 * in the descriptor.
 */
public final class BadAdminListOffsetsNoOptions {

    private static Map<TopicPartition, OffsetSpec> latestEnds() {
        return Map.of(
                new TopicPartition("payments.events", 0), OffsetSpec.latest(),
                new TopicPartition("payments.events", 1), OffsetSpec.latest(),
                new TopicPartition("payments.events", 2), OffsetSpec.latest());
    }

    /** Anti-pattern: no ListOffsetsOptions on Admin (INVOKEINTERFACE) — FIRES. */
    public ListOffsetsResult adminNoOptions(Admin admin) {
        return admin.listOffsets(latestEnds()); // FIRES — no options, READ_UNCOMMITTED
    }

    /** Anti-pattern: no ListOffsetsOptions on AdminClient (INVOKEVIRTUAL) — FIRES. */
    public ListOffsetsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.listOffsets(latestEnds()); // FIRES — no options, READ_UNCOMMITTED
        }
    }

    /** Control: ListOffsetsOptions with READ_COMMITTED + timeout — must NOT fire. */
    public ListOffsetsResult adminWithOptions(Admin admin) {
        ListOffsetsOptions opts = new ListOffsetsOptions(IsolationLevel.READ_COMMITTED)
                .timeoutMs(60_000);
        return admin.listOffsets(latestEnds(), opts);
    }
}
