package sample;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteRecordsOptions;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;

/**
 * RULE: ADMIN_DELETE_RECORDS_NO_OPTIONS.
 *
 * Fires when {@code Admin.deleteRecords(Map<TopicPartition, RecordsToDelete>)}
 * is called without a {@code DeleteRecordsOptions} argument. The call inherits
 * the default ~30 s AdminClient timeout. {@code deleteRecords} is the only
 * Kafka API that can advance the log-start offset of a topic-partition
 * (purging every record below the target offset) without dropping and
 * recreating the topic, and the truncation is IRREVERSIBLE: once the
 * log-start offset moves forward and replicates, consumers receive
 * {@code OffsetOutOfRangeException} for any prior offset.
 *
 * <p>Failure mode (GDPR cleanup, half-applied):
 * <ol>
 *   <li>A privacy-engineering team's nightly script computes per-partition
 *       cutoff offsets and calls
 *       {@code admin.deleteRecords(cutoffs).all().get()}.</li>
 *   <li>The topic has 400 partitions across 10 brokers; on a heavy night
 *       the call hits the 30 s default timeout mid-operation.</li>
 *   <li>{@link DeleteRecordsResult#all()} completes exceptionally; the
 *       per-partition futures expose mixed success/failure but the catch
 *       block ignores them and only logs "partial failure".</li>
 *   <li>Audit a week later: 38 partitions still hold records below the
 *       requested cutoff offset — including PII for users who explicitly
 *       requested deletion under GDPR Article 17. A compliance incident.</li>
 * </ol>
 *
 * <p>Fix: {@code new DeleteRecordsOptions().timeoutMs(120_000)} AND inspect
 * each per-partition future via {@link DeleteRecordsResult#lowWatermarks()}
 * rather than the aggregated {@code all()} future.
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code (Ljava/util/Map;)Lorg/apache/kafka/clients/admin/DeleteRecordsResult;}
 *   <li>With options: {@code (Ljava/util/Map;Lorg/apache/kafka/clients/admin/DeleteRecordsOptions;)Lorg/apache/kafka/clients/admin/DeleteRecordsResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/DeleteRecordsOptions;}
 * in the descriptor.
 */
public final class BadAdminDeleteRecordsNoOptions {

    private static Map<TopicPartition, RecordsToDelete> cutoffs() {
        return Map.of(
                new TopicPartition("user.events", 0), RecordsToDelete.beforeOffset(1_500_000L),
                new TopicPartition("user.events", 1), RecordsToDelete.beforeOffset(1_480_000L),
                new TopicPartition("user.events", 2), RecordsToDelete.beforeOffset(1_520_000L));
    }

    /** Anti-pattern: no DeleteRecordsOptions on Admin (INVOKEINTERFACE) — FIRES. */
    public DeleteRecordsResult adminNoOptions(Admin admin) {
        return admin.deleteRecords(cutoffs()); // FIRES — no options
    }

    /** Anti-pattern: no DeleteRecordsOptions on AdminClient (INVOKEVIRTUAL) — FIRES. */
    public DeleteRecordsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.deleteRecords(cutoffs()); // FIRES — no options
        }
    }

    /** Control: DeleteRecordsOptions with generous timeout — must NOT fire. */
    public DeleteRecordsResult adminWithOptions(Admin admin) {
        DeleteRecordsOptions opts = new DeleteRecordsOptions().timeoutMs(120_000);
        return admin.deleteRecords(cutoffs(), opts);
    }
}
