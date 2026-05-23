package sample;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.NewPartitions;

/**
 * RULE: ADMIN_CREATE_PARTITIONS_NO_OPTIONS.
 *
 * Fires when {@code Admin.createPartitions(Map<String, NewPartitions>)} is
 * called without a {@code CreatePartitionsOptions} argument. The no-options
 * form has TWO silent defaults that compound: (1) {@code validateOnly}
 * defaults to {@code false} — the change is executed immediately, with no
 * way to ask "would this succeed?" first; (2) request timeout inherits the
 * AdminClient default (~30 s).
 *
 * <p>Why this is the most-dangerous AdminClient call: adding partitions to
 * an existing topic does NOT migrate existing data. The default partitioner
 * is {@code hash(key) % partitionCount} — increasing {@code partitionCount}
 * maps the SAME key to a DIFFERENT partition than before. Every keyed
 * consumer that relied on per-key ordering breaks instantly: records for
 * key 'K' that used to land on partition 3 now land on partition 7, the
 * Streams aggregation state for 'K' is on the task processing the OLD
 * partition 3, and the NEW partition 7 has no state. The change is
 * IRREVERSIBLE — Kafka has no {@code decreasePartitions}.
 *
 * <p>Failure mode (silent under-reporting in production):
 * <ol>
 *   <li>{@code payments} topic has 12 partitions; a Streams app aggregates
 *       {@code payment_total per user} keyed by user_id.</li>
 *   <li>A new SRE runs
 *       {@code admin.createPartitions(Map.of("payments",
 *       NewPartitions.increaseTo(24)))} — no options, no dry-run because
 *       the SRE wasn't aware {@code validateOnly} exists.</li>
 *   <li>Topic immediately has 24 partitions; the next produce sends a
 *       record for {@code user_id=alice} (was partition 5 of 12, now
 *       partition 17 of 24); the Streams state for alice is on the OLD
 *       partition 5; the NEW partition 17 has no state.</li>
 *   <li>Alice's {@code payment_total} for the rest of the deploy is
 *       computed from zero. The bug is invisible until reconciliation.</li>
 * </ol>
 *
 * <p>Fix protocol: ALWAYS dry-run first —
 * {@code admin.createPartitions(specs, new CreatePartitionsOptions()
 * .validateOnly(true).timeoutMs(60_000))}. If every entry succeeds, only
 * THEN re-run with {@code validateOnly(false).timeoutMs(120_000)} to apply
 * the change. AND coordinate the cutover with a key-rebalance plan, not
 * just a partition-count increase.
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code (Ljava/util/Map;)Lorg/apache/kafka/clients/admin/CreatePartitionsResult;}
 *   <li>With options: {@code (Ljava/util/Map;Lorg/apache/kafka/clients/admin/CreatePartitionsOptions;)Lorg/apache/kafka/clients/admin/CreatePartitionsResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/CreatePartitionsOptions;}
 * in the descriptor.
 */
public final class BadAdminCreatePartitionsNoOptions {

    private static Map<String, NewPartitions> plan() {
        return Map.of("payments.events", NewPartitions.increaseTo(24));
    }

    /** Anti-pattern: no CreatePartitionsOptions on Admin (INVOKEINTERFACE) — FIRES. */
    public CreatePartitionsResult adminNoOptions(Admin admin) {
        return admin.createPartitions(plan()); // FIRES — no options, no dry-run
    }

    /** Anti-pattern: no CreatePartitionsOptions on AdminClient (INVOKEVIRTUAL) — FIRES. */
    public CreatePartitionsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.createPartitions(plan()); // FIRES — no options, no dry-run
        }
    }

    /** Control: CreatePartitionsOptions dry-run with generous timeout — must NOT fire. */
    public CreatePartitionsResult adminDryRunWithOptions(Admin admin) {
        CreatePartitionsOptions opts = new CreatePartitionsOptions()
                .validateOnly(true)
                .timeoutMs(60_000);
        return admin.createPartitions(plan(), opts);
    }
}
