package sample;

import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AbortTransactionOptions;
import org.apache.kafka.clients.admin.AbortTransactionResult;
import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;

/**
 * RULE: ADMIN_ABORT_TRANSACTION_NO_OPTIONS.
 *
 * Fires when {@code Admin.abortTransaction(AbortTransactionSpec)} is called without an
 * {@code AbortTransactionOptions} parameter. The bytecode descriptor of the no-options
 * overload does NOT contain {@code Lorg/apache/kafka/clients/admin/AbortTransactionOptions;}.
 */
public final class BadAdminAbortTransactionNoOptions {

    private static AbortTransactionSpec spec() {
        // (topicPartition, producerId, producerEpoch, coordinatorEpoch)
        return new AbortTransactionSpec(new TopicPartition("orders", 0), 1234L, (short) 0, 0);
    }

    /** Anti-pattern: no AbortTransactionOptions on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public AbortTransactionResult adminNoOptions(Admin admin) {
        return admin.abortTransaction(spec()); // FIRES — no AbortTransactionOptions
    }

    /** Anti-pattern: no AbortTransactionOptions on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public AbortTransactionResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.abortTransaction(spec()); // FIRES — no AbortTransactionOptions
        }
    }

    /** Control: AbortTransactionOptions passed with explicit long timeout — must NOT fire. */
    public AbortTransactionResult adminWithOptions(Admin admin) {
        AbortTransactionOptions opts = new AbortTransactionOptions().timeoutMs(120_000);
        return admin.abortTransaction(spec(), opts);
    }
}
