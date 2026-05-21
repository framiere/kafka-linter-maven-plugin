package sample;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTransactionsOptions;
import org.apache.kafka.clients.admin.DescribeTransactionsResult;

/**
 * RULE: ADMIN_DESCRIBE_TRANSACTIONS_NO_OPTIONS.
 *
 * Fires when {@code Admin.describeTransactions(Collection<String>)} is called
 * without a {@code DescribeTransactionsOptions} parameter. The bytecode descriptor
 * of the no-options overload does NOT contain
 * {@code Lorg/apache/kafka/clients/admin/DescribeTransactionsOptions;}.
 */
public final class BadAdminDescribeTransactionsNoOptions {

    private static List<String> txIds() {
        return List.of("payments-tx-0", "payments-tx-1", "orders-tx-0");
    }

    /** Anti-pattern: no options on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public DescribeTransactionsResult adminNoOptions(Admin admin) {
        return admin.describeTransactions(txIds()); // FIRES — no DescribeTransactionsOptions
    }

    /** Anti-pattern: no options on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public DescribeTransactionsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.describeTransactions(txIds()); // FIRES — no DescribeTransactionsOptions
        }
    }

    /** Control: explicit DescribeTransactionsOptions with long timeout — must NOT fire. */
    public DescribeTransactionsResult adminWithOptions(Admin admin) {
        DescribeTransactionsOptions opts = new DescribeTransactionsOptions().timeoutMs(120_000);
        return admin.describeTransactions(txIds(), opts);
    }
}
