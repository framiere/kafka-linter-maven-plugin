package sample;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsOptions;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;

/**
 * RULE: ADMIN_DESCRIBE_USER_SCRAM_CREDENTIALS_NO_OPTIONS.
 *
 * Fires when {@code Admin.describeUserScramCredentials()} (0-arg) or
 * {@code Admin.describeUserScramCredentials(List<String>)} is called without a
 * {@code DescribeUserScramCredentialsOptions} parameter. The bytecode descriptor
 * of both no-options overloads does NOT contain
 * {@code Lorg/apache/kafka/clients/admin/DescribeUserScramCredentialsOptions;}.
 */
public final class BadAdminDescribeUserScramCredentialsNoOptions {

    private static List<String> users() {
        return List.of("svc-payments", "svc-orders");
    }

    /** Anti-pattern: 0-arg form on Admin (full-table scan) — FIRES. */
    public DescribeUserScramCredentialsResult adminNoArgsNoOptions(Admin admin) {
        return admin.describeUserScramCredentials(); // FIRES — no DescribeUserScramCredentialsOptions
    }

    /** Anti-pattern: 1-arg list-users form on Admin (no options) — FIRES. */
    public DescribeUserScramCredentialsResult adminUsersNoOptions(Admin admin) {
        return admin.describeUserScramCredentials(users()); // FIRES — no DescribeUserScramCredentialsOptions
    }

    /** Anti-pattern: 0-arg form on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public DescribeUserScramCredentialsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.describeUserScramCredentials(); // FIRES — no DescribeUserScramCredentialsOptions
        }
    }

    /** Control: explicit DescribeUserScramCredentialsOptions with long timeout — must NOT fire. */
    public DescribeUserScramCredentialsResult adminWithOptions(Admin admin) {
        DescribeUserScramCredentialsOptions opts = new DescribeUserScramCredentialsOptions().timeoutMs(120_000);
        return admin.describeUserScramCredentials(users(), opts);
    }
}
