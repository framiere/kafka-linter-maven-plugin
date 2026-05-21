package sample;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeDelegationTokenOptions;
import org.apache.kafka.clients.admin.DescribeDelegationTokenResult;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * RULE: ADMIN_DESCRIBE_DELEGATION_TOKEN_NO_OPTIONS.
 *
 * Fires when {@code Admin.describeDelegationToken()} is called without a
 * {@code DescribeDelegationTokenOptions} parameter. The bytecode descriptor
 * of the no-options overload does NOT contain
 * {@code Lorg/apache/kafka/clients/admin/DescribeDelegationTokenOptions;}.
 */
public final class BadAdminDescribeDelegationTokenNoOptions {

    /** Anti-pattern: no options on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public DescribeDelegationTokenResult adminNoOptions(Admin admin) {
        return admin.describeDelegationToken(); // FIRES — no DescribeDelegationTokenOptions
    }

    /** Anti-pattern: no options on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public DescribeDelegationTokenResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.describeDelegationToken(); // FIRES — no DescribeDelegationTokenOptions
        }
    }

    /** Control: explicit DescribeDelegationTokenOptions with owners filter and long timeout — must NOT fire. */
    public DescribeDelegationTokenResult adminWithOptions(Admin admin) {
        DescribeDelegationTokenOptions opts = new DescribeDelegationTokenOptions()
                .owners(List.of(new KafkaPrincipal("User", "batch-job-svc")))
                .timeoutMs(120_000);
        return admin.describeDelegationToken(opts);
    }
}
