package sample;

import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClientQuotasOptions;
import org.apache.kafka.clients.admin.DescribeClientQuotasResult;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;

/**
 * RULE: ADMIN_DESCRIBE_CLIENT_QUOTAS_NO_OPTIONS.
 *
 * Fires when {@code Admin.describeClientQuotas(ClientQuotaFilter)} is called
 * without a {@code DescribeClientQuotasOptions} parameter. The bytecode descriptor
 * of the no-options overload does NOT contain
 * {@code Lorg/apache/kafka/clients/admin/DescribeClientQuotasOptions;}.
 */
public final class BadAdminDescribeClientQuotasNoOptions {

    private static ClientQuotaFilter filter() {
        return ClientQuotaFilter.contains(java.util.List.of(
                ClientQuotaFilterComponent.ofEntity("user", "svc-payments")));
    }

    /** Anti-pattern: no options on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public DescribeClientQuotasResult adminNoOptions(Admin admin) {
        return admin.describeClientQuotas(filter()); // FIRES — no DescribeClientQuotasOptions
    }

    /** Anti-pattern: no options on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public DescribeClientQuotasResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.describeClientQuotas(filter()); // FIRES — no DescribeClientQuotasOptions
        }
    }

    /** Control: explicit DescribeClientQuotasOptions with long timeout — must NOT fire. */
    public DescribeClientQuotasResult adminWithOptions(Admin admin) {
        DescribeClientQuotasOptions opts = new DescribeClientQuotasOptions().timeoutMs(120_000);
        return admin.describeClientQuotas(filter(), opts);
    }
}
