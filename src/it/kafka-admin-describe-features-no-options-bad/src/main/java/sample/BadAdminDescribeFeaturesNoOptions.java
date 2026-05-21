package sample;

import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeFeaturesOptions;
import org.apache.kafka.clients.admin.DescribeFeaturesResult;

/**
 * RULE: ADMIN_DESCRIBE_FEATURES_NO_OPTIONS.
 *
 * Fires when {@code Admin.describeFeatures()} is called without a
 * {@code DescribeFeaturesOptions} parameter. The bytecode descriptor of
 * the no-options overload does NOT contain
 * {@code Lorg/apache/kafka/clients/admin/DescribeFeaturesOptions;}.
 */
public final class BadAdminDescribeFeaturesNoOptions {

    /** Anti-pattern: no options on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public DescribeFeaturesResult adminNoOptions(Admin admin) {
        return admin.describeFeatures(); // FIRES — no DescribeFeaturesOptions
    }

    /** Anti-pattern: no options on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public DescribeFeaturesResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.describeFeatures(); // FIRES — no DescribeFeaturesOptions
        }
    }

    /** Control: explicit DescribeFeaturesOptions with long timeout — must NOT fire. */
    public DescribeFeaturesResult adminWithOptions(Admin admin) {
        DescribeFeaturesOptions opts = new DescribeFeaturesOptions().timeoutMs(120_000);
        return admin.describeFeatures(opts);
    }
}
