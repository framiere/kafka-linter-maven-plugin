package sample;

import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListClientMetricsResourcesOptions;
import org.apache.kafka.clients.admin.ListClientMetricsResourcesResult;

/**
 * RULE: ADMIN_LIST_CLIENT_METRICS_RESOURCES_NO_OPTIONS.
 *
 * Fires when {@code Admin.listClientMetricsResources()} is called without a
 * {@code ListClientMetricsResourcesOptions} parameter. The bytecode descriptor
 * of the no-options overload does NOT contain
 * {@code Lorg/apache/kafka/clients/admin/ListClientMetricsResourcesOptions;}.
 */
public final class BadAdminListClientMetricsResourcesNoOptions {

    /** Anti-pattern: no options on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public ListClientMetricsResourcesResult adminNoOptions(Admin admin) {
        return admin.listClientMetricsResources(); // FIRES — no ListClientMetricsResourcesOptions
    }

    /** Anti-pattern: no options on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public ListClientMetricsResourcesResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.listClientMetricsResources(); // FIRES — no ListClientMetricsResourcesOptions
        }
    }

    /** Control: explicit ListClientMetricsResourcesOptions with long timeout — must NOT fire. */
    public ListClientMetricsResourcesResult adminWithOptions(Admin admin) {
        ListClientMetricsResourcesOptions opts = new ListClientMetricsResourcesOptions().timeoutMs(120_000);
        return admin.listClientMetricsResources(opts);
    }
}
