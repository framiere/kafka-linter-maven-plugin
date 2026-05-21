package sample;

import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.UnregisterBrokerOptions;
import org.apache.kafka.clients.admin.UnregisterBrokerResult;

/**
 * RULE: ADMIN_UNREGISTER_BROKER_NO_OPTIONS.
 *
 * Fires when {@code Admin.unregisterBroker(int)} is called without an
 * {@code UnregisterBrokerOptions} parameter. The bytecode descriptor of the
 * no-options overload does NOT contain
 * {@code Lorg/apache/kafka/clients/admin/UnregisterBrokerOptions;}.
 */
public final class BadAdminUnregisterBrokerNoOptions {

    /** Anti-pattern: no options on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public UnregisterBrokerResult adminNoOptions(Admin admin) {
        return admin.unregisterBroker(7); // FIRES — no UnregisterBrokerOptions
    }

    /** Anti-pattern: no options on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public UnregisterBrokerResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.unregisterBroker(7); // FIRES — no UnregisterBrokerOptions
        }
    }

    /** Control: explicit UnregisterBrokerOptions with long timeout — must NOT fire. */
    public UnregisterBrokerResult adminWithOptions(Admin admin) {
        UnregisterBrokerOptions opts = new UnregisterBrokerOptions().timeoutMs(120_000);
        return admin.unregisterBroker(7, opts);
    }
}
