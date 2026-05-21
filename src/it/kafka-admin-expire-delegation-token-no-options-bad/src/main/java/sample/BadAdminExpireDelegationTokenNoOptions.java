package sample;

import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ExpireDelegationTokenOptions;
import org.apache.kafka.clients.admin.ExpireDelegationTokenResult;

/**
 * RULE: ADMIN_EXPIRE_DELEGATION_TOKEN_NO_OPTIONS.
 *
 * Fires when {@code Admin.expireDelegationToken(byte[])} is called without an
 * {@code ExpireDelegationTokenOptions} parameter. The bytecode descriptor of the
 * no-options overload does NOT contain
 * {@code Lorg/apache/kafka/clients/admin/ExpireDelegationTokenOptions;}.
 */
public final class BadAdminExpireDelegationTokenNoOptions {

    private static byte[] hmac() {
        return new byte[]{0x01, 0x02, 0x03, 0x04};
    }

    /** Anti-pattern: no options on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public ExpireDelegationTokenResult adminNoOptions(Admin admin) {
        return admin.expireDelegationToken(hmac()); // FIRES — no ExpireDelegationTokenOptions
    }

    /** Anti-pattern: no options on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public ExpireDelegationTokenResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.expireDelegationToken(hmac()); // FIRES — no ExpireDelegationTokenOptions
        }
    }

    /** Control: explicit ExpireDelegationTokenOptions with expiry=0 and long timeout — must NOT fire. */
    public ExpireDelegationTokenResult adminWithOptions(Admin admin) {
        ExpireDelegationTokenOptions opts = new ExpireDelegationTokenOptions()
                .expiryTimePeriodMs(0)
                .timeoutMs(120_000);
        return admin.expireDelegationToken(hmac(), opts);
    }
}
