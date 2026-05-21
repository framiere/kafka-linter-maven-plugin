package sample;

import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.RenewDelegationTokenOptions;
import org.apache.kafka.clients.admin.RenewDelegationTokenResult;

/**
 * RULE: ADMIN_RENEW_DELEGATION_TOKEN_NO_OPTIONS.
 *
 * Fires when {@code Admin.renewDelegationToken(byte[])} is called without a
 * {@code RenewDelegationTokenOptions} parameter. The bytecode descriptor of the
 * no-options overload does NOT contain
 * {@code Lorg/apache/kafka/clients/admin/RenewDelegationTokenOptions;}.
 */
public final class BadAdminRenewDelegationTokenNoOptions {

    private static byte[] hmac() {
        return new byte[]{0x01, 0x02, 0x03, 0x04};
    }

    /** Anti-pattern: no options on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public RenewDelegationTokenResult adminNoOptions(Admin admin) {
        return admin.renewDelegationToken(hmac()); // FIRES — no RenewDelegationTokenOptions
    }

    /** Anti-pattern: no options on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public RenewDelegationTokenResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.renewDelegationToken(hmac()); // FIRES — no RenewDelegationTokenOptions
        }
    }

    /** Control: explicit RenewDelegationTokenOptions with explicit renewal period and long timeout — must NOT fire. */
    public RenewDelegationTokenResult adminWithOptions(Admin admin) {
        RenewDelegationTokenOptions opts = new RenewDelegationTokenOptions()
                .renewTimePeriodMs(7L * 24 * 3600 * 1000)
                .timeoutMs(120_000);
        return admin.renewDelegationToken(hmac(), opts);
    }
}
