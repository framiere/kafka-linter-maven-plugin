package sample;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateDelegationTokenOptions;
import org.apache.kafka.clients.admin.CreateDelegationTokenResult;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * RULE: ADMIN_CREATE_DELEGATION_TOKEN_NO_OPTIONS.
 *
 * Fires when {@code Admin.createDelegationToken()} is called without a
 * {@code CreateDelegationTokenOptions} parameter. The bytecode descriptor of
 * the no-options overload does NOT contain
 * {@code Lorg/apache/kafka/clients/admin/CreateDelegationTokenOptions;}.
 */
public final class BadAdminCreateDelegationTokenNoOptions {

    /** Anti-pattern: no options on Admin (interface dispatch INVOKEINTERFACE) — FIRES. */
    public CreateDelegationTokenResult adminNoOptions(Admin admin) {
        return admin.createDelegationToken(); // FIRES — no CreateDelegationTokenOptions
    }

    /** Anti-pattern: no options on AdminClient (concrete class INVOKEVIRTUAL) — FIRES. */
    public CreateDelegationTokenResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.createDelegationToken(); // FIRES — no CreateDelegationTokenOptions
        }
    }

    /** Control: explicit CreateDelegationTokenOptions with owner/renewers/maxLifetime/timeout — must NOT fire. */
    public CreateDelegationTokenResult adminWithOptions(Admin admin) {
        CreateDelegationTokenOptions opts = new CreateDelegationTokenOptions()
                .owner(new KafkaPrincipal("User", "svc-foo"))
                .renewers(List.of(new KafkaPrincipal("User", "renewer-bot")))
                .maxlifeTimeMs(14L * 24 * 3600 * 1000)
                .timeoutMs(120_000);
        return admin.createDelegationToken(opts);
    }
}
