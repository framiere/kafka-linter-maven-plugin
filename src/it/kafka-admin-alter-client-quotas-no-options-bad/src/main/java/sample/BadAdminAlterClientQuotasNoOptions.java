package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterClientQuotasOptions;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * RULE: ADMIN_ALTER_CLIENT_QUOTAS_NO_OPTIONS.
 *
 * Fires on any call to {@code Admin.alterClientQuotas(Collection<ClientQuotaAlteration>)}
 * whose descriptor lacks {@code AlterClientQuotasOptions}. The no-options form
 * defaults {@code validateOnly=false} (the destructive quota mutation is APPLIED,
 * not previewed) and inherits the AdminClient default {@code request.timeout.ms}
 * (~30 s). On partial-failure mid-batch, some alterations are persisted and
 * others not, with no caller-visible way to tell which.
 *
 * Mix of receiver types deliberately exercises both INVOKEINTERFACE (against
 * {@code Admin}) and INVOKEVIRTUAL (against {@code AdminClient}).
 */
public final class BadAdminAlterClientQuotasNoOptions {

    private final Admin admin;

    public BadAdminAlterClientQuotasNoOptions(Admin admin) {
        this.admin = admin;
    }

    /** Anti-pattern: no-options call on the Admin interface — INVOKEINTERFACE alterClientQuotas(Collection). */
    public void applyTenantQuotas(Collection<ClientQuotaAlteration> alterations) {
        this.admin.alterClientQuotas(alterations); // FIRES — no AlterClientQuotasOptions
    }

    /** Anti-pattern: parameter typed as the abstract AdminClient — INVOKEVIRTUAL alterClientQuotas(Collection). */
    public static void applyTenantQuotasConcrete(AdminClient a, Collection<ClientQuotaAlteration> alterations) {
        a.alterClientQuotas(alterations); // FIRES — no AlterClientQuotasOptions
    }

    /** Anti-pattern: inline-built batch with literal entries — still no options. */
    public void resetTwoTenants() {
        ClientQuotaEntity userAlice = new ClientQuotaEntity(Map.of("user", "alice"));
        ClientQuotaEntity userBob = new ClientQuotaEntity(Map.of("user", "bob"));
        ClientQuotaAlteration aliceOp = new ClientQuotaAlteration(userAlice, List.of(
                new ClientQuotaAlteration.Op("producer_byte_rate", 10_485_760.0d),
                new ClientQuotaAlteration.Op("consumer_byte_rate", 10_485_760.0d)));
        ClientQuotaAlteration bobOp = new ClientQuotaAlteration(userBob, List.of(
                new ClientQuotaAlteration.Op("producer_byte_rate", 5_242_880.0d)));
        this.admin.alterClientQuotas(List.of(aliceOp, bobOp)); // FIRES — no AlterClientQuotasOptions
    }

    /** Control: explicit AlterClientQuotasOptions with dry-run — must NOT fire. */
    public void previewTenantQuotas(Collection<ClientQuotaAlteration> alterations) {
        this.admin.alterClientQuotas(alterations,
                new AlterClientQuotasOptions().validateOnly(true).timeoutMs(60_000));
    }

    /** Control: explicit AlterClientQuotasOptions with apply — must NOT fire. */
    public void applyTenantQuotasWithOptions(Collection<ClientQuotaAlteration> alterations) {
        this.admin.alterClientQuotas(alterations,
                new AlterClientQuotasOptions().validateOnly(false).timeoutMs(60_000));
    }
}
