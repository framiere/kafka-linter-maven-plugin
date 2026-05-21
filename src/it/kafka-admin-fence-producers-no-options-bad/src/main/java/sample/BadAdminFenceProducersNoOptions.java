package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.FenceProducersOptions;

import java.util.Collection;
import java.util.List;

/**
 * RULE: ADMIN_FENCE_PRODUCERS_NO_OPTIONS.
 *
 * Fires on any call to {@code Admin.fenceProducers(Collection<String>)}
 * whose descriptor lacks {@code FenceProducersOptions}. The no-options
 * form inherits the AdminClient default {@code request.timeout.ms} (~30 s).
 * On a busy transaction coordinator (the typical state during the EOS-v2
 * incidents when fenceProducers is actually called), a mid-batch
 * {@code TimeoutException} leaves some {@code transactional.id} values
 * fenced and others NOT — a split-brain state that defeats the purpose
 * of the fence.
 *
 * Mix of receiver types deliberately exercises both INVOKEINTERFACE
 * (against {@code Admin}) and INVOKEVIRTUAL (against {@code AdminClient}).
 */
public final class BadAdminFenceProducersNoOptions {

    private final Admin admin;

    public BadAdminFenceProducersNoOptions(Admin admin) {
        this.admin = admin;
    }

    /** Anti-pattern: no-options call on the Admin interface — INVOKEINTERFACE fenceProducers(Collection). */
    public void recoverAllTasks(Collection<String> transactionalIds) {
        this.admin.fenceProducers(transactionalIds); // FIRES — no FenceProducersOptions
    }

    /** Anti-pattern: parameter typed as the abstract AdminClient — INVOKEVIRTUAL fenceProducers(Collection). */
    public static void recoverAllTasksConcrete(AdminClient a, Collection<String> transactionalIds) {
        a.fenceProducers(transactionalIds); // FIRES — no FenceProducersOptions
    }

    /** Anti-pattern: inline batch of known tx-ids — still no options. */
    public void fenceKnownTaskIds() {
        this.admin.fenceProducers(List.of("payments-app-task-0", "payments-app-task-1", "payments-app-task-2")); // FIRES — no FenceProducersOptions
    }

    /** Control: explicit FenceProducersOptions with long timeout — must NOT fire. */
    public void recoverAllTasksWithOptions(Collection<String> transactionalIds) {
        this.admin.fenceProducers(transactionalIds,
                new FenceProducersOptions().timeoutMs(120_000));
    }
}
