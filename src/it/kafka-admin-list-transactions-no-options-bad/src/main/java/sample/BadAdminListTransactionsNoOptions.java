package sample;

import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.ListTransactionsResult;
import org.apache.kafka.clients.admin.TransactionState;

/**
 * RULE: ADMIN_LIST_TRANSACTIONS_NO_OPTIONS.
 *
 * Fires when {@code Admin.listTransactions()} is called without a
 * {@code ListTransactionsOptions} argument. The no-options form returns
 * EVERY transactional-ID ever recorded by the transaction coordinator
 * ({@code __transaction_state} topic), in every {@code TransactionState}:
 *
 * <ul>
 *   <li>{@code Ongoing} — in-flight transaction (may abort or commit)</li>
 *   <li>{@code PrepareCommit} / {@code PrepareAbort} — 2PC mid-flight</li>
 *   <li>{@code CompleteCommit} / {@code CompleteAbort} — terminal, retained
 *       for {@code transactional.id.expiration.ms} (default 7 days)</li>
 *   <li>{@code Empty} — metadata entry with no transaction</li>
 *   <li>{@code Dead} — marked for cleanup</li>
 * </ul>
 *
 * <p>On a Streams cluster with EOS-v2 enabled, EVERY Streams task creates
 * its own transactional-ID ({@code <application-id>-<taskId>}); a topology
 * with 200 tasks across 5 apps creates 1000 transactional-IDs, and
 * historical task assignments leave THOUSANDS more in
 * {@code CompleteCommit}/{@code CompleteAbort} retained for 7 days.
 *
 * <p>Failure mode (transaction monitoring dashboard floods with terminal
 * records):
 * <ol>
 *   <li>A Streams ops team builds a dashboard tracking 'how many
 *       transactions are mid-flight' as an EOS-v2 health proxy.</li>
 *   <li>The dashboard calls {@code admin.listTransactions().all().get()}
 *       every minute and counts entries.</li>
 *   <li>Day 1 — fresh cluster, 200 entries all Ongoing or Empty,
 *       refresh in 100 ms.</li>
 *   <li>Day 30 — 250K accumulated entries (CompleteCommit/CompleteAbort
 *       retained 7 days × daily task reassignments), refresh takes 8 s,
 *       dashboard times out, team scrambles to figure out what 'broke'.</li>
 *   <li>Nothing actually broke. With
 *       {@code filterStates(Set.of(TransactionState.ONGOING))} the
 *       response size drops 1000× and the dashboard works again.</li>
 * </ol>
 *
 * <p>Fix: {@code new ListTransactionsOptions()
 * .filterStates(Set.of(TransactionState.ONGOING)).timeoutMs(60_000)}
 * for monitoring use cases; add {@code .filterProducerIds(...)} for
 * targeted "is producer P stuck mid-transaction?" debugging.
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code ()Lorg/apache/kafka/clients/admin/ListTransactionsResult;}
 *   <li>With options: {@code (Lorg/apache/kafka/clients/admin/ListTransactionsOptions;)Lorg/apache/kafka/clients/admin/ListTransactionsResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/ListTransactionsOptions;}
 * in the descriptor.
 */
public final class BadAdminListTransactionsNoOptions {

    /** Anti-pattern: no ListTransactionsOptions on Admin (INVOKEINTERFACE) — FIRES. */
    public ListTransactionsResult adminNoOptions(Admin admin) {
        return admin.listTransactions(); // FIRES — no options, returns ALL states
    }

    /** Anti-pattern: no ListTransactionsOptions on AdminClient (INVOKEVIRTUAL) — FIRES. */
    public ListTransactionsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.listTransactions(); // FIRES — no options
        }
    }

    /** Control: ListTransactionsOptions with ONGOING state filter — must NOT fire. */
    public ListTransactionsResult adminWithOptions(Admin admin) {
        ListTransactionsOptions opts = new ListTransactionsOptions()
                .filterStates(Set.of(TransactionState.ONGOING))
                .timeoutMs(60_000);
        return admin.listTransactions(opts);
    }
}
