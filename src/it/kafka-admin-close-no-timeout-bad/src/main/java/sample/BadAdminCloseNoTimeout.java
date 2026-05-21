package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;

import java.time.Duration;

/**
 * RULE: ADMIN_CLOSE_NO_TIMEOUT.
 *
 * Each of the three methods below calls the no-argument
 * {@link Admin#close()} — which internally delegates to
 * {@code close(Duration.ofMillis(Long.MAX_VALUE))}. Outstanding
 * KafkaFutures returned by {@code createTopics}, {@code describeCluster},
 * {@code alterConfigs}, etc. must all complete or fail before close()
 * returns; against an unhealthy cluster they hang forever.
 *
 * The fourth method shows the correct shape: it passes a {@link Duration}
 * to the bounded overload — must NOT fire.
 *
 * Mix of receiver types deliberately exercises both INVOKEINTERFACE
 * (against {@code Admin}) and INVOKEVIRTUAL (against {@code AdminClient}).
 */
public final class BadAdminCloseNoTimeout {

    private final Admin admin;

    public BadAdminCloseNoTimeout(Admin admin) {
        this.admin = admin;
    }

    /** Anti-pattern: field on the Admin interface — INVOKEINTERFACE close(). */
    public void shutdownHook() {
        this.admin.close(); // FIRES
    }

    /** Anti-pattern: parameter typed as the abstract AdminClient — INVOKEVIRTUAL close(). */
    public static void closeConcrete(AdminClient a) {
        a.close(); // FIRES
    }

    /** Anti-pattern: try/finally — the finally branch blocks indefinitely. */
    public static void describeAndClose(Admin a) {
        try {
            a.describeCluster();
        } finally {
            a.close(); // FIRES
        }
    }

    /** Control: bounded overload with a deadline — must NOT fire. */
    public void shutdownWithDeadline() {
        this.admin.close(Duration.ofSeconds(30));
    }
}
