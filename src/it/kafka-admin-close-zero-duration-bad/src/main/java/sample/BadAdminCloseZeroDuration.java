package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * RULE: ADMIN_CLOSE_ZERO_DURATION.
 *
 * Fires when {@code admin.close(Duration.ZERO)} or
 * {@code admin.close(Duration.ofXxx(0))} is called. Every in-flight admin
 * request is abandoned: pending KafkaFutures complete with
 * {@code TimeoutException}, but the underlying broker-side operation may
 * have already begun (or already completed). The caller cannot tell
 * whether the cluster-side change actually happened — IaC tools report
 * a clean exit while the cluster diverges from intent.
 */
public final class BadAdminCloseZeroDuration {

    private Admin newAdmin(Properties p) {
        return Admin.create(p);
    }

    private AdminClient newAdminClient(Properties p) {
        return (AdminClient) Admin.create(p);
    }

    /** Anti-pattern: close(Duration.ZERO) — abandons every pending future. */
    public void closeZeroField(Properties p) {
        Admin a = newAdmin(p);
        a.createTopics(List.of(new NewTopic("orders", 6, (short) 3)));
        a.close(Duration.ZERO); // FIRES — Duration.ZERO field
    }

    /** Anti-pattern: close(Duration.ofMillis(0)) — same hazard, factory-method form. */
    public void closeOfMillisZero(Properties p) {
        Admin a = newAdmin(p);
        a.deleteTopics(List.of("scratch"));
        a.close(Duration.ofMillis(0)); // FIRES
    }

    /** Anti-pattern: close(Duration.ofSeconds(0)) on the concrete AdminClient. */
    public void closeOfSecondsZeroOnAdminClient(Properties p) {
        AdminClient a = newAdminClient(p);
        a.describeCluster();
        a.close(Duration.ofSeconds(0)); // FIRES
    }

    /** Anti-pattern: close(Duration.ofNanos(0L)). */
    public void closeOfNanosZero(Properties p) {
        Admin a = newAdmin(p);
        a.listTopics();
        a.close(Duration.ofNanos(0L)); // FIRES
    }

    /** Control: close(Duration.ofSeconds(30)) — the conventional shape. Must NOT fire. */
    public void closeBounded(Properties p) {
        Admin a = newAdmin(p);
        a.createTopics(List.of(new NewTopic("orders", 6, (short) 3)));
        a.close(Duration.ofSeconds(30)); // OK
    }

    /** Control: close(Duration.ofMillis(500)) — finite, well-bounded. Must NOT fire. */
    public void closeShortButNonZero(Properties p) {
        Admin a = newAdmin(p);
        a.listTopics();
        a.close(Duration.ofMillis(500)); // OK
    }
}
