package sample;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;

/**
 * Control for ADMIN_CLOSE_NO_TIMEOUT.
 *
 * <p>Three methods, each closes an Admin / AdminClient using the
 * BOUNDED overload {@code close(Duration)}. The rule explicitly
 * matches on descriptor {@code "()V"} only, so the bounded overload
 * descriptor {@code "(Ljava/time/Duration;)V"} is skipped — these
 * methods are silent.
 *
 * <p>Why {@code close(Duration)} is the right shape:
 * <ol>
 *   <li>It uses the SAME state machine as the no-arg overload —
 *       transition to CLOSING, wait for in-flight futures to drain
 *       — but with a HARD upper bound. If the deadline elapses with
 *       futures still pending, those futures are failed with a
 *       {@code TimeoutException} and close() returns.</li>
 *   <li>30 seconds is usually enough for normal operation against a
 *       healthy cluster — most admin futures complete in milliseconds.
 *       It is short enough to fit comfortably inside a
 *       {@code terminationGracePeriodSeconds: 60} default in Kubernetes
 *       deployments, and inside a typical Helm hook timeout.</li>
 *   <li>For long-running operators (controllers that ran for hours
 *       and accumulated many in-flight admin operations), match the
 *       deadline to the orchestration framework's grace period —
 *       same 30-60 s ballpark in practice. The exact number is less
 *       important than the EXISTENCE of an upper bound.</li>
 * </ol>
 *
 * <p>Trade-off the rule deliberately accepts: it does not check the
 * VALUE of the Duration. A caller that passes {@code Duration.ofDays(7)}
 * to close() satisfies the rule and is silent — but functionally
 * indistinguishable from the no-arg overload from the calling
 * thread's perspective. The rule's contract is "there is SOME
 * upper bound expressed in code"; validating that the bound is
 * reasonable is out of scope (would require constant-folding or
 * data-flow on the Duration argument).
 */
public final class GoodAdminCloseWithDuration {

    /** Bounded close on a locally-created Admin. 30 s fits inside k8s default grace period. */
    public void closeLocalAdmin() {
        Properties props = baseAdminProps();
        Admin admin = Admin.create(props);
        admin.listTopics();
        admin.close(Duration.ofSeconds(30)); // silent: close(Duration) descriptor
    }

    /** Bounded close on an injected Admin. */
    public void closeInjectedAdmin(Admin admin) {
        admin.describeCluster();
        admin.close(Duration.ofSeconds(30)); // silent
    }

    /** Bounded close on the legacy AdminClient class. Same overload accepted. */
    public void closeLegacyAdminClient() {
        Properties props = baseAdminProps();
        AdminClient admin = AdminClient.create(props);
        admin.listTopics();
        admin.close(Duration.ofSeconds(30)); // silent
    }

    private static Properties baseAdminProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("client.id", "good-admin-close-with-duration");
        return p;
    }
}
