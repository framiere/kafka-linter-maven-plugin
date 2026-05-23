package sample;

import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;

/**
 * RULE: ADMIN_CLOSE_NO_TIMEOUT.
 *
 * <p>Fires on every {@code close()} call (descriptor {@code ()V}) made
 * on the {@code Admin} interface or the {@code AdminClient} abstract
 * class. The bounded overload {@code close(Duration)} (descriptor
 * {@code (Ljava/time/Duration;)V}) is intentionally NOT flagged — it
 * is the recommended pattern.
 *
 * <p>Why the no-arg overload is dangerous — what it actually does:
 * <ol>
 *   <li>{@code Admin.close()} (no arg) is a shorthand for
 *       {@code close(Duration.ofMillis(Long.MAX_VALUE))}. The
 *       AdminClient transitions to the CLOSING state (rejecting
 *       new operations) and then BLOCKS the calling thread until
 *       every queued and in-flight {@code KafkaFuture} has either
 *       completed or failed.</li>
 *   <li>Each admin operation (listTopics, createTopics, describeCluster,
 *       alterConfigs, …) carries its own {@code request.timeout.ms}
 *       (default 30 s) and retry policy. When the controller is in
 *       the middle of an election, when a broker is unreachable,
 *       when the cluster is rolling — those operations may NEVER
 *       resolve. They just keep retrying until success, which can
 *       be never.</li>
 *   <li>Because {@code close()} waits for EVERY in-flight call,
 *       a single hung future is enough to hang the entire close().
 *       The CALLING THREAD (typically main(), or the orchestration
 *       framework's shutdown hook) is now stuck forever waiting on
 *       a network resource it cannot influence.</li>
 *   <li>The blast radius is exactly the kind of code where waiting
 *       forever is most painful: operational tooling. Lag monitors,
 *       topic provisioners, IaC reconcilers (Terraform Kafka
 *       provider), Helm pre-delete hooks, "reset offsets" scripts
 *       — all of them follow the same pattern: open AdminClient,
 *       perform N operations in a loop, close() at the end. These
 *       tools need to FINISH PROMPTLY during a cluster incident
 *       (so the orchestrator can move on, retry, page someone) —
 *       exactly the scenario where the no-arg close() will hang.</li>
 *   <li>Kubernetes operators see this as Pods stuck in Terminating,
 *       Helm sees it as a hung pre-delete hook (eventually killed
 *       by the orchestrator's grace period), and Terraform sees it
 *       as a "no progress" timeout from its own state machine.
 *       The downstream incident is almost always wrongly diagnosed
 *       as "Kafka is slow" when really it is "our admin tooling
 *       has no deadline on close()".</li>
 * </ol>
 *
 * <p>What the rule catches (per-METHOD scan, no slot tracking required):
 * <ol>
 *   <li>For every {@code MethodInsnNode} whose owner is the
 *       {@code Admin} interface OR the {@code AdminClient} class.</li>
 *   <li>Method name must be exactly {@code "close"}.</li>
 *   <li>Descriptor must be exactly {@code "()V"} — the no-arg
 *       overload. {@code close(Ljava/time/Duration;)V} is skipped
 *       on purpose.</li>
 *   <li>Fire site is the close() call instruction.</li>
 * </ol>
 *
 * <p>Note that the rule does NOT require the AdminClient to be
 * locally constructed — calling close() on an injected admin
 * reference (parameter, field) is just as problematic, so the rule
 * deliberately ignores receiver provenance and only looks at the
 * call site.
 *
 * <p>This Bad class triggers THREE fires — three different shapes of
 * the same anti-pattern:
 * <ol>
 *   <li>{@code closeLocalAdmin}: close() on a locally-created Admin.</li>
 *   <li>{@code closeInjectedAdmin}: close() on a parameter-passed
 *       Admin. The rule does not require local provenance.</li>
 *   <li>{@code closeLegacyAdminClient}: close() on the legacy
 *       AdminClient abstract class. Same rule, both owners are
 *       tracked.</li>
 * </ol>
 */
public final class BadAdminCloseNoTimeout {

    /** Anti-pattern: locally-created Admin, no-arg close at end. Hangs the JVM during cluster incidents. */
    public void closeLocalAdmin() {
        Properties props = baseAdminProps();
        Admin admin = Admin.create(props);
        admin.listTopics();
        admin.close(); // reported — close(()V) on Admin interface
    }

    /** Anti-pattern: caller injects the admin; we still close() with no deadline. */
    public void closeInjectedAdmin(Admin admin) {
        admin.describeCluster();
        admin.close(); // reported — close(()V) on Admin interface
    }

    /** Anti-pattern: legacy AdminClient abstract class. Same rule, both owners tracked. */
    public void closeLegacyAdminClient() {
        Properties props = baseAdminProps();
        AdminClient admin = AdminClient.create(props);
        admin.listTopics();
        admin.close(); // reported — close(()V) on AdminClient class
    }

    private static Properties baseAdminProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("client.id", "bad-admin-close-no-timeout");
        return p;
    }
}
