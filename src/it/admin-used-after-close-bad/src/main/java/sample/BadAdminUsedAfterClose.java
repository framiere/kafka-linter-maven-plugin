package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * RULE: ADMIN_USED_AFTER_CLOSE.
 *
 * <p>Fires when a lifecycle method (createTopics / deleteTopics / listTopics /
 * describeTopics / describeCluster / createAcls / alterConfigs /
 * incrementalAlterConfigs / describeConfigs / listConsumerGroups /
 * listConsumerGroupOffsets / alterConsumerGroupOffsets / listOffsets /
 * electLeaders / describeClientQuotas / alterClientQuotas / ...) is invoked on
 * a local-slot {@code AdminClient} AFTER {@code close()} has been called on the
 * same slot earlier in the same method.
 *
 * <p>Why use-after-close on an admin is a serious bug — what actually happens
 * at runtime:
 * <ol>
 *   <li>{@code Admin.close()} (and {@code close(Duration)}) is a ONE-WAY
 *       transition. It tells the {@code AdminClientRunnable} to drain its
 *       in-flight call queue (best-effort, bounded by the timeout), cancels
 *       every still-pending {@code KafkaFuture} with a TimeoutException,
 *       closes the broker sockets, and sets an internal {@code closed}
 *       flag. The closed state is sticky — there is no "reopen()".</li>
 *   <li>Every subsequent admin-facing call checks that flag first and
 *       throws {@code IllegalStateException: AdminClient has been closed}.
 *       Unlike the consumer's {@code wakeup()}, there is no admin method
 *       that is safe after close.</li>
 *   <li>The synchronous IllegalStateException is acceptable when it
 *       propagates to a caller that catches and surfaces it. In practice
 *       the admin is often used via an async path: a try-with-resources
 *       block that submitted a {@code KafkaFuture.thenApply(...)}
 *       continuation, an executor that races the close, a deployment
 *       pipeline that calls a finally-block close() while parallel topic
 *       creations are still in flight. In those flows, the exception is
 *       swallowed by the framework (or by the unobserved
 *       {@code KafkaFuture} chain) and the cluster mutation SILENTLY DOES
 *       NOT HAPPEN — the next deployment run sees the topic missing /
 *       ACL missing / config not applied, and re-attempts it, masking
 *       the original error.</li>
 *   <li>The typical real-world shape: a try-with-resources block on the
 *       admin followed by a stray usage that the developer thought was
 *       inside the try block but isn't (a stray brace, a rebase merge
 *       that moved code), or a CompletableFuture chain that captures the
 *       admin in a {@code thenApply} stage that runs after the outer
 *       method has exited the try block.</li>
 * </ol>
 *
 * <p>What the rule catches (per-METHOD scan, local-slot tracking):
 * <ol>
 *   <li>Track every {@code Admin.create(props) + ASTORE N} — record slot
 *       N. Construction is via the static factory (INVOKESTATIC), not a
 *       public constructor.</li>
 *   <li>For every method call whose receiver resolves to a tracked slot:
 *       <ul>
 *         <li>If the method name starts with {@code "close"}, add the
 *             slot to {@code closedSlots}.</li>
 *         <li>Otherwise, if the name is in the lifecycle set AND the
 *             slot is already in {@code closedSlots}, fire.</li>
 *       </ul></li>
 *   <li>Fire site is the lifecycle call (NOT the close call) — the
 *       operator wants to know where the offending use happens.</li>
 * </ol>
 *
 * <p>This Bad class triggers FIVE fires — five lifecycle operations each
 * performed after close() on the same slot.
 */
public final class BadAdminUsedAfterClose {

    /** Anti-pattern: close() then createTopics(). The topic is never created and the future never completes. */
    public void closeThenCreateTopics() {
        Properties props = baseAdminProps();
        Admin admin = Admin.create(props);
        admin.close();
        admin.createTopics(Collections.singletonList(new NewTopic("orders", 3, (short) 2))); // reported
    }

    /** Anti-pattern: close() then deleteTopics(). The cluster mutation never happens. */
    public void closeThenDeleteTopics() {
        Properties props = baseAdminProps();
        Admin admin = Admin.create(props);
        admin.close();
        admin.deleteTopics(List.of("orders")); // reported
    }

    /** Anti-pattern: close() then listTopics(). The audit query throws IllegalStateException. */
    public void closeThenListTopics() {
        Properties props = baseAdminProps();
        Admin admin = Admin.create(props);
        admin.close();
        admin.listTopics(); // reported
    }

    /** Anti-pattern: close(Duration) then describeCluster(). Exercises receiver-resolution backward simulation with the timeout arg. */
    public void closeWithTimeoutThenDescribeCluster() {
        Properties props = baseAdminProps();
        Admin admin = Admin.create(props);
        admin.close(Duration.ofSeconds(5));
        admin.describeCluster(); // reported
    }

    /** Anti-pattern: close() then incrementalAlterConfigs(new ConfigResource(...), ...). Nested arg expression — exercises receiver-resolution. */
    public void closeThenIncrementalAlterConfigs() {
        Properties props = baseAdminProps();
        Admin admin = Admin.create(props);
        admin.close();
        admin.incrementalAlterConfigs(java.util.Map.of(
                new ConfigResource(ConfigResource.Type.TOPIC, "orders"),
                java.util.List.of())); // reported
    }

    private static Properties baseAdminProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("client.id", "bad-admin-used-after-close");
        return p;
    }
}
