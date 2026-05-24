package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Control for ADMIN_RESULT_DISCARDED.
 *
 * <p>Six methods that each AVOID the rule for a DIFFERENT reason — the orthogonal
 * exits the rule's check supports:
 *
 * <ol>
 *   <li>{@code createTopicsAwaitedAllGet} — the textbook correct shape: the
 *       Result is consumed via {@code .all().get(timeout, unit)}. After the
 *       call, the next significant instruction is {@code INVOKEVIRTUAL all},
 *       not {@code POP}. Silent.</li>
 *   <li>{@code deleteTopicsResultStoredInLocal} — Result stored in a local
 *       via {@code ASTORE N}. The next significant instruction is {@code ASTORE},
 *       not {@code POP}. Silent.</li>
 *   <li>{@code createTopicsResultReturned} — Result returned via {@code ARETURN}.
 *       The next significant instruction is {@code ARETURN}, not {@code POP}.
 *       Silent. (Caller is responsible for observing; that's by design.)</li>
 *   <li>{@code listTopicsThenDiscard} — discarding the Result of a READ-ONLY
 *       describe / list operation. {@code listTopics} is NOT in the
 *       state-mutating set; discarding its Result wastes a round-trip but does
 *       not corrupt cluster state. Silent.</li>
 *   <li>{@code describeClusterThenDiscard} — same exit as above for a different
 *       read-only method. Silent.</li>
 *   <li>{@code createTopicsResultUsedAsExpression} — Result is the receiver of
 *       a chained call ({@code .values().get(\"orders\")} → KafkaFuture →
 *       {@code .get(timeout, unit)}); per-future error handling rather than
 *       all-or-nothing. The next significant instruction after createTopics is
 *       {@code INVOKEVIRTUAL values}, not {@code POP}. Silent.</li>
 * </ol>
 */
public final class GoodAdminResultObserved {

    /** Canonical observation pattern: chain .all().get(timeout, unit) to surface broker failures synchronously. */
    public void createTopicsAwaitedAllGet() throws InterruptedException, ExecutionException, TimeoutException {
        Admin admin = Admin.create(baseAdminProps());
        try {
            admin.createTopics(Collections.singletonList(new NewTopic("orders", 3, (short) 2)))
                    .all().get(30, TimeUnit.SECONDS); // silent: next insn is INVOKEVIRTUAL all, not POP
        } finally {
            admin.close();
        }
    }

    /** Result stored in a local variable — the next instruction is ASTORE, not POP. */
    public void deleteTopicsResultStoredInLocal() throws InterruptedException, ExecutionException {
        Admin admin = Admin.create(baseAdminProps());
        try {
            DeleteTopicsResult result = admin.deleteTopics(List.of("orders")); // silent: ASTORE follows
            result.all().get();
        } finally {
            admin.close();
        }
    }

    /** Result returned to the caller — the next instruction is ARETURN, not POP. */
    public CreateTopicsResult createTopicsResultReturned() {
        Admin admin = Admin.create(baseAdminProps());
        try {
            return admin.createTopics(Collections.singletonList(new NewTopic("orders", 3, (short) 2))); // silent: ARETURN
        } finally {
            admin.close();
        }
    }

    /** listTopics is READ-ONLY — discarding its Result wastes a round-trip but does not corrupt state. Silent. */
    public void listTopicsThenDiscard() {
        Admin admin = Admin.create(baseAdminProps());
        try {
            admin.listTopics(); // silent: listTopics not in state-mutating set
        } finally {
            admin.close();
        }
    }

    /** describeCluster is READ-ONLY — same exit as listTopics. Silent. */
    public void describeClusterThenDiscard() {
        Admin admin = Admin.create(baseAdminProps());
        try {
            admin.describeCluster(); // silent: describeCluster not in state-mutating set
        } finally {
            admin.close();
        }
    }

    /** Result used as receiver of a chained call — next instruction is INVOKEVIRTUAL values, not POP. */
    public void createTopicsResultUsedAsExpression() throws InterruptedException, ExecutionException, TimeoutException {
        Admin admin = Admin.create(baseAdminProps());
        try {
            KafkaFuture<Void> future = admin.createTopics(Collections.singletonList(new NewTopic("orders", 3, (short) 2)))
                    .values().get("orders"); // silent: chained, not discarded
            future.get(30, TimeUnit.SECONDS);
        } finally {
            admin.close();
        }
    }

    /** Two more read-only sentinels: just for symmetry — assert ListTopicsResult / DescribeClusterResult are never flagged. */
    public void mixedReadOnlyDiscard() {
        Admin admin = Admin.create(baseAdminProps());
        try {
            ListTopicsResult ltr = admin.listTopics(); // silent: read-only
            DescribeClusterResult dcr = admin.describeCluster(); // silent: read-only
            // Variables used to keep javac from warning, but no .get() — discarding read-only Result is fine per the rule's scope.
            if (ltr == null || dcr == null) {
                throw new IllegalStateException("unreachable");
            }
        } finally {
            admin.close();
        }
    }

    private static Properties baseAdminProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("client.id", "good-admin-result-observed");
        return p;
    }
}
