package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * RULE: KAFKA_FUTURE_GET_NO_TIMEOUT.
 *
 * Fires when {@code KafkaFuture.get()} (the no-argument, unbounded overload) is
 * called — surrenders the caller's deadline to the kafka-clients machinery.
 * Replace with {@code .get(timeout, TimeUnit)} matched to the surrounding
 * context's deadline.
 */
public final class BadKafkaFutureGetNoTimeout {

    private Admin newAdmin(Properties p) {
        return Admin.create(p);
    }

    /** Anti-pattern: .all().get() on createTopics — blocks until the controller responds. */
    public void createTopicsAllGet(Properties p) throws InterruptedException, ExecutionException {
        Admin admin = newAdmin(p);
        try {
            CreateTopicsResult r = admin.createTopics(List.of(new NewTopic("orders", 6, (short) 3)));
            r.all().get(); // FIRES — unbounded wait
        } finally {
            admin.close();
        }
    }

    /** Anti-pattern: .get() on a per-topic KafkaFuture pulled from .values(). */
    public void createTopicsPerTopicGet(Properties p) throws InterruptedException, ExecutionException {
        Admin admin = newAdmin(p);
        try {
            CreateTopicsResult r = admin.createTopics(List.of(new NewTopic("orders", 6, (short) 3)));
            KafkaFuture<Void> f = r.values().get("orders");
            f.get(); // FIRES — unbounded wait on a single-topic future
        } finally {
            admin.close();
        }
    }

    /** Anti-pattern: .all().get() on deleteTopics. */
    public void deleteTopicsAllGet(Properties p) throws InterruptedException, ExecutionException {
        Admin admin = newAdmin(p);
        try {
            DeleteTopicsResult r = admin.deleteTopics(List.of("scratch"));
            r.all().get(); // FIRES
        } finally {
            admin.close();
        }
    }

    /** Anti-pattern: .nodes().get() on describeCluster (the result accessor itself returns a KafkaFuture). */
    public void describeClusterNodesGet(Properties p) throws InterruptedException, ExecutionException {
        Admin admin = newAdmin(p);
        try {
            DescribeClusterResult r = admin.describeCluster();
            r.nodes().get(); // FIRES
        } finally {
            admin.close();
        }
    }

    /** Anti-pattern: .get() on a KafkaFuture variable assigned earlier. */
    public void getViaIntermediateVariable(Properties p) throws InterruptedException, ExecutionException {
        Admin admin = newAdmin(p);
        try {
            KafkaFuture<?> future = admin.listTopics().listings();
            future.get(); // FIRES — same hazard, just a different syntactic shape
        } finally {
            admin.close();
        }
    }

    /** Control: .get(30, TimeUnit.SECONDS) — bounded wait. Must NOT fire. */
    public void createTopicsAllGetBounded(Properties p)
            throws InterruptedException, ExecutionException, TimeoutException {
        Admin admin = newAdmin(p);
        try {
            CreateTopicsResult r = admin.createTopics(List.of(new NewTopic("orders", 6, (short) 3)));
            r.all().get(30, TimeUnit.SECONDS); // OK — bounded overload
        } finally {
            admin.close();
        }
    }

    /** Control: .get(5, TimeUnit.MINUTES) — bounded wait, longer deadline. Must NOT fire. */
    public void deleteTopicsAllGetBoundedLong(Properties p)
            throws InterruptedException, ExecutionException, TimeoutException {
        Admin admin = newAdmin(p);
        try {
            DeleteTopicsResult r = admin.deleteTopics(List.of("scratch"));
            r.all().get(5, TimeUnit.MINUTES); // OK
        } finally {
            admin.close();
        }
    }
}
