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
 * RULE: KAFKA_FUTURE_GET_NO_TIMEOUT — must NOT fire on any of the
 * methods below.
 *
 * <p>Every call site uses the bounded
 * {@link org.apache.kafka.common.KafkaFuture#get(long,
 * java.util.concurrent.TimeUnit)} overload, whose descriptor
 * {@code (JLjava/util/concurrent/TimeUnit;)Ljava/lang/Object;} is
 * strictly different from the no-arg
 * {@code ()Ljava/lang/Object;} descriptor that the rule targets.
 *
 * <p>The exact-descriptor predicate guarantees the bounded overload
 * is never flagged regardless of the surrounding context — the rule
 * predicate is {@code "()Ljava/lang/Object;".equals(desc)}.
 *
 * <p>Method-reference captures of the bounded overload have a
 * different SAM target type (a function with three arguments rather
 * than a Supplier-shaped {@code ()Ljava/lang/Object;}) and so do not
 * appear in indy bsm-args with the matching descriptor; no GOOD
 * indy-capture case is needed because no SAM in the JDK has the
 * {@code (long, TimeUnit) -> Object} shape.
 */
public final class GoodKafkaFutureGetNoTimeout {

    private Admin newAdmin(Properties p) {
        return Admin.create(p);
    }

    /**
     * OK — {@code .all().get(30, TimeUnit.SECONDS)} on createTopics.
     * Bounded by the typical HTTP request-budget deadline.
     */
    public void createTopicsAllGetBounded(Properties p)
            throws InterruptedException, ExecutionException, TimeoutException {
        Admin admin = newAdmin(p);
        try {
            CreateTopicsResult r = admin.createTopics(List.of(new NewTopic("orders", 6, (short) 3)));
            r.all().get(30, TimeUnit.SECONDS);
        } finally {
            admin.close();
        }
    }

    /**
     * OK — {@code .get(5, TimeUnit.MINUTES)} on a per-topic future.
     * Longer deadline tolerable in a CLI / backfill context.
     */
    public void createTopicsPerTopicGetBounded(Properties p)
            throws InterruptedException, ExecutionException, TimeoutException {
        Admin admin = newAdmin(p);
        try {
            CreateTopicsResult r = admin.createTopics(List.of(new NewTopic("orders", 6, (short) 3)));
            KafkaFuture<Void> f = r.values().get("orders");
            f.get(5, TimeUnit.MINUTES);
        } finally {
            admin.close();
        }
    }

    /**
     * OK — {@code .all().get(5, TimeUnit.MINUTES)} on deleteTopics.
     * Matches a Pod terminationGracePeriodSeconds budget minus a
     * buffer.
     */
    public void deleteTopicsAllGetBounded(Properties p)
            throws InterruptedException, ExecutionException, TimeoutException {
        Admin admin = newAdmin(p);
        try {
            DeleteTopicsResult r = admin.deleteTopics(List.of("scratch"));
            r.all().get(5, TimeUnit.MINUTES);
        } finally {
            admin.close();
        }
    }

    /**
     * OK — {@code .nodes().get(10, TimeUnit.SECONDS)} on
     * describeCluster. Caps the reconciliation-loop iteration
     * duration.
     */
    public void describeClusterNodesGetBounded(Properties p)
            throws InterruptedException, ExecutionException, TimeoutException {
        Admin admin = newAdmin(p);
        try {
            DescribeClusterResult r = admin.describeCluster();
            r.nodes().get(10, TimeUnit.SECONDS);
        } finally {
            admin.close();
        }
    }

    /**
     * OK — {@code .get(15, TimeUnit.SECONDS)} on a KafkaFuture-typed
     * local variable.
     */
    public void getViaIntermediateVariableBounded(Properties p)
            throws InterruptedException, ExecutionException, TimeoutException {
        Admin admin = newAdmin(p);
        try {
            KafkaFuture<?> future = admin.listTopics().listings();
            future.get(15, TimeUnit.SECONDS);
        } finally {
            admin.close();
        }
    }
}
