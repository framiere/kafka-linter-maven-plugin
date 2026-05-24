package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * RULE: ADMIN_USED_AFTER_CLOSE — method-reference variant.
 *
 * <p>Each method in this fixture closes a local {@code Admin} and THEN captures
 * an {@code admin::<lifecycle>} method reference into a downstream consumer
 * (an {@link ExecutorService#submit submit()} taking a {@code Callable<R>}, or
 * a {@link CompletableFuture#supplyAsync supplyAsync()} taking a
 * {@code Supplier<R>}). When the captured functional interface is later
 * invoked — on the executor thread or on the async stage — the actual call
 * into the admin hits an internal {@code closed=true} flag and the admin
 * throws {@code IllegalStateException("AdminClient has been closed")}. In an
 * unobserved {@code Future} / unobserved {@code CompletableFuture} the
 * exception is silently swallowed and the cluster mutation never happens.
 *
 * <h2>Why this variant uses {@code submit} and {@code supplyAsync} (not
 * {@code execute} / {@code runAsync})</h2>
 *
 * Every public method on {@code Admin} returns a {@code Result} type
 * ({@code ListTopicsResult}, {@code DescribeClusterResult}, ...). None
 * returns {@code void}. That makes {@code admin::<method>} incompatible with
 * a {@code Runnable} target (its SAM is {@code void run()}) — so the
 * {@code execute(Runnable)} and {@code runAsync(Runnable)} shapes used by
 * the producer / consumer method-ref ITs cannot be used here. Instead this
 * fixture uses the value-returning SAMs: {@code Callable<R>} (via
 * {@code submit}) and {@code Supplier<R>} (via {@code supplyAsync}).
 *
 * <h2>Why javac compiles a {@code REF_invokeInterface Admin.<method>:(...)}
 * handle into the {@code INVOKEDYNAMIC}, not a {@code REF_invokeVirtual}</h2>
 *
 * The static type of the receiver is the interface {@code Admin} (not the
 * concrete {@code KafkaAdminClient}). For a method reference whose receiver
 * has interface type, javac compiles to {@code REF_invokeInterface}; this is
 * why {@code AdminUsedAfterCloseRule} matches against {@link
 * org.apache.kafka.clients.admin.Admin Admin} AND
 * {@link org.apache.kafka.clients.admin.AdminClient AdminClient} via the
 * {@code ADMIN_OWNERS} set, the same way the consumer rule matches both
 * {@code Consumer} and {@code KafkaConsumer}.
 */
public final class BadAdminMethodRefAfterClose {

    /**
     * close() then executor.submit(admin::listTopics). submit's
     * {@code Callable<R>} overload is the only compatible target (listTopics
     * returns {@code ListTopicsResult}, not void). The executor thread invokes
     * call() later, which calls listTopics() on a closed admin and throws into
     * the returned {@code Future} no one ever calls {@code .get()} on — silent
     * swallow.
     */
    public void closeThenSubmitListTopicsRef() {
        Properties props = baseAdminProps();
        Admin admin = Admin.create(props);
        ExecutorService exec = Executors.newSingleThreadExecutor();
        admin.close();
        // Cast to Callable<R> disambiguates submit's Runnable / Callable<T> overloads;
        // listTopics has two overloads of its own so the SAM target type must be pinned.
        Future<ListTopicsResult> ignored = exec.submit((Callable<ListTopicsResult>) admin::listTopics); // reported
        exec.shutdown();
    }

    /**
     * close() then CompletableFuture.supplyAsync(admin::describeCluster). The
     * supplyAsync stage runs on the common pool after close() has already torn
     * down the AdminClientRunnable; describeCluster() throws into a stage no
     * one .join()s.
     */
    public void closeThenSupplyAsyncDescribeClusterRef() {
        Properties props = baseAdminProps();
        Admin admin = Admin.create(props);
        admin.close();
        CompletableFuture<DescribeClusterResult> ignored =
                CompletableFuture.supplyAsync(admin::describeCluster); // reported
    }

    /**
     * close() then CompletableFuture.supplyAsync(admin::listConsumerGroups).
     * The classic deploy-pipeline shape: the pipeline closes the admin in a
     * finally block while a parallel group-listing audit is still in flight on
     * the common pool. The audit fails silently; the next pipeline run sees
     * "audit never ran" and re-attempts it.
     */
    public void closeThenSupplyAsyncListConsumerGroupsRef() {
        Properties props = baseAdminProps();
        Admin admin = Admin.create(props);
        admin.close();
        CompletableFuture<ListConsumerGroupsResult> ignored =
                CompletableFuture.supplyAsync(admin::listConsumerGroups); // reported
    }

    /**
     * close() then submit(admin::listTopics) again from a different shape —
     * the {@code ExecutorService} variant complements the {@code
     * CompletableFuture} variant above by exercising the
     * {@code submit(Callable<T>)} resolution path through a different downstream
     * API. Same hazard, same silent failure mode.
     */
    public void closeWithTimeoutThenSubmitListTopicsRef() {
        Properties props = baseAdminProps();
        Admin admin = Admin.create(props);
        ExecutorService exec = Executors.newSingleThreadExecutor();
        admin.close(java.time.Duration.ofSeconds(5));
        // Cast to Callable<R> disambiguates submit's Runnable / Callable<T> overloads;
        // listTopics has two overloads of its own so the SAM target type must be pinned.
        Future<ListTopicsResult> ignored = exec.submit((Callable<ListTopicsResult>) admin::listTopics); // reported
        exec.shutdown();
    }

    private static Properties baseAdminProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("client.id", "bad-admin-method-ref-after-close");
        return p;
    }
}
