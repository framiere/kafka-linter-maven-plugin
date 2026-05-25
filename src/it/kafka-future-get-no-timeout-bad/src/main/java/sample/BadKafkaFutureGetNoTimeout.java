package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * RULE: KAFKA_FUTURE_GET_NO_TIMEOUT — must fire on EVERY method
 * below.
 *
 * <p>Each method exercises one of the bytecode shapes the rule is
 * required to catch:
 *
 * <ol>
 *   <li>direct {@code INVOKEVIRTUAL} on
 *       {@code KafkaFuture.get()Ljava/lang/Object;};</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code future::get} bound to {@link Callable} or a custom
 *       {@code @FunctionalInterface} whose erased SAM descriptor
 *       matches the no-arg get.</li>
 * </ol>
 *
 * <h2>Why a custom throwing SAM, not {@link java.util.function.Supplier}</h2>
 *
 * <p>{@link org.apache.kafka.common.KafkaFuture#get()} declares
 * {@code throws InterruptedException, ExecutionException}, but
 * {@link java.util.function.Supplier#get} declares no checked
 * exceptions. A {@code future::get} method reference bound to
 * {@code Supplier<T>} does NOT compile — javac rejects the functional
 * expression as having "incompatible thrown types". The compiler-
 * accepted SAM shapes are therefore (a) the JDK's {@link Callable}
 * which declares {@code throws Exception} (a superset of
 * InterruptedException + ExecutionException), and (b) any custom
 * {@code @FunctionalInterface} whose abstract method declares a
 * superset of those checked exceptions. Both produce the same
 * {@code INVOKEDYNAMIC} + {@code LambdaMetafactory} indy site whose
 * bsm-args contain a {@code REF_invokeVirtual} handle pointing at
 * {@code KafkaFuture.get()Ljava/lang/Object;}.
 *
 * <h2>Why the bounded {@code .get(long, TimeUnit)} overload exists</h2>
 *
 * <p>The unbounded {@code .get()} parks the calling thread until the
 * kafka-clients machinery resolves the future, with no caller-side
 * deadline. Under a slow/unreachable/stuck controller the call hangs
 * indefinitely; HTTP request handlers exhaust their thread pool, Pod
 * terminationGracePeriodSeconds is exceeded, reconciliation loops
 * silently fall behind without an error to alert on, and
 * CompletableFuture stages composed downstream remain pending
 * forever. The bounded {@code .get(long, TimeUnit)} returns control
 * to the caller after the deadline so it can retry, surface the
 * failure, or shed load.
 */
public final class BadKafkaFutureGetNoTimeout {

    /**
     * Custom throwing SAM. Used to demonstrate the
     * {@code future::get} indy capture against a real, production-
     * shape functional interface that compiles with KafkaFuture.get's
     * checked exceptions.
     */
    @FunctionalInterface
    public interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    private Admin newAdmin(Properties p) {
        return Admin.create(p);
    }

    // ===== Direct INVOKEVIRTUAL on KafkaFuture.get()Ljava/lang/Object; =====

    /**
     * MUST FIRE — {@code .all().get()} on createTopics. Hangs forever
     * if the controller is unreachable; the request thread that
     * called this never returns.
     */
    public void createTopicsAllGet(Properties p) throws InterruptedException, ExecutionException {
        Admin admin = newAdmin(p);
        try {
            CreateTopicsResult r = admin.createTopics(List.of(new NewTopic("orders", 6, (short) 3)));
            r.all().get();
        } finally {
            admin.close();
        }
    }

    /**
     * MUST FIRE — {@code .get()} on a per-topic future pulled out of
     * {@code .values()}. Same hazard at a finer granularity.
     */
    public void createTopicsPerTopicGet(Properties p) throws InterruptedException, ExecutionException {
        Admin admin = newAdmin(p);
        try {
            CreateTopicsResult r = admin.createTopics(List.of(new NewTopic("orders", 6, (short) 3)));
            KafkaFuture<Void> f = r.values().get("orders");
            f.get();
        } finally {
            admin.close();
        }
    }

    /**
     * MUST FIRE — {@code .all().get()} on deleteTopics. Pod
     * terminationGracePeriodSeconds is at risk under a slow
     * controller; the kubelet sends SIGKILL while the call is still
     * blocked.
     */
    public void deleteTopicsAllGet(Properties p) throws InterruptedException, ExecutionException {
        Admin admin = newAdmin(p);
        try {
            DeleteTopicsResult r = admin.deleteTopics(List.of("scratch"));
            r.all().get();
        } finally {
            admin.close();
        }
    }

    /**
     * MUST FIRE — {@code .nodes().get()} on describeCluster.
     * Reconciliation-loop callers stretch from seconds to hours under
     * a slow controller; reconcile-lag metrics climb without an error
     * to alert on.
     */
    public void describeClusterNodesGet(Properties p) throws InterruptedException, ExecutionException {
        Admin admin = newAdmin(p);
        try {
            DescribeClusterResult r = admin.describeCluster();
            r.nodes().get();
        } finally {
            admin.close();
        }
    }

    /**
     * MUST FIRE — {@code .get()} on a KafkaFuture-typed local variable
     * assigned earlier. Same hazard at a different syntactic shape;
     * an AST-shape-based lint that only looks at method-chain
     * expressions misses this.
     */
    public void getViaIntermediateVariable(Properties p) throws InterruptedException, ExecutionException {
        Admin admin = newAdmin(p);
        try {
            KafkaFuture<?> future = admin.listTopics().listings();
            future.get();
        } finally {
            admin.close();
        }
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /**
     * MUST FIRE — {@code future::get} bound to {@link Callable}. The
     * user-class bytecode contains ZERO direct INVOKEVIRTUAL on
     * KafkaFuture.get; only an INVOKEDYNAMIC + LambdaMetafactory bridge
     * whose bsm-args contain a REF_invokeVirtual handle on
     * {@code KafkaFuture.get()Ljava/lang/Object;}. The Callable SAM
     * is the dominant production shape for bridging kafka-clients
     * futures into {@link ExecutorService#submit(Callable)} or
     * {@code reactor.core.publisher.Mono.fromCallable(Callable)}
     * pipelines.
     */
    public Callable<?> capturedAsCallable(Properties p) {
        Admin admin = newAdmin(p);
        KafkaFuture<?> future = admin.createTopics(List.of(new NewTopic("orders", 6, (short) 3))).all();
        return future::get;
    }

    /**
     * MUST FIRE — {@code future::get} bound to a custom
     * {@code @FunctionalInterface ThrowingSupplier<T>} whose abstract
     * method declares {@code throws Exception}. Production codebases
     * routinely declare such SAMs to bridge checked-exception
     * blocking calls into otherwise-Supplier-shaped APIs (Vavr
     * CheckedFunction0, Cyclops Try, custom retry helpers); the indy
     * capture is identical to the Callable case.
     */
    public ThrowingSupplier<?> capturedAsThrowingSupplier(Properties p) {
        Admin admin = newAdmin(p);
        KafkaFuture<?> future = admin.deleteTopics(List.of("scratch")).all();
        return future::get;
    }

    /**
     * MUST FIRE — {@code future::get} bridged through
     * {@link ExecutorService#submit(Callable)}. Universal pattern
     * when bridging a kafka-clients KafkaFuture pipeline into a
     * thread-pool / {@link Future} pipeline. The resulting Future
     * carries the unbounded-hang hazard forward — every downstream
     * {@code futureFromSubmit.get(...)} call inherits the blocking
     * semantics of the underlying KafkaFuture.
     */
    public Future<?> bridgedToExecutor(Properties p, ExecutorService executor) {
        Admin admin = newAdmin(p);
        KafkaFuture<?> future = admin.describeCluster().nodes();
        Callable<?> task = future::get;
        return executor.submit(task);
    }
}
