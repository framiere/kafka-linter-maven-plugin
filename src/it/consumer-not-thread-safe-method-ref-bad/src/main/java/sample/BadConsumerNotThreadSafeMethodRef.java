package sample;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * RULE: CONSUMER_NOT_THREAD_SAFE — direct method-reference variant.
 *
 * <p>Each method in this fixture captures a {@code KafkaConsumer} method
 * (other than {@code wakeup}) as a method reference and hands it to a thread
 * sink: an {@link ExecutorService}, a fresh {@link Thread}, a
 * {@link ScheduledExecutorService}, or {@link CompletableFuture#runAsync}.
 *
 * <h2>Why this is dangerous</h2>
 *
 * {@code KafkaConsumer} is documented as not thread-safe. Every public method
 * other than {@code wakeup()} enters {@code acquireAndEnsureOpen()}, which
 * records the calling thread and throws {@code ConcurrentModificationException}
 * ("KafkaConsumer is not safe for multi-threaded access") on the first call
 * from any thread other than the recorded owner. The moment the SAM adapter
 * for the captured method-ref runs on the worker / scheduler / shutdown-hook
 * thread, that contract is violated and the next consumer method on the poll
 * thread (or vice versa) throws.
 *
 * <h2>Why the naive scan misses this</h2>
 *
 * {@code consumer::commitSync} compiles to an {@code INVOKEDYNAMIC} whose
 * bsm-arg list contains a direct
 * {@code REF_invokeVirtual KafkaConsumer.commitSync:()V} handle. The user
 * class's bytecode contains <strong>zero {@code INVOKE*} instructions
 * targeting {@code commitSync}</strong>. The previous version of this rule
 * scanned for "lambda$N synthetic bodies in this class that touch a
 * non-wakeup consumer method", which by construction finds nothing on a
 * direct method-ref: there is no synthetic body — the LambdaMetafactory-
 * generated SAM adapter lives in the JDK, not in the user class.
 *
 * <p>The fix is to also accept impl handles whose owner is in
 * {@code CONSUMER_OWNERS} (KafkaConsumer / Consumer) and whose name is not
 * {@code wakeup}. The dispatch-site check (next significant instruction is
 * a recognised thread dispatcher) is identical to the lambda-body path.
 *
 * <h2>Why no SAM-return discriminator</h2>
 *
 * Some consumer methods return values ({@code position}, {@code assignment},
 * {@code subscription}) and would compile to a {@code Supplier} / {@code Function}
 * SAM; others return {@code void} ({@code commitSync}, {@code unsubscribe},
 * {@code close}) and compile to {@code Runnable} / {@code Consumer}. The
 * thread-safety hazard is the same in both cases: the call materialises on
 * the worker thread, regardless of whether the caller observes the return
 * value or discards it. So the rule fires on every non-wakeup capture
 * dispatched to a recognised sink, regardless of SAM return type.
 *
 * <p>Expected fires from this fixture: FOUR — one for each capture site
 * below. The wakeup controls at the bottom must NOT fire.
 */
public final class BadConsumerNotThreadSafeMethodRef {

    /**
     * Executor sink — {@code consumer::commitSync} captured as a {@code Runnable}
     * (commitSync is no-arg void). Worker thread reaches into the consumer's
     * internal state on every {@code execute} call; throws CME the moment
     * the poll thread next touches the consumer.
     */
    public void executeCommitSyncRef(ExecutorService exec, KafkaConsumer<String, String> consumer) {
        exec.execute(consumer::commitSync); // reported
    }

    /**
     * Bare Thread — {@code consumer::commitAsync} captured as a {@code Runnable}.
     * commitAsync is async on the broker side but still touches consumer
     * state (the in-flight commits set, the coordinator's offsets-to-commit
     * cache), so cross-thread access is still a CME hazard.
     */
    public void freshThreadCommitAsyncRef(KafkaConsumer<String, String> consumer) {
        new Thread(consumer::commitAsync).start(); // reported
    }

    /**
     * Scheduled task — {@code consumer::unsubscribe} captured into a
     * {@code ScheduledExecutorService.schedule}. "Drop subscriptions after a
     * delay" looks innocent but unsubscribe mutates the consumer's
     * coordinator state and explicitly goes through acquireAndEnsureOpen.
     */
    public void scheduledUnsubscribeRef(ScheduledExecutorService sched, KafkaConsumer<String, String> consumer) {
        sched.schedule(consumer::unsubscribe, 30, TimeUnit.SECONDS); // reported
    }

    /**
     * CompletableFuture pipeline — {@code consumer::commitSync} dispatched
     * onto the common ForkJoinPool via {@code runAsync}. Most insidious
     * variant: developers reach for runAsync to "make commit non-blocking",
     * not realising that the cost is cross-thread access on a consumer that
     * forbids it.
     */
    public CompletableFuture<Void> runAsyncCommitSyncRef(KafkaConsumer<String, String> consumer) {
        return CompletableFuture.runAsync(consumer::commitSync); // reported
    }

    /**
     * Good control — {@code consumer::wakeup} captured into a shutdown hook.
     * wakeup() is explicitly designed for cross-thread interruption (the
     * only thread-safe consumer method); this is the canonical Kafka
     * shutdown idiom. Must NOT fire.
     */
    public void shutdownHookWakeupRef(KafkaConsumer<String, String> consumer) {
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
    }

    /**
     * Good control — {@code consumer::wakeup} captured into an executor.
     * Same logic as above: wakeup is the sanctioned cross-thread method.
     * Must NOT fire.
     */
    public void executeWakeupRef(ExecutorService exec, KafkaConsumer<String, String> consumer) {
        exec.execute(consumer::wakeup);
    }
}
