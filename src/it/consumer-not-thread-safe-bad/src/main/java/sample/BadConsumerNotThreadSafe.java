package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * RULE: CONSUMER_NOT_THREAD_SAFE — per-call-site bytecode rule that
 * combines lambda-body analysis with dispatch-site recognition.
 *
 * Two-step semantics:
 *   (1) Inside the class, find every lambda whose body invokes a
 *       method on a KafkaConsumer / Consumer instance OTHER THAN
 *       `wakeup()`. The rule maintains a set of these
 *       "consumer-touching lambdas" — they are flagged because the
 *       moment such a lambda is executed off the poll thread, the
 *       runtime contract is violated.
 *   (2) For every INVOKEDYNAMIC instruction in the class that
 *       PRODUCES one of these consumer-touching lambdas, check the
 *       NEXT significant instruction. If it is a recognized
 *       dispatcher — ExecutorService.submit/execute/invokeAll/
 *       invokeAny, ScheduledExecutorService.schedule/
 *       scheduleAtFixedRate/scheduleWithFixedDelay,
 *       ForkJoinPool.submit/execute/invoke, CompletableFuture.
 *       runAsync/supplyAsync/thenRunAsync/thenApplyAsync/
 *       thenAcceptAsync, or Thread.&lt;init&gt; — the rule fires.
 *
 * Why this matters:
 *
 *   KafkaConsumer is documented as "not safe for multi-threaded
 *   access". Internally it asserts on a thread-local check at the
 *   start of nearly every public method: if a different thread
 *   touches the consumer than the one that touched it last, the
 *   consumer throws
 *     ConcurrentModificationException:
 *       "KafkaConsumer is not safe for multi-threaded access"
 *
 *   The ONLY exception is `wakeup()` — explicitly designed for
 *   cross-thread interruption, since the typical shutdown pattern
 *   is "another thread (signal handler, shutdown hook) calls
 *   consumer.wakeup() to break the poll thread out of poll()". The
 *   rule excludes wakeup-only lambdas for this reason — see
 *   GoodWakeupOnlyOffload below.
 *
 * Operational impact:
 *
 *   - Crashes the first time the worker thread touches the
 *     consumer. Often masked in dev/local by accidental thread
 *     coincidence (single-threaded executor) and surfaces only under
 *     prod concurrency.
 *   - Even when not crashing, breaks the consumer's heartbeat
 *     thread coordination — group coordinator may evict the
 *     consumer mid-rebalance.
 *   - Particularly insidious because the OFFLOAD shape (`process
 *     each record on a worker thread`) is a legitimate pattern —
 *     just it must offload the VALUE/RECORD, not the consumer
 *     reference. The right shape is: poll() on the poll thread,
 *     submit the BATCH for processing, commitSync() on the poll
 *     thread after the batch completes.
 *
 * Each method below contains exactly one Bad call site → one fire.
 * Total: 4 violations in this class. The
 * ScheduledExecutorService.schedule/scheduleAtFixedRate shapes are
 * caught in the sibling BadConsumerScheduledDispatch fixture (2
 * additional fires).
 */
public class BadConsumerNotThreadSafe {

    private final Consumer<String, String> consumer;
    private final ExecutorService executor = Executors.newFixedThreadPool(4);

    public BadConsumerNotThreadSafe(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    /**
     * Shape 1: ExecutorService.submit(lambda) where the lambda
     * invokes `consumer.poll(...)`. Classic "let's parallelize the
     * polling" anti-pattern.
     *
     * Bytecode: the lambda body is compiled to `lambda$shape1$0`
     * (or similar), which contains
     *   INVOKEINTERFACE Consumer.poll(Duration)
     * That makes the lambda a "consumer-touching, non-wakeup"
     * lambda. The dispatch site is
     *   INVOKEDYNAMIC ... (Runnable) — produces lambda instance
     *   INVOKEINTERFACE ExecutorService.submit(Runnable)
     * which matches DISPATCH_METHODS["ExecutorService"] = "submit".
     * Rule fires on the INVOKEDYNAMIC.
     *
     * Crash shape: first iteration succeeds (the poll thread that
     * submitted the task and the executor worker happen to share
     * thread-affinity nothing). Second iteration throws
     * ConcurrentModificationException from the consumer's internal
     * `acquireAndEnsureOpen()` check.
     */
    public void parallelPollSubmit() {
        // FIRES: lambda touching consumer.poll, submitted to ExecutorService.
        executor.submit(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> r : records) {
                System.out.println(r.value());
            }
        });
    }

    /**
     * Shape 2: ExecutorService.execute(lambda) where the lambda
     * calls `consumer.commitSync()`. Common after async record
     * processing — the developer assumes commitSync is OK from a
     * worker because it "just sends an offset commit", but it
     * mutates internal consumer state (the commit-in-flight set,
     * the heartbeat coordinator's offsets-to-commit cache).
     *
     * Bytecode: the lambda body is `lambda$shape2$N` containing
     *   INVOKEINTERFACE Consumer.commitSync()
     * — consumer-touching, non-wakeup. The dispatch is
     *   INVOKEDYNAMIC ... (Runnable)
     *   INVOKEINTERFACE ExecutorService.execute(Runnable)
     * matching DISPATCH_METHODS["ExecutorService"] = "execute".
     */
    public void asyncCommitExecute() {
        // FIRES: lambda touching consumer.commitSync, dispatched via execute.
        executor.execute(() -> {
            consumer.commitSync();
        });
    }

    /**
     * Shape 3: `new Thread(lambda).start()` where the lambda
     * touches the consumer. The "we'll just spin up a thread for
     * this" naive pattern — bypasses any pool, but the
     * thread-safety problem is identical.
     *
     * Bytecode: the lambda becomes `lambda$shape3$N` with
     *   INVOKEINTERFACE Consumer.subscribe(...)
     * — consumer-touching, non-wakeup. The dispatch is
     *   NEW java/lang/Thread
     *   DUP
     *   INVOKEDYNAMIC ... (Runnable)
     *   INVOKESPECIAL java/lang/Thread.&lt;init&gt;(Runnable)V
     * The rule's `nextSignificant(indy)` skips past the DUP/etc.
     * and lands on the Thread.&lt;init&gt; INVOKESPECIAL —
     * matching DISPATCH_METHODS["Thread"] = "&lt;init&gt;". The
     * `.start()` call after that is irrelevant to the rule (the
     * thread-safety violation is sealed once the lambda crosses the
     * thread boundary).
     */
    public void naiveThreadStart() {
        // FIRES: lambda touching consumer.subscribe, passed to Thread ctor.
        new Thread(() -> {
            consumer.subscribe(java.util.List.of("orders"));
        }).start();
    }

    /**
     * Shape 4: CompletableFuture.runAsync(lambda) where the lambda
     * touches the consumer. The "modern async" shape — looks
     * fluent and contemporary, but runAsync uses ForkJoinPool.
     * commonPool() by default, so the lambda runs on a worker
     * thread other than the caller.
     *
     * Bytecode dispatch:
     *   INVOKEDYNAMIC ... (Runnable)
     *   INVOKESTATIC CompletableFuture.runAsync(Runnable)
     * matching DISPATCH_METHODS["CompletableFuture"] = "runAsync".
     */
    public void completableFutureAsync() {
        // FIRES: lambda touching consumer.assignment, run on CompletableFuture.runAsync.
        CompletableFuture.runAsync(() -> {
            consumer.assignment().forEach(tp -> System.out.println(tp));
        });
    }
}
