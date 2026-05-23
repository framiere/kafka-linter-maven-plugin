package sample;

import java.time.Duration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * RULE: COMMIT_ASYNC_NO_FINAL_SYNC.
 *
 * <p>Fires when a class uses {@code Consumer.commitAsync(...)} in its
 * poll loop but never calls {@code Consumer.commitSync(...)} anywhere
 * in the same class. That combination — async-only commits with no
 * synchronous safety net — is one of the highest-magnitude correctness
 * bugs the linter detects, because it silently loses the LAST batch
 * of commits on every clean shutdown.
 *
 * <p>The shutdown sequence that causes the loss:
 * <ol>
 *   <li>The consumer processes a batch of records and calls
 *       {@code consumer.commitAsync()}. This schedules a commit but
 *       returns IMMEDIATELY — the actual {@code OffsetCommitRequest}
 *       hasn't been sent yet; it sits in a queue waiting for the next
 *       poll() iteration to flush it.</li>
 *   <li>The shutdown hook fires (SIGTERM, k8s graceful stop, etc.).
 *       The application's normal idiom is something like:
 *       <pre>{@code
 *         running.set(false);  // signals poll loop to exit
 *         consumer.wakeup();   // breaks out of an in-flight poll()
 *         consumer.close();    // cleans up
 *       }</pre>
 *       Note: {@code close()} does NOT flush pending async commits.
 *       It calls the {@code OffsetCommitCallback} for any pending
 *       commits with a {@code DisconnectException}, but the offset
 *       itself is never persisted.</li>
 *   <li>The last commitAsync's offset is now lost. When the consumer
 *       restarts, it begins from the last SUCCESSFULLY-PERSISTED
 *       offset — which can be hundreds or thousands of records
 *       behind, leading to large-scale reprocessing of records the
 *       application has already handled.</li>
 * </ol>
 *
 * <p>For exactly-once or even at-least-once-with-bounded-redelivery
 * pipelines, this loss is catastrophic. The canonical pattern that
 * eliminates it:
 * <pre>{@code
 *   try {
 *       while (running.get()) {
 *           ConsumerRecords<K, V> recs = consumer.poll(Duration.ofMillis(500));
 *           process(recs);
 *           consumer.commitAsync();  // fast path: throughput-friendly
 *       }
 *   } catch (WakeupException wakeUp) {
 *       // shutdown requested
 *   } finally {
 *       try {
 *           consumer.commitSync();   // safety net: blocks until acked
 *       } finally {
 *           consumer.close();
 *       }
 *   }
 * }</pre>
 *
 * <p>The async commit gives the throughput properties of fire-and-forget
 * during normal operation. The final sync commit gives the durability
 * properties of "this offset is on the broker before I shut down." Both
 * are required — neither is sufficient alone.
 *
 * <p>What this looks like in production:
 * <ol>
 *   <li>The consumer is configured with the async-only pattern. Local
 *       and staging testing rarely exercises shutdown timing precisely
 *       (most tests use Ctrl-C / SIGKILL, which never reaches the
 *       graceful path anyway).</li>
 *   <li>The first time a k8s pod is rescheduled or a deploy rolls
 *       through, every consumer instance loses its tail commits. The
 *       new pods come up and start reading from the prior committed
 *       offset, double-processing whatever records had been async-committed
 *       but not yet flushed.</li>
 *   <li>Downstream — depending on idempotency — this manifests as
 *       duplicate emails, double-charged invoices, or
 *       inventory-overcommit. The signature is "every deploy reprocesses
 *       1–5 minutes of traffic."</li>
 * </ol>
 *
 * <p>What the rule catches: the rule's scope is CLASS-LEVEL (not method),
 * because the typical idiom puts {@code commitAsync()} in one method
 * (the poll loop) and {@code commitSync()} in another (the shutdown
 * hook / finally block). It walks every method body of the class, and:
 * <ul>
 *   <li>If ANY method calls {@code commitSync()} on a Consumer owner,
 *       the class is considered safe (no fire).</li>
 *   <li>If NO method calls {@code commitSync()} but at least one method
 *       calls {@code commitAsync()}, the first {@code commitAsync()}
 *       site is reported.</li>
 * </ul>
 *
 * <p>This file contains the BAD class. The matching control — a class
 * that uses commitAsync in the poll loop AND commitSync in a finally
 * block — lives in {@link GoodCommitAsyncWithFinalSync} (separate
 * top-level class, so the linter sees it as a separate ClassNode and
 * applies the class-scoped rule independently).
 */
public final class BadCommitAsyncNoFinalSync {

    private final Consumer<String, String> consumer;

    public BadCommitAsyncNoFinalSync(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    /** Anti-pattern: only commitAsync in the poll loop, no commitSync anywhere in this class — FIRES. */
    public void runPollLoop() {
        while (true) {
            ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
            process(recs);
            consumer.commitAsync(); // FIRES — the only commit in the class is async
        }
    }

    /** Helper called from {@link #runPollLoop}. The shutdown path closes the consumer but never commitSyncs. */
    public void shutdown() {
        consumer.wakeup();
        consumer.close();
        // Notice: NO commitSync() here. The last async commit will be lost.
    }

    private void process(ConsumerRecords<String, String> recs) {
        if (recs.isEmpty()) {
            return;
        }
    }
}
