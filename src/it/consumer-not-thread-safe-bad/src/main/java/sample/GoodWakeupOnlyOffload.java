package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Silent: the lambda dispatched to another thread invokes ONLY
 * `consumer.wakeup()` — the single thread-safe method on
 * KafkaConsumer. This is the canonical cross-thread shutdown
 * pattern documented in the kafka-clients javadoc:
 *
 *   "wakeup is the only method on KafkaConsumer that is safe to
 *   call from another thread"
 *
 * The rule's lambda-body scan explicitly excludes "wakeup" — see
 * the `if ("wakeup".equals(mi.name)) continue;` check in
 * ConsumerNotThreadSafeRule#lambdaTouchesConsumerNonWakeup. A
 * lambda that ONLY calls wakeup on the consumer (no other consumer
 * method, no other consumer-owner method) is NOT added to the
 * consumerTouchingLambdas set, so the dispatch-site check abstains
 * for it.
 *
 * This is critical: shutdown hooks, signal handlers, and admin
 * threads MUST be able to interrupt a blocked poll() — and the
 * wakeup() call is precisely how kafka-clients exposes that.
 * Flagging this shape would force users to suppress the rule
 * everywhere they implement clean shutdown.
 */
public class GoodWakeupOnlyOffload {

    private final Consumer<String, String> consumer;
    private final ExecutorService scheduler = Executors.newSingleThreadExecutor();

    public GoodWakeupOnlyOffload(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    /**
     * Shape A: shutdown-hook style — schedule a wakeup from another
     * thread after a timeout, so the poll loop can break out
     * cleanly.
     *
     * Bytecode of the lambda body:
     *   ALOAD this
     *   GETFIELD this.consumer
     *   INVOKEINTERFACE Consumer.wakeup()V
     *   RETURN
     * The rule's scan sees the Consumer.wakeup INVOKE and short-
     * circuits (`if ("wakeup".equals(mi.name)) continue;`). It
     * finds NO other consumer INVOKE in this lambda, so the lambda
     * is NOT consumer-touching by the rule's definition. The
     * dispatch INVOKEDYNAMIC → ExecutorService.submit is therefore
     * not flagged.
     */
    public void scheduleShutdownWakeup() {
        // SILENT: lambda body invokes only consumer.wakeup() — the
        // single thread-safe consumer method.
        scheduler.submit(() -> consumer.wakeup());
    }

    /**
     * Shape B: poll loop with shutdown handling via wakeup. The
     * poll() runs on the calling thread (no lambda, no dispatch).
     * The wakeup() is invoked from a separate thread inside another
     * lambda — that lambda's body is `consumer.wakeup()` only, so
     * the rule abstains.
     */
    public void pollLoopWithWakeupShutdown() {
        // Schedule wakeup from a different thread — silent per the
        // wakeup-only exclusion.
        scheduler.submit(() -> consumer.wakeup());

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                System.out.println("polled " + records.count() + " records");
                consumer.commitSync();
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            // Clean exit triggered by the scheduled wakeup.
            consumer.close();
        }
    }
}
