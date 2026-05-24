package sample;

import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

/**
 * Control for COMMIT_ASYNC_NO_FINAL_SYNC — method-reference variant.
 *
 * <p>This fixture asserts that the {@code commitSync} suppressor in the
 * rule recognises the canonical Kafka shutdown idiom even when the only
 * mention of {@code commitSync} anywhere in the class is captured as a
 * method reference:
 *
 * <pre>{@code
 *   Runtime.getRuntime().addShutdownHook(new Thread(consumer::commitSync));
 * }</pre>
 *
 * <p>The combined idiom — {@code commitAsync()} on the hot path for
 * throughput, plus a one-shot {@code commitSync()} on shutdown for
 * durability — is the textbook KafkaConsumer pattern. When the final
 * sync is wired via a shutdown hook, the cleanest shape uses a method
 * reference, because the shutdown hook is "call this thing exactly once
 * with no args" — the exact contract of a no-arg method ref.
 *
 * <h2>Why this is a tricky linting target</h2>
 *
 * javac compiles {@code consumer::commitSync} to an
 * {@code INVOKEDYNAMIC} whose bootstrap-method arguments include a
 * direct {@code REF_invokeVirtual KafkaConsumer.commitSync:()V} handle.
 * The deferred call materialises only when the shutdown hook fires and
 * dispatches the captured {@code Runnable}. <strong>The outer method's
 * bytecode contains zero {@code INVOKEVIRTUAL} instructions targeting
 * {@code commitSync}.</strong>
 *
 * <p>A naive class-scope suppressor that walks only
 * {@code MethodInsnNode} would conclude {@code syncFound = false},
 * see the unmatched {@code commitAsync()} in the poll loop, and fire
 * a false positive against this — the textbook-correct — shape. That
 * is the wrong direction: the linter would push the user toward a
 * worse pattern (a synchronous {@code commitSync()} sprinkled inside
 * the hot loop, which blocks the consumer thread on every commit).
 *
 * <p>The fix: extend the class-scope scan to also inspect every
 * {@code INVOKEDYNAMIC}'s bsm-arg list for any {@code Handle} whose
 * owner is in {@code KafkaTypes.CONSUMER_OWNERS} and whose name is
 * {@code commitSync} (or {@code commitAsync}). This fixture's
 * expected violations: ZERO.
 */
public final class GoodCommitAsyncMethodRefSync {

    private final KafkaConsumer<String, String> consumer;

    public GoodCommitAsyncMethodRefSync(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
        // The ONLY mention of commitSync in this class — captured as a method
        // reference. Compiles to INVOKEDYNAMIC with a REF_invokeVirtual
        // KafkaConsumer.commitSync:()V bootstrap handle. No INVOKEVIRTUAL
        // commitSync appears anywhere in the outer method's bytecode.
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::commitSync));
    }

    /**
     * Standard at-least-once poll loop: commitAsync after each batch for
     * throughput. The final sync — required for durability — is wired
     * via the shutdown hook above (method-ref capture). Would fire
     * COMMIT_ASYNC_NO_FINAL_SYNC if the rule's suppressor missed the
     * indy-captured commitSync.
     */
    public void runForever() {
        consumer.subscribe(Collections.singletonList("orders"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(30));
                records.forEach(r -> {
                    // process record (omitted)
                });
                consumer.commitAsync(); // throughput-optimised commit on the hot path
            }
        } catch (WakeupException expected) {
            // expected when SIGTERM fires the shutdown hook
        } finally {
            consumer.close(Duration.ofSeconds(20));
        }
    }
}
