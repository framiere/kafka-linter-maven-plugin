package sample;

import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

/**
 * Control for CONSUMER_NO_WAKEUP_SHUTDOWN — method-reference variant.
 *
 * <p>This fixture asserts that the class-scope wakeup-suppressor recognises
 * the canonical Kafka shutdown idiom even when the only mention of
 * {@code wakeup()} in the entire class is a <strong>method reference</strong>:
 *
 * <pre>{@code
 *   Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
 * }</pre>
 *
 * <p>This is the exact shape recommended in the Kafka documentation
 * ("Handling SIGTERM with KafkaConsumer") and in countless production
 * codebases. It is the cleanest way to wire the JVM's shutdown hook to
 * the only thread-safe method on {@code KafkaConsumer}: {@code wakeup()}.
 *
 * <h2>Why this is a tricky linting target</h2>
 *
 * javac compiles {@code consumer::wakeup} to an {@code INVOKEDYNAMIC}
 * instruction whose bootstrap-method arguments include a direct
 * {@code REF_invokeVirtual KafkaConsumer.wakeup:()V} handle. The deferred
 * call (which runs when the shutdown hook fires) materialises on the
 * shutdown thread when {@code Thread.run()} dispatches the captured
 * {@code Runnable}. <strong>The outer method's bytecode contains zero
 * {@code INVOKEVIRTUAL} instructions targeting {@code wakeup}.</strong>
 *
 * <p>The naive form of the rule's class-scope suppressor only walks
 * {@code MethodInsnNode} and would miss the callsite entirely — which
 * means a class whose ONLY wakeup is captured via {@code ::wakeup} would
 * (incorrectly) fail the rule. That is exactly the wrong direction: the
 * code shown here is the gold-standard Kafka shutdown pattern, and a
 * linter that flags it would push users toward the worse (direct-call
 * wakeup on a separate field) shape. The suppressor must additionally
 * inspect the indy bootstrap-method arg list and accept any
 * {@code Consumer.wakeup:()V} or {@code KafkaConsumer.wakeup:()V}
 * handle it finds — that recovery is what this fixture asserts.
 *
 * <p>The class contains a poll() in a while-loop (which would normally
 * fire the rule), the canonical try/catch(WakeupException)/finally
 * shape, and registers the shutdown hook in the constructor using only
 * the {@code consumer::wakeup} method reference. Expected fires: ZERO.
 */
public final class GoodConsumerMethodRefWakeup {

    private final KafkaConsumer<String, String> consumer;

    public GoodConsumerMethodRefWakeup(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
        // The ONLY mention of wakeup in this class — captured as a method reference.
        // Compiles to INVOKEDYNAMIC with a REF_invokeVirtual KafkaConsumer.wakeup:()V
        // bootstrap-method handle; no INVOKEVIRTUAL wakeup appears anywhere here.
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
    }

    /**
     * Polling loop with the canonical try/catch(WakeupException)/finally shape.
     * Would fire CONSUMER_NO_WAKEUP_SHUTDOWN if the suppressor missed the
     * method-ref capture above — so a violation here is the regression signal.
     */
    public void runForever() {
        consumer.subscribe(Collections.singletonList("orders"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(30));
                records.forEach(r -> {
                    // process record (omitted)
                });
            }
        } catch (WakeupException expected) {
            // expected when SIGTERM fires the shutdown hook, which runs consumer.wakeup()
        } finally {
            consumer.close(Duration.ofSeconds(20));
        }
    }
}
