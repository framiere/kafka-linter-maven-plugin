package sample;

import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

/**
 * Control for CONSUMER_NO_WAKEUP_SHUTDOWN — wakeup() variant.
 *
 * <p>A class-scope rule treats {@code "any method in the class calls
 * Consumer.wakeup()"} as proof of intent: even if the wakeup method
 * is named anything (including {@code shutdown()}), the rule is
 * satisfied. The presence of the wakeup() call is what matters.
 *
 * <p>Production-grade pattern:
 * <ol>
 *   <li>Polling thread runs the loop. Each iteration calls
 *       poll(Duration) and processes records. The loop is wrapped
 *       in try { ... } catch (WakeupException) { ... } finally
 *       { close() }.</li>
 *   <li>A separate thread (a Runtime shutdown hook, a Spring
 *       {@code @PreDestroy}, an HTTP shutdown endpoint, the
 *       orchestrator's signal handler) calls
 *       {@code consumer.wakeup()}. wakeup() is the ONLY method
 *       on KafkaConsumer that is documented as safe to call from
 *       another thread.</li>
 *   <li>wakeup() sets an internal flag; the consumer's network
 *       selector returns immediately; the in-flight poll() throws
 *       WakeupException. The catch block exits the loop, the
 *       finally block calls close(), close() sends LeaveGroupRequest,
 *       and the coordinator triggers a rebalance in milliseconds.</li>
 * </ol>
 */
public final class GoodConsumerWithWakeup {

    private KafkaConsumer<String, String> consumer;

    /** Polling loop with the canonical try/catch(WakeupException)/finally shape. */
    public void runForever(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
        consumer.subscribe(Collections.singletonList("orders"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(30));
                records.forEach(r -> {
                    // process record (omitted)
                });
            }
        } catch (WakeupException expected) {
            // expected when shutdown() is called from another thread
        } finally {
            consumer.close();
        }
    }

    /** Shutdown method called by the orchestrator. Uses wakeup() — class-scope rule is satisfied. */
    public void shutdown() {
        if (consumer != null) {
            consumer.wakeup(); // <- this single call silences the rule for the entire class
        }
    }
}
