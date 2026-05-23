package sample;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Control for CONSUMER_NOT_CLOSED.
 *
 * <p>Four methods, each constructs a {@code KafkaConsumer} into a
 * local slot AND avoids firing the rule by satisfying one of the
 * four exits the rule supports — 1:1 with {@code GoodProducerClosed}.
 *
 * <ol>
 *   <li>{@code tryWithResources} — javac synthesizes a finally block
 *       around the try-body. Inside, javac emits {@code ALOAD N;
 *       INVOKEVIRTUAL KafkaConsumer.close()V}. Slot CLOSED. Silent.
 *       This is the recommended pattern when the consumer's life is
 *       bounded by a single method (a batch job, a one-shot
 *       offset-reader, a tooling script).</li>
 *   <li>{@code explicitFinallyClose} — manual try/finally with
 *       {@code consumer.close()} in the finally block. Bytecode is
 *       identical to case 1. This pattern is what you'd write in a
 *       long-running consumer loop with a wakeup-driven exit, where
 *       the {@code WakeupException} propagates out of poll() and the
 *       finally block guarantees the LeaveGroupRequest goes out.</li>
 *   <li>{@code returnsTheConsumer} — slot is USED (subscribe + poll)
 *       and then {@code ALOAD N; ARETURN}. The rule's escape detector
 *       sees the load-then-return pair → slot ESCAPED → silent. The
 *       caller now owns close().</li>
 *   <li>{@code stashesIntoField} — escape via {@code PUTFIELD}.
 *       Containing object now owns the lifecycle (usually via
 *       {@code @PreDestroy} or an {@code AutoCloseable.close()}).
 *       Rule does not chase cross-method ownership.</li>
 * </ol>
 *
 * <p>The two ESCAPED-path methods (cases 3 + 4) accept a deliberate
 * false negative: a caller can still forget to close. The rule's
 * design choice is "don't fire on escape" because chasing ownership
 * across method boundaries would require full data-flow analysis and
 * would inevitably produce false positives on Spring-managed
 * @Bean methods (where the framework owns the lifecycle via
 * @PreDestroy callbacks). False positives are worse than the false
 * negatives, so the rule errs toward silence on escape.
 */
public final class GoodConsumerClosed {

    /** Stashed-into-field consumer for case 4. */
    private KafkaConsumer<String, String> ownedConsumer;

    /** Try-with-resources: javac emits synthetic close() in generated finally. Slot CLOSED. */
    public void tryWithResources() {
        Properties props = baseConsumerProps();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("orders"));
            consumer.poll(Duration.ofMillis(100));
        } // synthetic close() — slot CLOSED, LeaveGroupRequest sent, silent
    }

    /** Manual try/finally with explicit close() — bytecode-equivalent to case 1. */
    public void explicitFinallyClose() {
        Properties props = baseConsumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        try {
            consumer.subscribe(Collections.singletonList("orders"));
            consumer.poll(Duration.ofMillis(100));
        } finally {
            consumer.close(); // slot CLOSED, silent
        }
    }

    /** Escape via ARETURN: caller takes ownership. */
    public KafkaConsumer<String, String> returnsTheConsumer() {
        Properties props = baseConsumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("orders"));
        return consumer; // ALOAD N + ARETURN — slot ESCAPED, silent
    }

    /** Escape via PUTFIELD: containing object now owns the consumer's lifecycle. */
    public void stashesIntoField() {
        Properties props = baseConsumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("orders"));
        this.ownedConsumer = consumer; // ALOAD this; ALOAD N; PUTFIELD — slot ESCAPED, silent
    }

    private static Properties baseConsumerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("client.id", "good-consumer-closed");
        p.put("group.id", "orders-processor");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return p;
    }
}
