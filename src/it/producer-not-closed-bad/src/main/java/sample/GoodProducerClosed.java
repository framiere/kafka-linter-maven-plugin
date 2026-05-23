package sample;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Control for PRODUCER_NOT_CLOSED.
 *
 * <p>Four methods, each constructs a {@code KafkaProducer} into a local
 * slot AND avoids firing the rule by satisfying one of the four exits
 * the rule supports. They map 1:1 to the four branches of the slot
 * state machine described in {@link BadProducerNotClosed}.
 *
 * <ol>
 *   <li>{@code tryWithResources} — javac synthesizes a finally block
 *       around the try-body. Inside that finally, javac emits an
 *       {@code ALOAD N} (where N is the tracked slot) followed by
 *       {@code INVOKEVIRTUAL KafkaProducer.close()V}. The rule sees a
 *       call whose receiver resolves to slot N AND whose method name
 *       starts with {@code "close"} → slot marked CLOSED. Silent.</li>
 *   <li>{@code explicitFinallyClose} — manually written
 *       try/finally. Bytecode is essentially identical to the
 *       try-with-resources case (javac is just doing the work the
 *       programmer wrote here). Silent for the same reason.</li>
 *   <li>{@code returnsTheProducer} — slot is USED (send call) and
 *       then loaded onto the stack and ARETURNed. The rule's escape
 *       detector sees {@code ALOAD N} immediately followed by
 *       {@code ARETURN} → slot marked ESCAPED. Escaped slots are not
 *       reported — the assumption is the caller takes ownership and
 *       will close (or escalate the ownership transfer further). The
 *       rule deliberately accepts this false negative: tracking
 *       cross-method ownership requires whole-program analysis, which
 *       is out of scope for a per-method byte-code lint.</li>
 *   <li>{@code stashesIntoField} — same logic as case 3 but the
 *       escape is via {@code PUTFIELD}: {@code ALOAD this; ALOAD N;
 *       PUTFIELD owner}. The rule's escape detector recognizes the
 *       {@code ALOAD N} immediately preceding the PUTFIELD as an
 *       escape. The producer's lifecycle is now owned by the
 *       containing object — typically closed in a {@code @PreDestroy},
 *       a {@code AutoCloseable.close()}, or a shutdown hook. The rule
 *       does not chase that ownership.</li>
 * </ol>
 *
 * <p>Together these four methods make the rule's three exits visible
 * in one fixture: CLOSED (cases 1 + 2), ESCAPED-via-return (case 3),
 * ESCAPED-via-field (case 4). With PRODUCER_NOT_CLOSED severity ERROR
 * and these four methods only, the build is silent.
 */
public final class GoodProducerClosed {

    /** Stashed-into-field producer for case 4. */
    private KafkaProducer<String, String> ownedProducer;

    /** Try-with-resources: javac emits a synthetic close() in the generated finally. */
    public void tryWithResources(ProducerRecord<String, String> record) {
        Properties props = baseProducerProps();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(record);
            producer.flush();
        } // synthetic close() here — slot CLOSED, silent
    }

    /** Manual try/finally with explicit close() — bytecode-equivalent to try-with-resources. */
    public void explicitFinallyClose(ProducerRecord<String, String> record) {
        Properties props = baseProducerProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        try {
            producer.send(record);
            producer.flush();
        } finally {
            producer.close(); // slot CLOSED, silent
        }
    }

    /** Escape via ARETURN: caller takes ownership. Rule does not chase cross-method ownership. */
    public KafkaProducer<String, String> returnsTheProducer(ProducerRecord<String, String> record) {
        Properties props = baseProducerProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.send(record);
        return producer; // ALOAD N + ARETURN — slot ESCAPED, silent
    }

    /** Escape via PUTFIELD: containing object now owns the producer's lifecycle. */
    public void stashesIntoField(ProducerRecord<String, String> record) {
        Properties props = baseProducerProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.send(record);
        this.ownedProducer = producer; // ALOAD this; ALOAD N; PUTFIELD — slot ESCAPED, silent
    }

    private static Properties baseProducerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("client.id", "good-producer-closed");
        p.put("compression.type", "lz4");
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return p;
    }
}
