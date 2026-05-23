package sample;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * RULE: PRODUCER_NOT_CLOSED.
 *
 * <p>Fires when a method constructs a {@code KafkaProducer} into a
 * local variable, uses it (any non-close method call on the same
 * slot), and never calls {@code close()} on that slot, AND does
 * not let the producer escape (no return, no field write).
 *
 * <p>Why missing close() is a real bug — what the Sender thread
 * actually does:
 * <ol>
 *   <li>{@code send()} does NOT actually send. It appends the
 *       record to an in-process {@code RecordAccumulator} (one
 *       deque of batches per topic-partition) and returns a
 *       {@code Future}. The Future will complete when the broker
 *       has acked the batch.</li>
 *   <li>A separate background thread, the SENDER, polls the
 *       accumulator, picks ready batches, and writes them to
 *       broker connections. The sender runs on a daemon thread,
 *       so it does NOT keep the JVM alive.</li>
 *   <li>{@code close()} is the ONLY public API that flushes:
 *       it drains the accumulator, waits for in-flight batches
 *       to ack, and then stops the sender. Without close(), JVM
 *       shutdown kills the daemon thread mid-batch and every
 *       record still in the accumulator is silently dropped.</li>
 *   <li>{@code flush()} drains but does NOT stop the sender, so
 *       it doesn't reach the "all in-flight acked" state on its
 *       own. Useful in transactional code, but not a substitute
 *       for close() on shutdown.</li>
 *   <li>For a TRANSACTIONAL producer the picture is worse: the
 *       producer-id is held until the broker side decides the
 *       producer is dead, which only happens after
 *       {@code transaction.timeout.ms} (default 60 s). Every
 *       {@code read_committed} consumer downstream stalls on
 *       the still-open transaction for that full window.</li>
 * </ol>
 *
 * <p>How JVM shutdown actually plays out without close():
 * <ol>
 *   <li>Application code returns / main exits.</li>
 *   <li>JVM enters shutdown. Non-daemon threads (the application
 *       threads that built the producer) start unwinding.</li>
 *   <li>JVM signals daemon threads to stop. The Sender thread
 *       receives the signal in the middle of a network read
 *       (or worse, in the middle of constructing a Produce
 *       request) and dies.</li>
 *   <li>Records that are 100% local — never even sent to the
 *       broker — are gone. The send() callback never runs;
 *       the Future never completes. Downstream code that was
 *       waiting on .get() sees no exception, just process
 *       death.</li>
 * </ol>
 *
 * <p>What the rule catches (per-method, local-slot data flow):
 * <ol>
 *   <li>Track every {@code new KafkaProducer(...)} that ASTOREs
 *       into a local slot. Record the slot number.</li>
 *   <li>For every method call whose receiver resolves (via
 *       {@code AsmUtil.resolveReceiverSlot}) to a tracked slot:
 *       <ul>
 *         <li>If the method name starts with {@code "close"},
 *             mark the slot CLOSED.</li>
 *         <li>Otherwise (send / flush / send-transaction /
 *             initTransactions / etc.), mark the slot USED.</li>
 *       </ul></li>
 *   <li>Detect escape: any {@code PUTFIELD} / {@code PUTSTATIC}
 *       / {@code ARETURN} immediately preceded by an
 *       {@code ALOAD N} that points to a tracked slot marks the
 *       slot ESCAPED. The rule does not fire on escaped slots —
 *       the assumption is that another method (often a
 *       {@code @PreDestroy} / a shutdown hook / a caller) will
 *       close it.</li>
 *   <li>Try-with-resources is handled transparently — javac
 *       emits a synthetic {@code close()} call in the generated
 *       finally region, which step 2 picks up as a normal close.</li>
 *   <li>Fire only when slot was USED, was NOT CLOSED, and did
 *       NOT ESCAPE. The fire site is the constructor line.</li>
 * </ol>
 *
 * <p>This Bad class triggers TWO fires — two methods that construct
 * + use + leak a producer in different ways:
 * <ol>
 *   <li>{@code sendAndForget}: send() then return without closing.</li>
 *   <li>{@code flushButDontClose}: flush() then return — flush is
 *       not close, the rule treats it as a USE not a close.</li>
 * </ol>
 */
public final class BadProducerNotClosed {

    /** Anti-pattern: construct, send, return. Accumulator drained only by JVM shutdown — records dropped. */
    public void sendAndForget(ProducerRecord<String, String> record) {
        Properties props = baseProducerProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props); // reported
        producer.send(record);
        // no close() — Sender thread dies mid-batch on shutdown
    }

    /** Anti-pattern: flush() is NOT close(). The slot is USED, not CLOSED, still leaks. */
    public void flushButDontClose(ProducerRecord<String, String> record) {
        Properties props = baseProducerProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props); // reported
        producer.send(record);
        producer.flush();
        // flush drains the accumulator NOW, but the Sender thread keeps running and there is no
        // promise the next batch will get drained on shutdown. close() is the only API that does both.
    }

    private static Properties baseProducerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("client.id", "bad-producer-not-closed");
        p.put("compression.type", "lz4");
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return p;
    }
}
