package sample;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * RULE: PRODUCER_SEND_IN_CALLBACK.
 *
 * <p>Fires when {@code producer.send(...)} appears inside the body of a
 * {@link Callback#onCompletion(RecordMetadata, Exception)} method. Unlike
 * {@code flush()} and {@code close()} — which Kafka 2.8+ now blocks
 * explicitly with a {@code KafkaException} (KAFKA-10852) — there is no
 * runtime guard for {@code send()} from a callback. The anti-pattern
 * therefore ships green out of dev, ships green out of staging, and only
 * surfaces under production load when the accumulator finally fills.
 *
 * <p>The mechanism:
 * <ol>
 *   <li>The KafkaProducer maintains exactly one Sender I/O thread. That
 *       thread drains the {@code RecordAccumulator}, issues
 *       {@code ProduceRequest}s, receives responses, and dispatches user
 *       callbacks synchronously between batches.</li>
 *   <li>{@code KafkaProducer.send(record, callback)} is non-blocking
 *       only as long as the accumulator has buffer capacity. When it is
 *       full, {@code send()} blocks the calling thread inside
 *       {@code RecordAccumulator.append} on the BufferPool semaphore
 *       for up to {@code max.block.ms} (default 60 s), then throws
 *       {@code TimeoutException} and silently drops the record.</li>
 *   <li>If the calling thread IS the Sender thread (because we're
 *       inside a callback), then send() is asking the Sender thread to
 *       wait for itself to free buffer space. It never does. The thread
 *       parks for {@code max.block.ms} per call, the application sheds
 *       throughput, and every record produced after the accumulator
 *       fills is dropped.</li>
 * </ol>
 *
 * <p>What this looks like in production:
 * <ol>
 *   <li>A developer writes a publish-on-ack fan-out: "when this record
 *       is acked, send the derived record." The intuitive shape is
 *       {@code producer.send(rec, (m, e) -> producer.send(derived))}.
 *       At low volume (dev, staging, integration tests) the accumulator
 *       never fills, so the lambda completes in microseconds and looks
 *       fine.</li>
 *   <li>In production, traffic ramps until the accumulator pressure
 *       crosses {@code buffer.memory}. The Sender thread is now its own
 *       producer; every callback-issued send blocks the Sender thread
 *       on the BufferPool semaphore.</li>
 *   <li>Throughput collapses — p99 quadruples — and downstream records
 *       silently time out. Thread dump shows the single Sender thread
 *       parked in {@code BufferPool.allocate} called from
 *       {@code RecordAccumulator.append} called from the user's
 *       {@code onCompletion}. Instant signature.</li>
 *   <li>Transactional producers get an additional failure mode: a
 *       re-entrant send from inside a callback violates the
 *       {@code TransactionManager} state machine (KIP-98) and can
 *       fence the producer entirely.</li>
 * </ol>
 *
 * <p>What the rule catches:
 * <ul>
 *   <li><b>Named class implementing {@link Callback}.</b> The rule
 *       walks the class's {@code onCompletion(RecordMetadata, Exception)V}
 *       method body and reports any {@code send()} call on a
 *       KafkaProducer / Producer.</li>
 *   <li><b>Lambda Callback.</b> When the producer is invoked as
 *       {@code producer.send(record, (m, e) -> { ... producer.send(...); ... })},
 *       javac compiles the lambda body to a synthetic method on the
 *       enclosing class, paired with an {@code INVOKEDYNAMIC} that
 *       links it as a {@code Callback}. The rule walks
 *       INVOKEDYNAMIC nodes, looks for ones whose SAM-return is
 *       {@code Lorg/apache/kafka/clients/producer/Callback;}, resolves
 *       the implementation handle, and checks the synthetic method
 *       body for {@code send()} calls.</li>
 * </ul>
 *
 * <p>Note: this is the third member of the in-callback trilogy alongside
 * PRODUCER_FLUSH_IN_CALLBACK and PRODUCER_CLOSE_IN_CALLBACK. All three
 * share the same root cause (Sender-thread reentrancy on its own
 * blocking primitives), but {@code send()} is the most insidious of the
 * three because it has NO explicit runtime guard.
 */
public final class BadProducerSendInCallback {

    /** Anti-pattern #1: named-class Callback with send() in onCompletion — FIRES. */
    public static final class FanOutCallback implements Callback {
        private final KafkaProducer<String, String> producer;
        private final ProducerRecord<String, String> derived;

        public FanOutCallback(KafkaProducer<String, String> producer,
                              ProducerRecord<String, String> derived) {
            this.producer = producer;
            this.derived = derived;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null) {
                producer.send(derived); // FIRES — Sender thread waits on its own BufferPool
            }
        }
    }

    /** Anti-pattern #2: lambda Callback with send(record) in body — FIRES. */
    public void publishOnAckLambda(KafkaProducer<String, String> producer,
                                   ProducerRecord<String, String> record,
                                   ProducerRecord<String, String> derived) {
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                producer.send(derived); // FIRES — Sender thread waits on its own BufferPool
            }
        });
    }

    /** Anti-pattern #3: lambda Callback with two-arg send(record, anotherCallback) — FIRES. */
    public void publishOnAckLambdaNestedCallback(KafkaProducer<String, String> producer,
                                                 ProducerRecord<String, String> record,
                                                 ProducerRecord<String, String> derived) {
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                producer.send(derived, (m2, e2) -> { // FIRES — outer callback body sends
                    /* no-op observer */
                });
            }
        });
    }

    /** Anti-pattern #4: named-class Callback with TWO sends in onCompletion — FIRES TWICE. */
    public static final class DoubleFanOutCallback implements Callback {
        private final KafkaProducer<String, String> producer;
        private final ProducerRecord<String, String> a;
        private final ProducerRecord<String, String> b;

        public DoubleFanOutCallback(KafkaProducer<String, String> producer,
                                    ProducerRecord<String, String> a,
                                    ProducerRecord<String, String> b) {
            this.producer = producer;
            this.a = a;
            this.b = b;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null) {
                producer.send(a); // FIRES
                producer.send(b); // FIRES
            }
        }
    }
}
