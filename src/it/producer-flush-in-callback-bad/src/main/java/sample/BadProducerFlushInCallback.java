package sample;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * RULE: PRODUCER_FLUSH_IN_CALLBACK.
 *
 * <p>Fires when {@code producer.flush()} appears inside the body of a
 * {@link Callback#onCompletion(RecordMetadata, Exception)} method —
 * which on Kafka's producer means a GUARANTEED DEADLOCK with no
 * timeout.
 *
 * <p>The mechanism:
 * <ol>
 *   <li>The KafkaProducer maintains a single "Sender" I/O thread that
 *       drains the accumulator, performs ProduceRequests, receives
 *       responses, and invokes the user-supplied callbacks.</li>
 *   <li>{@code Callback.onCompletion} runs synchronously on that
 *       Sender thread, between draining one batch and starting the
 *       next.</li>
 *   <li>{@code producer.flush()} BLOCKS the calling thread until the
 *       accumulator is empty — i.e. until the Sender thread finishes
 *       all outstanding work.</li>
 *   <li>If the calling thread IS the Sender thread (because we're
 *       inside a callback), then flush() is asking the Sender thread
 *       to wait for itself to finish. It never does. The producer
 *       hangs forever, the application's request-handler pool drains,
 *       and the only mitigation is a JVM restart.</li>
 * </ol>
 *
 * <p>What this looks like in production:
 * <ol>
 *   <li>A developer adds {@code producer.flush()} inside a callback
 *       "to make sure this record is committed before continuing" —
 *       not realizing the callback IS the Sender thread.</li>
 *   <li>Local testing passes (the deadlock only triggers when the
 *       callback actually runs, which depends on broker acks).</li>
 *   <li>Production hits the deadlock on the first send that completes.
 *       The Sender thread hangs forever; subsequent send() calls
 *       block on the accumulator; the application's worker pool
 *       drains; ALL requests time out.</li>
 *   <li>Discovery: thread dump shows one Sender thread parked in
 *       Object.wait inside flush(), all worker threads parked in
 *       accumulator.append() — instant signature, root cause obvious
 *       once seen.</li>
 * </ol>
 *
 * <p>What the rule catches:
 * <ul>
 *   <li><b>Named class implementing {@link Callback}.</b> The rule
 *       walks the class's {@code onCompletion(RecordMetadata, Exception)V}
 *       method body and reports any {@code flush()} call on a
 *       KafkaProducer / Producer.</li>
 *   <li><b>Lambda Callback.</b> When the producer is invoked as
 *       {@code producer.send(record, (m, e) -> { ... producer.flush(); ... })},
 *       javac compiles the lambda body to a synthetic method on the
 *       enclosing class, paired with an {@code INVOKEDYNAMIC} that
 *       links it as a {@code Callback}. The rule walks
 *       INVOKEDYNAMIC nodes, looks for ones whose SAM-return is
 *       {@code Lorg/apache/kafka/clients/producer/Callback;}, resolves
 *       the implementation handle, and checks the synthetic method
 *       body for {@code flush()} calls.</li>
 * </ul>
 *
 * <p>Note: this is NOT the same as PRODUCER_FLUSH_IN_LOOP, which fires
 * for {@code flush()} inside any kind of loop. PRODUCER_FLUSH_IN_CALLBACK
 * fires only inside callback bodies and corresponds to the deadlock
 * shape, not the throughput shape.
 */
public final class BadProducerFlushInCallback {

    /** Anti-pattern: named-class Callback with flush() in onCompletion — FIRES. */
    public static final class DeadlockCallback implements Callback {
        private final KafkaProducer<String, String> producer;

        public DeadlockCallback(KafkaProducer<String, String> producer) {
            this.producer = producer;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                producer.flush(); // FIRES — Sender thread waits for itself
            }
        }
    }

    /** Anti-pattern: lambda Callback with flush() in body — FIRES. */
    public void sendWithLambdaFlush(KafkaProducer<String, String> producer,
                                    ProducerRecord<String, String> record) {
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                producer.flush(); // FIRES — Sender thread waits for itself
            }
        });
    }

    /** Control: lambda Callback that only LOGS — must NOT fire. */
    public void sendWithLogOnlyLambda(KafkaProducer<String, String> producer,
                                      ProducerRecord<String, String> record) {
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("send failed: " + exception.getMessage());
            }
        });
    }

    /** Control: caller flushes on the user thread (not the callback) — must NOT fire. */
    public void flushOnUserThread(KafkaProducer<String, String> producer,
                                  ProducerRecord<String, String> record) {
        producer.send(record);
        producer.flush(); // safe — user thread, not Sender thread
    }
}
