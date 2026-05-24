package sample;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;

/**
 * RULE: PRODUCER_CLOSE_IN_CALLBACK.
 *
 * <p>Fires when {@code producer.close(...)} appears inside the body of
 * a {@link Callback#onCompletion(RecordMetadata, Exception)} method.
 * Closing the producer from inside its own callback is a guaranteed
 * deadlock (kafka-clients &lt; 0.11) or a silent
 * zero-timeout forced shutdown that discards in-flight records
 * (kafka-clients &gt;= 0.11). Either way, it is wrong.
 *
 * <p>Why this is a real production problem (mechanism):
 * <ol>
 *   <li>A {@code KafkaProducer} is two threads in one object: the
 *       <i>user thread</i> that owns the producer reference and calls
 *       {@code send(...)}, and the internal <i>sender thread</i>
 *       (named {@code kafka-producer-network-thread | <client-id>})
 *       that owns the accumulator and the socket.</li>
 *   <li>Callbacks registered via
 *       {@code send(record, callback)} execute on the sender thread,
 *       immediately after the broker ACK is processed for the
 *       corresponding record. The {@code Callback} Javadoc states
 *       this explicitly: <i>"This callback will generally execute in
 *       the I/O thread of the producer"</i>.</li>
 *   <li>{@code KafkaProducer.close()} (no-arg, or with a non-zero
 *       {@code Duration}) calls {@code Sender.initiateClose()} and
 *       then waits on {@code ioThread.join()} for the sender thread
 *       to terminate.</li>
 *   <li>If the calling thread IS the sender thread (because we're
 *       inside a callback), then {@code close()} is asking the
 *       sender thread to wait for itself to terminate. Pre-0.11:
 *       deadlocks forever. Post-0.11: kafka-clients detects the
 *       self-join via {@code Thread.currentThread() == ioThread}
 *       and silently substitutes {@code Duration.ZERO}, forcing an
 *       immediate shutdown — every in-flight {@code ProducerBatch}
 *       not yet ACKed is discarded with
 *       {@code KafkaException("Producer is closed forcefully")}, and
 *       the {@code Callback} is fired once for each lost batch with
 *       that exception (potentially re-entering this same buggy
 *       callback path with another {@code close()}).</li>
 * </ol>
 *
 * <p>What this looks like in production (pre-0.11):
 * <ol>
 *   <li>A developer wires "close on error" inside the callback
 *       because the callback is where the exception is observed:
 *       {@code if (exception != null) producer.close();}.</li>
 *   <li>Local tests pass — the close path is only exercised when the
 *       callback actually runs with an exception, which depends on
 *       broker NACKs that few test harnesses simulate.</li>
 *   <li>Production: the first send that fails fires the callback,
 *       the callback calls {@code close()}, the sender thread parks
 *       inside its own {@code join()}, every subsequent
 *       {@code send()} blocks on the accumulator, the
 *       request-handler pool drains, and the application becomes
 *       unresponsive. Thread dump signature is unmistakable: one
 *       sender thread parked in {@code Object.wait} inside
 *       {@code close()}, all worker threads parked in
 *       {@code accumulator.append()}.</li>
 * </ol>
 *
 * <p>What the rule catches:
 * <ul>
 *   <li><b>Named class implementing {@link Callback}.</b> The rule
 *       walks the class's
 *       {@code onCompletion(RecordMetadata, Exception)V} method body
 *       and reports any {@code close} call on a KafkaProducer /
 *       Producer — regardless of descriptor (no-arg,
 *       {@code Duration}, or the deprecated {@code long, TimeUnit}
 *       form).</li>
 *   <li><b>Lambda Callback.</b> When the producer is invoked as
 *       {@code producer.send(record, (m, e) -> { ... producer.close(); ... })},
 *       javac compiles the lambda body into a synthetic method on
 *       the enclosing class, paired with an {@code INVOKEDYNAMIC}
 *       that links it as a {@code Callback}. The rule walks
 *       INVOKEDYNAMIC nodes, looks for ones whose SAM-return is
 *       {@code Lorg/apache/kafka/clients/producer/Callback;}, resolves
 *       the implementation handle, and checks the synthetic method
 *       body for {@code close} calls.</li>
 * </ul>
 *
 * <p>This is NOT the same as
 * {@link io.conductor.kafkalinter.RuleId#PRODUCER_FLUSH_IN_CALLBACK}
 * — that rule fires for the {@code flush()} self-deadlock, which has
 * the same mechanism but a different blocked-on call. Both rules are
 * deliberately separate so the violation message points at the
 * correct call site.
 */
public final class BadProducerCloseInCallback {

    /** Anti-pattern #1: named-class Callback with no-arg close() in onCompletion — FIRES. */
    public static final class DeadlockNoArgCloseCallback implements Callback {
        private final KafkaProducer<String, String> producer;

        public DeadlockNoArgCloseCallback(KafkaProducer<String, String> producer) {
            this.producer = producer;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                producer.close(); // FIRES — sender thread waits to join itself
            }
        }
    }

    /** Anti-pattern #2: named-class Callback with Duration-arg close in onCompletion — FIRES. */
    public static final class DeadlockDurationCloseCallback implements Callback {
        private final KafkaProducer<String, String> producer;

        public DeadlockDurationCloseCallback(KafkaProducer<String, String> producer) {
            this.producer = producer;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                producer.close(Duration.ofSeconds(30)); // FIRES — same self-join
            }
        }
    }

    /** Anti-pattern #3: lambda Callback with close() in body — FIRES. */
    public void sendWithLambdaClose(KafkaProducer<String, String> producer,
                                    ProducerRecord<String, String> record) {
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                producer.close(); // FIRES — same self-join
            }
        });
    }

    /** Anti-pattern #4: lambda Callback with Duration close in body — FIRES. */
    public void sendWithLambdaCloseDuration(KafkaProducer<String, String> producer,
                                            ProducerRecord<String, String> record) {
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                producer.close(Duration.ofSeconds(30)); // FIRES — same self-join
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

    /** Control: caller closes on the user thread (not the callback) — must NOT fire. */
    public void closeOnUserThread(KafkaProducer<String, String> producer,
                                  ProducerRecord<String, String> record) {
        producer.send(record);
        producer.close(Duration.ofSeconds(30)); // safe — user thread, not sender thread
    }
}
