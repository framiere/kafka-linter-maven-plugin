package sample;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * RULE: PRODUCER_SEND_NO_CALLBACK.
 *
 * <p>Fires when {@code producer.send(record)} is invoked WITHOUT a
 * Callback AND the returned {@link Future} is immediately discarded
 * (the bytecode shape is: {@code INVOKE send(ProducerRecord)} followed
 * by {@code POP}). This is the classic "fire-and-forget" producer
 * pattern — and it silently swallows every send failure.
 *
 * <p>Why this matters:
 * <ol>
 *   <li>{@code KafkaProducer.send(record)} returns a
 *       {@code Future<RecordMetadata>}. There are exactly two ways
 *       the producer surfaces a send failure: (a) the Future
 *       completes exceptionally (so {@code .get()} throws), or
 *       (b) the Callback's {@code onCompletion(metadata, exception)}
 *       is invoked with a non-null exception.</li>
 *   <li>The one-arg, no-callback overload paired with a discarded
 *       Future blocks BOTH error channels. Nothing in the user code
 *       ever observes the Future, and there is no Callback to
 *       observe the exception either.</li>
 *   <li>The failure modes are not theoretical:
 *       <ul>
 *         <li>{@code RecordTooLargeException} (payload exceeds
 *             {@code max.request.size}) — silently dropped.</li>
 *         <li>{@code NotEnoughReplicasException} (acks=all but
 *             min.insync.replicas not met) — silently dropped.</li>
 *         <li>{@code AuthenticationException} (ACL mid-flight
 *             revocation) — silently dropped.</li>
 *         <li>{@code SerializationException} (value serializer
 *             throws) — silently dropped.</li>
 *         <li>{@code TimeoutException} (delivery.timeout.ms elapsed
 *             before broker ack) — silently dropped.</li>
 *       </ul>
 *       The application thinks every record was sent. The broker
 *       never received them. Downstream consumers see a gap. Nobody
 *       knows where it came from.</li>
 * </ol>
 *
 * <p>What this looks like in production:
 * <ol>
 *   <li>Pipeline emits "audit events" via fire-and-forget {@code send(record)}.
 *       Local tests show every event arriving — the broker has no
 *       reason to reject them.</li>
 *   <li>Production cluster has {@code min.insync.replicas=2} and
 *       loses a replica. Producer with {@code acks=all} now starts
 *       failing sends with {@code NotEnoughReplicasException}. The
 *       app logs nothing because nothing observes the Future.</li>
 *   <li>Audit dashboard shows a 10-minute gap of "no events". Hours
 *       of incident triage trying to figure out which service is
 *       silent — only resolved when someone reads the producer
 *       source and notices the discarded Future.</li>
 * </ol>
 *
 * <p>What the rule catches: the rule is intentionally CONSERVATIVE.
 * It only fires when both conditions hold:
 * <ul>
 *   <li>The send overload taken is the one-arg form (last arg is
 *       NOT a Callback) — detected by inspecting the method descriptor.</li>
 *   <li>The next significant instruction after the send is {@code POP}
 *       (the JVM bytecode that discards the top stack value). This
 *       means the Future is not stored, not returned, not chained
 *       into {@code .get()} — it's thrown away on the spot.</li>
 * </ul>
 *
 * <p>If the Future is stored in a local, returned to the caller, or
 * chained via {@code .get()} / {@code .thenAccept()}, the rule does
 * NOT fire — those shapes either observe failures (Future.get throws)
 * or hand the Future to someone else who might observe it. The rule
 * is targeted specifically at "send then forget".
 *
 * <p>Correct pattern — observable failure mode with no synchronous block:
 * <pre>{@code
 *   producer.send(record, (metadata, exception) -> {
 *       if (exception != null) {
 *           log.error("send failed: {}", record.key(), exception);
 *           failedSendsCounter.inc();
 *       }
 *   });
 * }</pre>
 */
public final class BadProducerSendNoCallback {

    /** Anti-pattern: one-arg send(record) on KafkaProducer concrete, Future discarded — FIRES. */
    public void fireAndForgetConcrete(KafkaProducer<String, String> producer,
                                      ProducerRecord<String, String> record) {
        producer.send(record); // FIRES — POP on the discarded Future
    }

    /** Anti-pattern: one-arg send(record) on Producer interface, Future discarded — FIRES. */
    public void fireAndForgetInterface(Producer<String, String> producer,
                                       ProducerRecord<String, String> record) {
        producer.send(record); // FIRES — INVOKEINTERFACE, same shape
    }

    /** Control: two-arg send(record, callback) — must NOT fire (callback observes failures). */
    public void sendWithCallback(KafkaProducer<String, String> producer,
                                 ProducerRecord<String, String> record) {
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("send failed: " + exception.getMessage());
            }
        });
    }

    /** Control: callback referenced by variable, not literal lambda — must NOT fire. */
    public void sendWithCallbackVariable(KafkaProducer<String, String> producer,
                                         ProducerRecord<String, String> record) {
        Callback cb = (metadata, exception) -> { /* no-op */ };
        producer.send(record, cb);
    }

    /** Control: Future stored in a local var — must NOT fire (Future is observable by the caller). */
    public Future<RecordMetadata> sendAndReturnFuture(KafkaProducer<String, String> producer,
                                                     ProducerRecord<String, String> record) {
        Future<RecordMetadata> pending = producer.send(record);
        return pending;
    }

    /** Control: Future returned directly — must NOT fire (ARETURN, not POP). */
    public Future<RecordMetadata> sendAndReturnDirectly(Producer<String, String> producer,
                                                       ProducerRecord<String, String> record) {
        return producer.send(record);
    }

    /**
     * Control: Future stored, then observed via .get() — must NOT fire.
     *
     * <p>Note: this method is intentionally written with an ASTORE between
     * send() and get(); writing it as {@code producer.send(record).get()}
     * would correctly trigger PRODUCER_SEND_BLOCKING_GET (a sibling rule
     * targeting the chained-blocking shape). What this fixture tests is
     * PRODUCER_SEND_NO_CALLBACK — the Future-discarded shape — and this
     * method demonstrates that observing the Future suppresses it.
     */
    public RecordMetadata sendAndGetSeparately(KafkaProducer<String, String> producer,
                                               ProducerRecord<String, String> record)
            throws InterruptedException, ExecutionException {
        Future<RecordMetadata> pending = producer.send(record);
        return pending.get();
    }
}
