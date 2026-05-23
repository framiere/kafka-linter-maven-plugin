package sample;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * RULE: PRODUCER_SEND_BLOCKING_GET.
 *
 * <p>Fires when {@code producer.send(...)} is followed IMMEDIATELY by
 * {@code .get()} on the returned {@link Future}, i.e. the chained
 * shape {@code producer.send(record).get()}. This silently converts
 * the asynchronous, batched producer API into a synchronous,
 * one-record-at-a-time producer and is one of the most common
 * "I thought I was using Kafka the fast way" mistakes.
 *
 * <p>Why this is bad — what {@code .get()} actually does:
 * <ol>
 *   <li>{@code KafkaProducer.send(record)} returns a
 *       {@code Future<RecordMetadata>}. The Future is completed by
 *       the Sender I/O thread once the broker acknowledges the
 *       record (or, with {@code acks=all}, once the in-sync replicas
 *       acknowledge it).</li>
 *   <li>Calling {@code .get()} on that Future BLOCKS the calling
 *       thread until completion. Until the broker acks, the caller
 *       sits in {@code Object.wait}.</li>
 *   <li>So {@code producer.send(record).get()} guarantees: send the
 *       record, wait for the broker round-trip, then continue. No
 *       overlap, no batching. The next record can't even enter the
 *       accumulator until this one is fully acknowledged.</li>
 * </ol>
 *
 * <p>The throughput cost is enormous:
 * <ul>
 *   <li>An async producer at default settings can hit ~50k–200k
 *       records/sec from a single thread, because hundreds of
 *       records share each ProduceRequest.</li>
 *   <li>With {@code .get()} after every send, the same thread is
 *       capped at {@code 1 / broker_rtt} — typically 50–200
 *       records/sec. That's a 250×–4000× regression.</li>
 *   <li>Worse: the broker now serves 250× more ProduceRequests for
 *       the same payload (one record per request instead of a full
 *       batch). The whole cluster pays for one app's mistake.</li>
 * </ul>
 *
 * <p>What it looks like in production:
 * <ol>
 *   <li>Developer wants "send-and-confirm" semantics: "I want to know
 *       this record made it to Kafka before I respond to the HTTP
 *       caller." They write {@code producer.send(record).get()}.</li>
 *   <li>Local benchmarks against a co-located broker look fine (RTT
 *       is sub-millisecond, throughput is acceptable).</li>
 *   <li>Production hits a real cross-AZ or cross-region broker with
 *       2–5 ms RTT. Throughput collapses to 200–500 req/s per
 *       thread. The team adds more threads, which works at first,
 *       then runs into broker-side request-handler saturation.</li>
 *   <li>Root cause is invisible in code review (it just looks like
 *       "send, wait, continue") — the rule catches it at compile
 *       time before it ever reaches prod.</li>
 * </ol>
 *
 * <p>What the rule catches: every {@code INVOKEVIRTUAL} /
 * {@code INVOKEINTERFACE} of {@code send} on a Producer owner whose
 * NEXT significant instruction is {@code INVOKE} of
 * {@code java/util/concurrent/Future#get}. The "next significant"
 * skips over non-semantic instructions (labels, frames, line
 * numbers, type-cast for the generic Future) but does NOT skip a
 * store/load round-trip — so storing the Future in a variable and
 * then calling {@code .get()} on it later is OUT OF SCOPE for this
 * rule (it's a different anti-pattern with different ergonomics —
 * the developer at least named the deferred action).
 *
 * <p>Correct pattern — observable failure mode without a synchronous
 * block:
 * <pre>{@code
 *   producer.send(record, (metadata, exception) -> {
 *       if (exception != null) {
 *           log.error("send failed", exception);
 *       }
 *   });
 * }</pre>
 *
 * <p>For genuinely-needed end-of-batch flush semantics, call
 * {@code producer.flush()} once after the batch — not {@code .get()}
 * per record.
 */
public final class BadProducerSendBlockingGet {

    /** Anti-pattern: send(record).get() chained on KafkaProducer concrete — FIRES. */
    public void chainedGetConcrete(KafkaProducer<String, String> producer,
                                   ProducerRecord<String, String> record)
            throws InterruptedException, ExecutionException {
        producer.send(record).get(); // FIRES — synchronous per-record send
    }

    /** Anti-pattern: send(record).get() chained on Producer interface — FIRES. */
    public void chainedGetInterface(Producer<String, String> producer,
                                    ProducerRecord<String, String> record)
            throws InterruptedException, ExecutionException {
        producer.send(record).get(); // FIRES — same shape via INVOKEINTERFACE
    }

    /** Anti-pattern: send(record, callback).get() — FIRES (rule matches both send overloads). */
    public void chainedGetWithCallback(KafkaProducer<String, String> producer,
                                       ProducerRecord<String, String> record)
            throws InterruptedException, ExecutionException {
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("send failed: " + exception.getMessage());
            }
        }).get(); // FIRES — callback doesn't matter, .get() still blocks
    }

    /** Control: real Callback, NO .get() chained — must NOT fire. */
    public void sendWithCallbackOnly(KafkaProducer<String, String> producer,
                                     ProducerRecord<String, String> record) {
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("send failed: " + exception.getMessage());
            }
        });
    }

    /** Control: Future stored in a variable, .get() called separately — OUT OF SCOPE. */
    public RecordMetadata sendThenGetSeparately(KafkaProducer<String, String> producer,
                                                ProducerRecord<String, String> record)
            throws InterruptedException, ExecutionException {
        Future<RecordMetadata> pending = producer.send(record);
        return pending.get(); // by design out of scope — not the chained shape the rule catches
    }
}
