package sample;

import java.util.List;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * RULE: PRODUCER_FLUSH_IN_LOOP.
 *
 * <p>Fires when {@code producer.flush()} is called from inside any kind
 * of iteration context — a classic {@code for}/{@code while} loop, OR
 * an iterating lambda body (e.g. {@code list.forEach(r -> ...)},
 * {@code stream.map(...)}, etc.). This is the THROUGHPUT shape of the
 * flush() anti-pattern, distinct from PRODUCER_FLUSH_IN_CALLBACK which
 * is the DEADLOCK shape.
 *
 * <p>Why flush()-in-a-loop is wrong:
 * <ol>
 *   <li>The KafkaProducer is designed around BATCHING. Records sit in
 *       the per-partition accumulator until either (a) the batch fills
 *       to {@code batch.size}, or (b) {@code linger.ms} elapses, or
 *       (c) someone calls {@code flush()}. Then the Sender thread
 *       drains the accumulator in one shot — one network round-trip
 *       carries hundreds or thousands of records.</li>
 *   <li>{@code flush()} forces (c) immediately. The accumulator is
 *       emptied right now, regardless of how full any batch was.</li>
 *   <li>If you call {@code flush()} after every {@code send()}, you
 *       have effectively turned a high-throughput async producer into
 *       a synchronous per-record producer. Each record now pays a full
 *       broker round-trip (typically 5–50ms in healthy clusters) on
 *       the critical path.</li>
 *   <li>For a producer that would otherwise hit ~50k req/s with
 *       batching, this drops throughput to ~20–200 req/s — a
 *       250×–2500× regression. The producer is no longer the
 *       bottleneck; the network is. And the broker now sees 250×
 *       more ProduceRequests for the same payload, multiplying its
 *       request-handler queue depth.</li>
 * </ol>
 *
 * <p>What this looks like in production:
 * <ol>
 *   <li>Developer "translates" a synchronous API (e.g. an HTTP-to-Kafka
 *       bridge that processes a batch payload) into Kafka. They iterate
 *       the input list, call {@code producer.send(record)} inside the
 *       loop, then — "to be safe" — call {@code producer.flush()} on
 *       every iteration to make sure nothing is lost.</li>
 *   <li>Local benchmarks against a single-node broker look "fine":
 *       request rates are low, network RTT is sub-millisecond, the
 *       cost is invisible.</li>
 *   <li>Production traffic ramps. The producer's throughput plateaus
 *       at a fraction of expected. Investigations point at "Kafka is
 *       slow" — but the broker shows low CPU. Eventually someone
 *       inspects the producer's send-rate metric and sees one record
 *       per ProduceRequest. Root cause: flush() in the loop.</li>
 * </ol>
 *
 * <p>What the rule catches: it walks every method body and reports any
 * {@code flush()} call on a {@code KafkaProducer} / {@code Producer}
 * whose containing instruction is either (a) inside a back-edge
 * (classic loop), or (b) inside a method body that this class
 * recognizes as an iterating-lambda body — synthesized by
 * {@code forEach}/stream-style APIs that invoke the lambda many times.
 *
 * <p>Correct pattern: call {@code flush()} ONCE after the loop (or just
 * before {@code close()}), letting the accumulator batch normally during
 * the loop:
 * <pre>{@code
 *   for (Record r : records) {
 *       producer.send(toProducerRecord(r));
 *   }
 *   producer.flush();   // once, OUTSIDE the loop
 * }</pre>
 *
 * <p>Note: this rule fires for ANY loop shape. PRODUCER_FLUSH_IN_CALLBACK
 * fires specifically for {@code flush()} inside a {@code Callback.onCompletion}
 * body — that is a deadlock, not a throughput issue, and is reported by a
 * separate rule with a separate fix.
 */
public final class BadProducerFlushInLoop {

    /** Anti-pattern: flush() inside a classic for-loop — FIRES. */
    public void flushInForLoop(KafkaProducer<String, String> producer,
                               List<ProducerRecord<String, String>> records) {
        for (ProducerRecord<String, String> r : records) {
            producer.send(r);
            producer.flush(); // FIRES — forces a network RTT per record
        }
    }

    /** Anti-pattern: flush() inside a while-loop — FIRES. */
    public void flushInWhileLoop(KafkaProducer<String, String> producer,
                                 ProducerRecord<String, String> record,
                                 int count) {
        int i = 0;
        while (i < count) {
            producer.send(record);
            producer.flush(); // FIRES — same problem with a different loop shape
            i++;
        }
    }

    /** Anti-pattern: flush() inside a forEach iterating lambda — FIRES. */
    public void flushInForEachLambda(KafkaProducer<String, String> producer,
                                     List<ProducerRecord<String, String>> records) {
        records.forEach(r -> {
            producer.send(r);
            producer.flush(); // FIRES — lambda body is an iterating-lambda context
        });
    }

    /** Control: flush() ONCE after the loop — must NOT fire. */
    public void flushAfterLoop(KafkaProducer<String, String> producer,
                               List<ProducerRecord<String, String>> records) {
        for (ProducerRecord<String, String> r : records) {
            producer.send(r);
        }
        producer.flush(); // safe — outside the loop, batches normally during iteration
    }

    /** Control: loop with NO flush inside — must NOT fire. */
    public void loopWithoutFlush(KafkaProducer<String, String> producer,
                                 List<ProducerRecord<String, String>> records) {
        for (ProducerRecord<String, String> r : records) {
            producer.send(r);
        }
    }
}
