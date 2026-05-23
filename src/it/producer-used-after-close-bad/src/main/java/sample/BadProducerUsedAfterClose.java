package sample;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * RULE: PRODUCER_USED_AFTER_CLOSE.
 *
 * <p>Fires when a lifecycle method (send / flush / beginTransaction /
 * commitTransaction / abortTransaction / initTransactions /
 * sendOffsetsToTransaction / partitionsFor / metrics) is invoked on a
 * local-slot {@code KafkaProducer} AFTER {@code close()} has been
 * called on the same slot earlier in the same method.
 *
 * <p>Why use-after-close on a producer is a serious bug — what
 * actually happens at runtime:
 * <ol>
 *   <li>{@code KafkaProducer.close()} is a ONE-WAY transition. Once
 *       called, an internal {@code AtomicBoolean closed} is set and
 *       every subsequent call into a public API checks it first.
 *       The closed state is sticky — there is no "reopen()". This
 *       is intentional: re-entering an open state after the Sender
 *       thread has stopped would race with whatever called close().</li>
 *   <li>{@code send(...)} after close() throws
 *       {@code IllegalStateException("Cannot perform operation
 *       after producer has been closed")} synchronously from the
 *       producer's caller thread. Same for {@code flush()},
 *       {@code beginTransaction()}, the transactional commit/abort
 *       APIs, and {@code initTransactions()}.</li>
 *   <li>The synchronous IllegalStateException would be acceptable
 *       if it propagated to a place that catches and surfaces it.
 *       In practice the producer is often used via an async path:
 *       a callback, an executor task, a {@code @Async} method, a
 *       {@code CompletableFuture.thenApply}. In those flows, the
 *       exception is swallowed by the framework (or logged at WARN
 *       and forgotten) and the record is SILENTLY DROPPED with no
 *       visible signal to the calling code.</li>
 *   <li>For transactional producers, calling
 *       {@code commitTransaction()} after close() is doubly bad:
 *       the close() already aborts any in-flight transaction. So
 *       the records you THINK you committed were actually aborted
 *       — and the commitTransaction() call throws on top. Downstream
 *       {@code read_committed} consumers see no records; the
 *       application logs an exception it didn't expect; the
 *       database / Kafka write split-brain.</li>
 *   <li>The typical real-world shape: a finally-block that closes
 *       too eagerly, then a return path that still calls
 *       {@code flush()} or a callback that fires after the
 *       finally block. The author thinks "close after flush" but
 *       writes the code as "close THEN flush" by mistake. The bug
 *       does not show up in unit tests because the local
 *       IllegalStateException is loud — but it shows up in
 *       production where the call sits behind an executor.</li>
 * </ol>
 *
 * <p>What the rule catches (per-METHOD scan, local-slot tracking):
 * <ol>
 *   <li>Track every {@code new KafkaProducer(...) + ASTORE N} —
 *       record slot N.</li>
 *   <li>For every method call whose receiver resolves to a tracked
 *       slot (via {@code AsmUtil.resolveReceiverSlot} —
 *       backwards stack simulation that handles nested arg
 *       expressions like {@code send(new ProducerRecord(...))}):
 *       <ul>
 *         <li>If the method name starts with {@code "close"}, add
 *             the slot to {@code closedSlots}.</li>
 *         <li>Otherwise, if the name is in the lifecycle set AND
 *             the slot is already in {@code closedSlots}, fire.</li>
 *       </ul></li>
 *   <li>Fire site is the lifecycle call (NOT the close call) —
 *       the operator wants to know where the offending use happens.</li>
 * </ol>
 *
 * <p>This Bad class triggers FOUR fires — four lifecycle operations
 * each performed after close() on the same slot:
 * <ol>
 *   <li>{@code closeThenSend}: close() then send() — the canonical
 *       finally-too-early shape.</li>
 *   <li>{@code closeThenFlush}: close() then flush().</li>
 *   <li>{@code closeThenBeginTransaction}: close() then
 *       beginTransaction() — transactional producer misuse.</li>
 *   <li>{@code closeThenSendWithNestedArg}: close() then
 *       send(new ProducerRecord(...)) — the nested arg expression
 *       exercises the backward stack-effect simulation; without it
 *       the rule would miss this case because the receiver is not
 *       the instruction immediately before the call.</li>
 * </ol>
 */
public final class BadProducerUsedAfterClose {

    /** Anti-pattern: close() then send(). IllegalStateException at runtime — but often swallowed in async paths. */
    public void closeThenSend(ProducerRecord<String, String> record) {
        Properties props = baseProducerProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.close();
        producer.send(record); // reported
    }

    /** Anti-pattern: close() then flush(). Same outcome. */
    public void closeThenFlush() {
        Properties props = baseProducerProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.close();
        producer.flush(); // reported
    }

    /** Anti-pattern: close() then beginTransaction(). Transactional misuse — the producer is dead. */
    public void closeThenBeginTransaction() {
        Properties props = transactionalProducerProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();
        producer.close();
        producer.beginTransaction(); // reported
    }

    /** Anti-pattern: close() then send(new ProducerRecord(...)). Nested arg expression — exercises receiver-resolution. */
    public void closeThenSendWithNestedArg() {
        Properties props = baseProducerProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.close();
        producer.send(new ProducerRecord<>("orders", "key", "value")); // reported
    }

    private static Properties baseProducerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("client.id", "bad-producer-used-after-close");
        p.put("compression.type", "lz4");
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return p;
    }

    private static Properties transactionalProducerProps() {
        Properties p = baseProducerProps();
        p.put("transactional.id", "bad-producer-used-after-close-txn");
        return p;
    }
}
