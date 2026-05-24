package sample;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * RULE: PRODUCER_BEGIN_TRANSACTION_NO_ABORT.
 *
 * <p>Method-scoped rule. Fires when a method calls
 * {@code Producer.beginTransaction()} but does not call
 * {@code Producer.abortTransaction()} anywhere in the same method.
 *
 * <p>Why method scope (and not project scope):
 * <ol>
 *   <li>The canonical EOS skeleton co-locates begin and abort in
 *       the same try/catch — they belong to the same logical
 *       transaction boundary. The catch handler that fires the
 *       abort lives in the same method that opened the
 *       transaction.</li>
 *   <li>A project-scoped check would silently suppress the rule
 *       whenever a different helper somewhere in the project happens
 *       to call {@code abortTransaction()} — a recovery utility used
 *       only by tests, an admin-only "clear stuck transaction"
 *       button, an unrelated EOS pipeline. The transaction that
 *       fails to abort here would not be rescued by that helper
 *       there.</li>
 *   <li>Method scope keeps the rule tied to the actual call site
 *       that owns the transaction — exactly the granularity at
 *       which the recovery path must exist.</li>
 * </ol>
 *
 * <p>Why a missing abort is a real production problem:
 * <ol>
 *   <li>{@code KafkaProducer.beginTransaction()} promotes the
 *       internal {@code TransactionManager} from {@code READY} to
 *       {@code IN_TRANSACTION}. After {@code beginTransaction()},
 *       only {@code commitTransaction()} and
 *       {@code abortTransaction()} can move it back to
 *       {@code READY}.</li>
 *   <li>If any {@code send()}, {@code commitTransaction()}, or any
 *       work between begin and commit throws, the transaction is
 *       left open. The producer's TransactionManager stays in
 *       {@code IN_TRANSACTION}.</li>
 *   <li>The next attempt to start a transaction
 *       ({@code beginTransaction()} again) throws
 *       {@code IllegalStateException: Invalid transition attempted
 *       from state IN_TRANSACTION to state IN_TRANSACTION}. The
 *       producer is unusable for further transactional writes until
 *       it is closed and reconstructed.</li>
 *   <li>On the broker side, every read_committed consumer of every
 *       partition the in-flight transaction wrote to is gated by
 *       the Last Stable Offset (LSO) until
 *       {@code transaction.timeout.ms} elapses. Default: 60 seconds
 *       for plain producers, 10 minutes for Streams. During that
 *       window, downstream reads stall partition-wide — even for
 *       records produced by unrelated transactions that already
 *       committed past the stuck one.</li>
 * </ol>
 *
 * <p>Why this slips through code review:
 * <ol>
 *   <li>The happy path — {@code begin → send → commit} — looks
 *       complete. The bug is what is NOT there: the catch handler.
 *       A reviewer skimming the method sees three lifecycle calls
 *       in the right order and moves on.</li>
 *   <li>Unit tests typically exercise the happy path. The catch
 *       handler is only exercised when {@code send()} or
 *       {@code commit()} actually throws — which requires a broker
 *       failure injection most test suites don't have.</li>
 *   <li>Production failures from this bug are intermittent (only
 *       on broker hiccups) and the symptom is "downstream
 *       read_committed consumers stalled for 60s" — which looks
 *       like a broker problem, not a producer-code problem. The
 *       link back to the missing abort is rarely obvious.</li>
 * </ol>
 *
 * <p>What the rule checks (method-scope, single-pass):
 * <ol>
 *   <li>For each {@code MethodNode}, walk its instructions and
 *       collect every line number where the bytecode contains an
 *       INVOKEVIRTUAL/INVOKEINTERFACE whose owner is in
 *       {@code PRODUCER_OWNERS}, name is {@code beginTransaction},
 *       descriptor is {@code ()V}.</li>
 *   <li>In the same pass, set a method-local {@code hasAbort} flag
 *       to true on any same-shape {@code abortTransaction()V}
 *       call OR on any INVOKEDYNAMIC whose bootstrap-method args
 *       contain a {@code REF_invokeVirtual} /
 *       {@code REF_invokeInterface} handle pointing at
 *       {@code Producer.abortTransaction:()V} (the method-reference
 *       shape: {@code producer::abortTransaction} compiled as
 *       INVOKEDYNAMIC).</li>
 *   <li>If begin-sites are non-empty and {@code hasAbort} is
 *       false, emit one violation per begin-site.</li>
 * </ol>
 *
 * <p>This class exercises two distinct method-scope shapes that
 * both must fire:
 * <ol>
 *   <li>{@link #processBatchHappyPathOnly} — straight-line
 *       begin/send/commit with no try/catch. The most common
 *       junior-developer shape.</li>
 *   <li>{@link #processBatchInsufficientCatch} — try/catch that
 *       does log+throw but never calls abort. Looks "defensive"
 *       to reviewers but leaves the transaction stuck.</li>
 * </ol>
 *
 * <p>The {@code setup} method calls
 * {@code initTransactions()} so the sibling project-scoped rule
 * {@code PRODUCER_INIT_TRANSACTIONS_NOT_CALLED} stays silent —
 * the fixture is designed to exercise this rule, not that one.
 */
public final class BadProducerBeginTransactionNoAbort {

    /** Lifecycle: called once at construction; silences PRODUCER_INIT_TRANSACTIONS_NOT_CALLED. */
    public void setup(Producer<String, String> producer) {
        producer.initTransactions();
    }

    /**
     * Anti-pattern #1: straight-line begin/send/commit with no
     * try/catch at all. A broker NACK or serialization failure on
     * {@code send()} (or on the commit itself) propagates out of
     * the method with the transaction still in IN_TRANSACTION.
     */
    public void processBatchHappyPathOnly(Producer<String, String> producer,
                                           ProducerRecord<String, String> record) {
        producer.beginTransaction(); // reported
        producer.send(record);
        producer.commitTransaction();
    }

    /**
     * Anti-pattern #2: try/catch is present, but the catch path
     * logs and rethrows without calling {@code abortTransaction()}.
     * This shape often appears in code that "wanted to add error
     * handling" but missed the EOS protocol requirement that the
     * producer-side transaction state must be explicitly aborted.
     */
    public void processBatchInsufficientCatch(Producer<String, String> producer,
                                              ProducerRecord<String, String> record) {
        producer.beginTransaction(); // reported
        try {
            producer.send(record);
            producer.commitTransaction();
        } catch (RuntimeException e) {
            // logs only — no abortTransaction()
            throw e;
        }
    }
}
