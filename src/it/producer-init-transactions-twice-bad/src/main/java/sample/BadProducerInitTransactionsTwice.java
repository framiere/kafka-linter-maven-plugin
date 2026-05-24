package sample;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * RULE: PRODUCER_INIT_TRANSACTIONS_TWICE.
 *
 * <p>Fires when {@code Producer.initTransactions()} is called more than once
 * on the same local KafkaProducer slot in the same method. The producer's
 * internal {@code TransactionManager} state machine only allows the
 * {@code UNINITIALIZED → INITIALIZING} transition; every subsequent call runs
 * {@code transitionTo(INITIALIZING)} against the now-READY (or IN_TRANSACTION /
 * COMMITTING_TRANSACTION / ABORTING_TRANSACTION / FATAL_ERROR) state, fails the
 * validity check, and throws
 * {@code KafkaException("TransactionalId <id>: Invalid transition attempted
 * from state READY to state INITIALIZING")} BEFORE any RPC.
 *
 * <p>The bug shape is structurally common: developers write 'idempotent init'
 * helpers (try/catch retry, lifecycle-hook + admin-endpoint duplication,
 * test-harness @BeforeAll + @BeforeEach overlap) without realizing the
 * runtime semantics — initTransactions() is single-shot, NOT idempotent. The
 * second call's exception is typically swallowed by the outer try/catch and
 * the operator sees only alarming-looking log lines on every restart while
 * the producer (which actually IS initialized after the first call) works
 * correctly.
 *
 * <p>What this rule catches (five direct-call and indy-capture shapes, each
 * firing exactly once):
 * <ol>
 *   <li>{@code initTwiceBackToBack} — two consecutive
 *       {@code initTransactions()} calls. Second call fires.</li>
 *   <li>{@code initInsideIdempotentRetryWrapper} — first init in try, second
 *       init in catch (the canonical 'idempotent init retry' bug). Second
 *       call fires.</li>
 *   <li>{@code initAfterFullTransactionalCycle} — init, begin, send, commit,
 *       then init again 'to refresh the handshake.' Second init fires; the
 *       state is READY, not UNINITIALIZED.</li>
 *   <li>{@code initThreeTimes} — three init calls in a row. Second and third
 *       fire (two violations).</li>
 *   <li>{@code initThenCaptureForExecutor} — synchronous
 *       {@code producer.initTransactions()} followed by
 *       {@code executor.execute(producer::initTransactions)}. The indy
 *       capture fires because the captured runnable will fail when the
 *       executor runs it on a producer that is already in READY.</li>
 * </ol>
 *
 * <p>Correct pattern: every transactional producer must call
 * {@code initTransactions()} EXACTLY ONCE in its construction path, before
 * any other transactional method. There is no API path to re-initialize a
 * producer; if you need a fresh handshake (e.g., after a fatal error), you
 * must {@code close()} the producer and construct a new one.
 */
public final class BadProducerInitTransactionsTwice {

    private static final String BROKERS = "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092";

    private static Properties txnProducerProps(String txnId) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "init-twice-" + txnId);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return p;
    }

    /**
     * Anti-pattern #1: two consecutive initTransactions() calls. Typical
     * origin: a developer copy-pasted an init line during a refactor and
     * left the duplicate. At runtime the first call succeeds; the second
     * call's transitionTo(INITIALIZING) check fails because state is READY.
     */
    public void initTwiceBackToBack() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-init-twice"));
        try {
            producer.initTransactions();
            producer.initTransactions(); // FIRES — state is READY, not UNINITIALIZED
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #2: the canonical 'idempotent init retry' bug. The
     * developer wraps init in try/catch expecting transient
     * TimeoutExceptions on first init and 'retries' from the catch. The
     * first init succeeds; the catch never runs on the happy path — but
     * developers add a 'safety net' second init in the catch's fall-through
     * path, or in a separate 'reinitialize' helper invoked after the catch.
     * Either way, when the catch DOES fire (legitimate transient error),
     * the retry runs against a READY state and throws.
     */
    public void initInsideIdempotentRetryWrapper() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-init-retry"));
        try {
            try {
                producer.initTransactions();
            } catch (RuntimeException ignored) {
                // Misguided 'retry on transient init failure' — but the first
                // call actually returned normally (sync RPC), so 'retry' here
                // is dead code on the happy path. If the catch DOES run on a
                // real transient, the state may already be partially advanced;
                // re-invoking init still throws against the live state machine.
            }
            producer.initTransactions(); // FIRES — second init, state is READY
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #3: init followed by a full transactional cycle, followed
     * by ANOTHER init 'to refresh the handshake' (or 'in case the broker
     * forgot us'). At runtime the second init runs against a READY state
     * and throws — the first init's handshake is still valid for the
     * lifetime of the producer.
     */
    public void initAfterFullTransactionalCycle(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-init-after-cycle"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            producer.commitTransaction();
            producer.initTransactions(); // FIRES — state is READY after commit, not UNINITIALIZED
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #4: three init calls in a row — the 'belt and braces and
     * a third belt' anti-pattern. Often appears in defensive code that
     * 'really really wants to make sure init happened.' The second AND
     * third init calls fire (two violations from one method).
     */
    public void initThreeTimes() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-init-three"));
        try {
            producer.initTransactions();
            producer.initTransactions(); // FIRES — second init
            producer.initTransactions(); // FIRES — third init
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #5: synchronous init followed by a deferred capture of
     * {@code producer::initTransactions}. When the executor runs the
     * captured Runnable, the producer is already in READY and the call
     * throws KafkaException. The executor's default uncaught-exception
     * handler typically swallows the exception, the worker thread dies
     * silently, and the operator sees a shrinking worker pool over many
     * restarts.
     */
    public void initThenCaptureForExecutor(Executor executor) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("bad-init-capture"));
        try {
            producer.initTransactions();
            executor.execute(producer::initTransactions); // FIRES — capture sees slot already inited
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }
}
