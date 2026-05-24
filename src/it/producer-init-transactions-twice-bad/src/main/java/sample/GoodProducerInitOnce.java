package sample;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Control for PRODUCER_INIT_TRANSACTIONS_TWICE.
 *
 * <p>Five methods that each AVOID the rule by calling
 * {@code initTransactions()} EXACTLY ONCE per producer slot in the method.
 *
 * <ol>
 *   <li>{@code initOnce} — single init, no other transactional methods. The
 *       canonical minimum shape.</li>
 *   <li>{@code initThenFullTransactionalCycle} — single init followed by
 *       begin / send / commit. The canonical EOS-v2 producer shape.</li>
 *   <li>{@code initThenAbortCycle} — single init followed by begin / send /
 *       abort. The error-recovery shape.</li>
 *   <li>{@code twoProducersEachInitedOnce} — two SEPARATE producer slots,
 *       each with its own init. The rule is per-slot, so two inits across
 *       two slots is silent.</li>
 *   <li>{@code initCapturedOnceForExecutor} — single
 *       {@code producer::initTransactions} method-reference capture (no
 *       prior direct init). The deferred capture fires init once on the
 *       executor's thread, which is legitimate.</li>
 * </ol>
 */
public final class GoodProducerInitOnce {

    private static final String BROKERS = "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092";

    private static Properties txnProducerProps(String txnId) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "good-init-once-" + txnId);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return p;
    }

    /** Correct: single init, no other transactional methods. */
    public void initOnce() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-init-once"));
        try {
            producer.initTransactions();
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: canonical EOS-v2 init + begin + send + commit. */
    public void initThenFullTransactionalCycle(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-init-full-cycle"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            producer.commitTransaction();
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: init + begin + send + abort (error-recovery shape). */
    public void initThenAbortCycle(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-init-abort-cycle"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            producer.abortTransaction();
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Correct: TWO producer slots, each independently constructed and inited
     * exactly once. The rule is per-slot — two inits across two slots is
     * silent because each slot's first init is legitimate.
     */
    public void twoProducersEachInitedOnce() {
        KafkaProducer<String, String> producerA = new KafkaProducer<>(txnProducerProps("good-init-slot-a"));
        KafkaProducer<String, String> producerB = new KafkaProducer<>(txnProducerProps("good-init-slot-b"));
        try {
            producerA.initTransactions();
            producerB.initTransactions();
        } finally {
            producerA.close(Duration.ofSeconds(10));
            producerB.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Correct: single capture of {@code producer::initTransactions} with no
     * prior direct init. The executor runs the captured Runnable exactly
     * once; on the worker thread the producer is UNINITIALIZED at call time
     * and the init succeeds.
     */
    public void initCapturedOnceForExecutor(Executor executor) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-init-capture-once"));
        try {
            executor.execute(producer::initTransactions);
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }
}
