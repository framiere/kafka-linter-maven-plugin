package sample;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Control for PRODUCER_SEND_OUTSIDE_TRANSACTION.
 *
 * <p>Five methods that each AVOID the rule by either (a) not being
 * transactional at all (no initTransactions call → the rule is silent), or
 * (b) properly bracketing every send between beginTransaction() and a
 * matching commitTransaction()/abortTransaction() on the same slot in the
 * same execution path.
 *
 * <ol>
 *   <li>{@code nonTransactionalProducer} — vanilla / idempotent producer with
 *       NO initTransactions() call. send() outside any transaction is the
 *       legitimate shape (this is the dominant non-EOS producer pattern).</li>
 *   <li>{@code initBeginSendCommit} — canonical EOS-v2 happy path: init,
 *       begin, send, commit. The send is bracketed correctly.</li>
 *   <li>{@code initBeginSendAbort} — error-recovery shape: init, begin, send,
 *       abort. The send is bracketed correctly even though the transaction
 *       is aborted rather than committed.</li>
 *   <li>{@code twoProducersEachBracketingSend} — two separate transactional
 *       producer slots, each correctly bracketing its own send. The rule is
 *       per-slot, so each producer's bracket is independent.</li>
 *   <li>{@code multipleSendsInOneTransaction} — three sends inside a single
 *       begin/commit bracket. The rule does not fire on 'multiple sends'; it
 *       only fires on 'send outside an open transaction'.</li>
 * </ol>
 */
public final class GoodProducerSendInsideTransaction {

    private static final String BROKERS = "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092";

    private static Properties txnProducerProps(String txnId) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "good-send-inside-txn-" + txnId);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return p;
    }

    private static Properties vanillaProducerProps() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "good-send-vanilla");
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return p;
    }

    /**
     * Correct: vanilla / idempotent producer with NO initTransactions() call.
     * The slot is not transactional from the rule's perspective; send()
     * outside any transaction is the legitimate shape. This is the dominant
     * non-EOS producer pattern and the rule must be silent on it.
     */
    public void nonTransactionalProducer(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(vanillaProducerProps());
        try {
            producer.send(record);
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: canonical EOS-v2 happy path. */
    public void initBeginSendCommit(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-init-begin-send-commit"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(record);
            producer.commitTransaction();
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: error-recovery shape — bracketed but aborted. */
    public void initBeginSendAbort(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-init-begin-send-abort"));
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
     * Correct: two separate transactional producer slots, each correctly
     * bracketing its own send. The rule is per-slot, so each producer's
     * bracket is independent.
     */
    public void twoProducersEachBracketingSend(ProducerRecord<String, String> recordA, ProducerRecord<String, String> recordB) {
        KafkaProducer<String, String> producerA = new KafkaProducer<>(txnProducerProps("good-two-producers-a"));
        KafkaProducer<String, String> producerB = new KafkaProducer<>(txnProducerProps("good-two-producers-b"));
        try {
            producerA.initTransactions();
            producerB.initTransactions();
            producerA.beginTransaction();
            producerA.send(recordA);
            producerA.commitTransaction();
            producerB.beginTransaction();
            producerB.send(recordB);
            producerB.commitTransaction();
        } finally {
            producerA.close(Duration.ofSeconds(10));
            producerB.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Correct: three sends inside a single begin/commit bracket. The rule
     * does not fire on 'multiple sends'; it only fires on 'send outside an
     * open transaction'. Per-batch transactional sends with N records is the
     * canonical EOS-v2 throughput pattern.
     */
    public void multipleSendsInOneTransaction(ProducerRecord<String, String> r1, ProducerRecord<String, String> r2, ProducerRecord<String, String> r3) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(txnProducerProps("good-multiple-sends"));
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(r1);
            producer.send(r2);
            producer.send(r3);
            producer.commitTransaction();
        } finally {
            producer.close(Duration.ofSeconds(10));
        }
    }
}
