package sample;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * RULE: PRODUCER_IN_LOOP.
 *
 * <p>Fires when {@code new KafkaProducer(...)} appears inside any
 * iteration context — a classic {@code for}/{@code while} loop, OR
 * an iterating lambda body ({@code list.forEach(...)},
 * {@code stream.map(...)}, etc.). Constructing a KafkaProducer once
 * per iteration is one of the highest-magnitude anti-patterns the
 * linter detects: it routinely turns a 50k req/s producer into a
 * 50 req/s producer (a 1000× regression) and simultaneously
 * hammers the broker with one metadata-fetch + TCP handshake per
 * loop iteration.
 *
 * <p>Why KafkaProducer construction is expensive:
 * <ol>
 *   <li><b>TCP handshake + bootstrap metadata.</b> The constructor
 *       opens at least one TCP connection to a bootstrap broker,
 *       performs the SASL/SSL handshake if configured, and sends an
 *       initial MetadataRequest to learn the cluster topology. In
 *       a healthy cluster this is ~50–200 ms; in a cross-region or
 *       TLS-heavy setup it can exceed 1 s.</li>
 *   <li><b>Record-accumulator buffer allocation.</b> By default the
 *       producer pre-allocates {@code buffer.memory} (32 MB) of
 *       off-heap-ish memory for batching. Doing this in a loop
 *       allocates and frees 32 MB per iteration — heap pressure,
 *       GC churn, and (worst case) OutOfMemoryError under bursty
 *       traffic.</li>
 *   <li><b>Sender I/O thread.</b> A new daemon thread named
 *       "kafka-producer-network-thread | ..." is spawned on every
 *       construction. A loop that runs a few thousand iterations
 *       can leak thousands of threads if the producer is not
 *       closed promptly (PRODUCER_NOT_CLOSED is a separate rule
 *       that catches that side of the issue).</li>
 *   <li><b>Zero batching benefit.</b> A producer constructed for a
 *       single record will send that one record on its own
 *       ProduceRequest, regardless of how many records the loop
 *       processes. The whole point of the per-partition accumulator
 *       is to amortize the network cost across many records — that
 *       only works if the producer outlives the loop.</li>
 * </ol>
 *
 * <p>What this looks like in production:
 * <ol>
 *   <li>An ETL job processes a daily batch — say, 5 million records.
 *       The author writes {@code for (Record r : batch) { try (var p
 *       = new KafkaProducer<>(props)) { p.send(...); } }} thinking
 *       try-with-resources makes it tidy.</li>
 *   <li>Result: 5 million TCP handshakes, 5 million MetadataRequests
 *       to the broker, 5 million threads created and destroyed. The
 *       broker's IO threads saturate purely on connection setup.
 *       Other apps on the same cluster see ProduceRequest latency
 *       spike by 10×–100× as a side effect.</li>
 *   <li>Discovery: broker shows abnormally high connection-rate
 *       metrics, app-side shows ~50 records/sec throughput (one
 *       record per producer × ~20 ms/setup). Thread dumps show
 *       hundreds of "kafka-producer-network-thread" entries —
 *       instant signature.</li>
 * </ol>
 *
 * <p>What the rule catches: every {@code NEW org/apache/kafka/clients/producer/KafkaProducer}
 * instruction whose containing instruction is either inside a
 * back-edge (classic loop, detected by {@code LoopFinder}) or inside
 * a method body classified as an iterating lambda (detected by
 * {@code LambdaTracker} — {@code forEach}, stream operations, etc.).
 *
 * <p>Correct pattern: instantiate ONCE outside any loop, share the
 * producer for the lifetime of the application (or at minimum for
 * the entire iteration), and close it deterministically at shutdown:
 * <pre>{@code
 *   try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
 *       for (Record r : batch) {
 *           producer.send(toProducerRecord(r));
 *       }
 *   } // closed once, at the end
 * }</pre>
 */
public final class BadProducerInLoop {

    private static final String BROKERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092";

    private static Properties props() {
        Properties p = new Properties();
        p.put("bootstrap.servers", BROKERS);
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return p;
    }

    /** Anti-pattern: new KafkaProducer inside a classic for-loop — FIRES. */
    public void newProducerInForLoop(List<ProducerRecord<String, String>> records) {
        for (ProducerRecord<String, String> r : records) {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props())) { // FIRES
                producer.send(r);
            }
        }
    }

    /** Anti-pattern: new KafkaProducer inside a while-loop — FIRES. */
    public void newProducerInWhileLoop(int iterations, ProducerRecord<String, String> record) {
        int i = 0;
        while (i < iterations) {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props())) { // FIRES
                producer.send(record);
            }
            i++;
        }
    }

    /** Anti-pattern: new KafkaProducer inside a forEach iterating lambda — FIRES. */
    public void newProducerInForEachLambda(List<ProducerRecord<String, String>> records) {
        records.forEach(r -> {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props())) { // FIRES
                producer.send(r);
            }
        });
    }

    /** Control: producer instantiated ONCE outside the loop, reused — must NOT fire. */
    public void producerInstantiatedOutsideLoop(List<ProducerRecord<String, String>> records) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props())) {
            for (ProducerRecord<String, String> r : records) {
                producer.send(r);
            }
        }
    }
}
