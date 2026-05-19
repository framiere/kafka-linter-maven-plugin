package sample;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Each method here is a deliberate anti-pattern. The linter must flag every one of
 * the 9 rule IDs at least once across the file.
 */
public final class BadKafkaUsage {

    // RULE 1a: PRODUCER_IN_LOOP — classic for-loop instantiation.
    public void producerInForLoop() {
        Properties props = props();
        for (int i = 0; i < 10; i++) {
            KafkaProducer<String, String> p = new KafkaProducer<>(props);
            p.send(new ProducerRecord<>("t", "v"), (md, ex) -> {});
            p.close();
        }
    }

    // RULE 1b: PRODUCER_IN_LOOP via iterating lambda — list.forEach(... new KafkaProducer ...).
    public void producerInForEachLambda(List<String> topics) {
        Properties props = props();
        topics.forEach(t -> {
            KafkaProducer<String, String> p = new KafkaProducer<>(props);
            p.send(new ProducerRecord<>(t, "v"), (md, ex) -> {});
            p.close();
        });
    }

    // RULE 2: CONSUMER_IN_LOOP.
    public void consumerInForLoop() {
        Properties props = consumerProps();
        for (int i = 0; i < 3; i++) {
            KafkaConsumer<String, String> c = new KafkaConsumer<>(props);
            c.close();
        }
    }

    // RULE 3: PRODUCER_NO_COMPRESSION — method builds a producer but never mentions compression.type.
    public void producerWithoutCompression() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.send(new ProducerRecord<>("t", "v"), (md, ex) -> {});
        producer.close();
    }

    // RULE 4: PRODUCER_SEND_BLOCKING_GET.
    public void producerSendBlockingGet(KafkaProducer<String, String> p) throws Exception {
        p.send(new ProducerRecord<>("t", "v")).get();
    }

    // RULE 5: PRODUCER_SEND_NO_CALLBACK — fire-and-forget, the Future is discarded.
    public void producerSendNoCallback(KafkaProducer<String, String> p) {
        p.send(new ProducerRecord<>("t", "v"));
    }

    // RULE 6: PRODUCER_FLUSH_IN_LOOP.
    public void producerFlushInLoop(KafkaProducer<String, String> p) {
        for (int i = 0; i < 10; i++) {
            p.flush();
        }
    }

    // RULE 7: CONSUMER_AUTO_COMMIT_TRUE.
    public void consumerAutoCommitTrue() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("enable.auto.commit", "true");
        KafkaConsumer<String, String> c = new KafkaConsumer<>(props);
        c.close();
    }

    // RULE 8: CONSUMER_COMMIT_PER_RECORD — commitSync inside the per-record loop.
    public void consumerCommitPerRecord(KafkaConsumer<String, String> c) {
        while (running()) {
            ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : records) {
                handle(r);
                c.commitSync();
            }
        }
    }

    // RULE 9: CONSUMER_POLL_ZERO.
    public void consumerPollZero(KafkaConsumer<String, String> c) {
        c.poll(Duration.ZERO);
    }

    private Properties props() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("compression.type", "snappy");
        return p;
    }

    private Properties consumerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        return p;
    }

    private boolean running() { return true; }
    private void handle(ConsumerRecord<String, String> r) {}
}
