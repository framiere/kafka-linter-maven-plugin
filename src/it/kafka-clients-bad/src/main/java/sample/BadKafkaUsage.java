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
 * Each method here is a deliberate anti-pattern. The linter must flag the kafka-clients
 * rule subset at least once across the file.
 */
public final class BadKafkaUsage {

    // RULE: PRODUCER_IN_LOOP — classic for-loop instantiation.
    public void producerInForLoop() {
        Properties props = props();
        for (int i = 0; i < 10; i++) {
            KafkaProducer<String, String> p = new KafkaProducer<>(props);
            p.send(new ProducerRecord<>("t", "v"), (md, ex) -> {});
            p.close();
        }
    }

    // RULE: PRODUCER_IN_LOOP via iterating lambda.
    public void producerInForEachLambda(List<String> topics) {
        Properties props = props();
        topics.forEach(t -> {
            KafkaProducer<String, String> p = new KafkaProducer<>(props);
            p.send(new ProducerRecord<>(t, "v"), (md, ex) -> {});
            p.close();
        });
    }

    // RULE: CONSUMER_IN_LOOP.
    public void consumerInForLoop() {
        Properties props = consumerProps();
        for (int i = 0; i < 3; i++) {
            KafkaConsumer<String, String> c = new KafkaConsumer<>(props);
            c.close();
        }
    }

    // RULE: PRODUCER_NO_COMPRESSION.
    public void producerWithoutCompression() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.send(new ProducerRecord<>("t", "v"), (md, ex) -> {});
        producer.close();
    }

    // RULE: PRODUCER_SEND_BLOCKING_GET.
    public void producerSendBlockingGet(KafkaProducer<String, String> p) throws Exception {
        p.send(new ProducerRecord<>("t", "v")).get();
    }

    // RULE: PRODUCER_SEND_NO_CALLBACK.
    public void producerSendNoCallback(KafkaProducer<String, String> p) {
        p.send(new ProducerRecord<>("t", "v"));
    }

    // RULE: PRODUCER_FLUSH_IN_LOOP.
    public void producerFlushInLoop(KafkaProducer<String, String> p) {
        for (int i = 0; i < 10; i++) {
            p.flush();
        }
    }

    // RULE: CONSUMER_AUTO_COMMIT_TRUE.
    public void consumerAutoCommitTrue() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("enable.auto.commit", "true");
        KafkaConsumer<String, String> c = new KafkaConsumer<>(props);
        c.close();
    }

    // RULE: CONSUMER_COMMIT_PER_RECORD.
    public void consumerCommitPerRecord(KafkaConsumer<String, String> c) {
        while (running()) {
            ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : records) {
                handle(r);
                c.commitSync();
            }
        }
    }

    // RULE: CONSUMER_POLL_ZERO.
    public void consumerPollZero(KafkaConsumer<String, String> c) {
        c.poll(Duration.ZERO);
    }

    // RULE: PRODUCER_ACKS_ZERO.
    public void producerAcksZero() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("acks", "0");
        p.put("compression.type", "snappy");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_ALLOW_AUTO_CREATE_TOPICS_TRUE.
    public void consumerAllowAutoCreate() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("allow.auto.create.topics", "true");
        p.put("group.id", "g");
        KafkaConsumer<String, String> c = new KafkaConsumer<>(p);
        c.close();
    }

    // RULE: PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE.
    public void txnIdWithoutIdempotence() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("transactional.id", "my-tx");
        p.put("enable.idempotence", "false");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: PRODUCER_MAX_IN_FLIGHT_TOO_HIGH.
    public void maxInFlightTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("max.in.flight.requests.per.connection", "10");
        p.put("compression.type", "snappy");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_ASSIGN_AND_SUBSCRIBE.
    public void assignAndSubscribe(KafkaConsumer<String, String> c) {
        c.subscribe(List.of("t"));
        c.assign(List.of(new org.apache.kafka.common.TopicPartition("t", 0)));
    }

    // RULE: PRODUCER_ACKS_ONE.
    public void producerAcksOne() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("acks", "1");
        p.put("compression.type", "snappy");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: PRODUCER_RETRIES_ZERO.
    public void producerRetriesZero() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("retries", "0");
        p.put("compression.type", "snappy");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: PRODUCER_COMPRESSION_NONE_EXPLICIT.
    public void producerCompressionNoneExplicit() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("compression.type", "none");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: PRODUCER_LINGER_ZERO_NO_BATCH.
    public void producerLingerZero() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("linger.ms", "0");
        p.put("compression.type", "snappy");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_AUTO_OFFSET_RESET_LATEST.
    public void consumerAutoOffsetResetLatest() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("group.id", "g");
        p.put("auto.offset.reset", "latest");
        KafkaConsumer<String, String> c = new KafkaConsumer<>(p);
        c.close();
    }

    // RULE: PRODUCER_IDEMPOTENCE_DISABLED.
    public void producerIdempotenceDisabled() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("enable.idempotence", "false");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: KAFKA_CLIENT_TYPO_GROUP_ID.
    public void typoGroupId() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("groupId", "my-group");  // typo: should be group.id
        KafkaConsumer<String, String> c = new KafkaConsumer<>(p);
        c.close();
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
        p.put("group.id", "g");
        return p;
    }

    private boolean running() { return true; }
    private void handle(ConsumerRecord<String, String> r) {}
}
