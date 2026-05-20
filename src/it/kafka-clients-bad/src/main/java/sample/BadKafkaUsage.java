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

    // RULE: CONSUMER_MAX_POLL_RECORDS_TOO_HIGH.
    public void consumerMaxPollRecordsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("group.id", "g");
        p.put("max.poll.records", "5000");
        KafkaConsumer<String, String> c = new KafkaConsumer<>(p);
        c.close();
    }

    // RULE: PRODUCER_DELIVERY_TIMEOUT_TOO_SMALL.
    public void producerDeliveryTimeoutTooSmall() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("delivery.timeout.ms", "5000");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
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

    // RULE: SECURITY_PROTOCOL_PLAINTEXT.
    public void securityProtocolPlaintext() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("security.protocol", "PLAINTEXT");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: SECURITY_PROTOCOL_SASL_PLAINTEXT.
    public void securityProtocolSaslPlaintext() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("security.protocol", "SASL_PLAINTEXT");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: SSL_ENDPOINT_IDENTIFICATION_DISABLED.
    public void sslEndpointIdentificationDisabled() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("ssl.endpoint.identification.algorithm", "");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: SR_AUTO_REGISTER_SCHEMAS_TRUE.
    public void srAutoRegisterSchemasTrue() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("auto.register.schemas", "true");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: SR_USE_LATEST_VERSION_TRUE.
    public void srUseLatestVersionTrue() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("use.latest.version", "true");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: SCHEMA_REGISTRY_URL_HTTP.
    public void schemaRegistryUrlHttp() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("schema.registry.url", "http://schema-registry:8081");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: PRODUCER_BUFFER_MEMORY_TOO_SMALL.
    public void producerBufferMemoryTooSmall() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("buffer.memory", "1048576");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: PRODUCER_REQUEST_TIMEOUT_TOO_LOW.
    public void producerRequestTimeoutTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("request.timeout.ms", "3000");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_SESSION_TIMEOUT_TOO_LOW.
    public void consumerSessionTimeoutTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "g");
        p.put("session.timeout.ms", "5000");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: PRODUCER_TXN_TIMEOUT_TOO_LOW.
    public void producerTxnTimeoutTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("transactional.id", "tx");
        p.put("transaction.timeout.ms", "5000");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: PRODUCER_TXN_TIMEOUT_TOO_HIGH.
    public void producerTxnTimeoutTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("transactional.id", "tx");
        p.put("transaction.timeout.ms", "1800000");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_ISOLATION_LEVEL_READ_UNCOMMITTED_EXPLICIT.
    public void consumerIsolationLevelReadUncommitted() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "g");
        p.put("isolation.level", "read_uncommitted");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: CONSUMER_MAX_POLL_INTERVAL_MS_TOO_LOW.
    public void consumerMaxPollIntervalMsTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "g");
        p.put("max.poll.interval.ms", "30000");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: PRODUCER_BATCH_SIZE_TOO_SMALL.
    public void producerBatchSizeTooSmall() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("batch.size", "1024");
        p.put("compression.type", "snappy");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: PRODUCER_LINGER_MS_TOO_HIGH.
    public void producerLingerMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("linger.ms", "300000");
        p.put("compression.type", "snappy");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CRED_SASL_JAAS_LITERAL.
    public void credSaslJaasLiteral() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"svc\" password=\"hunter2\";");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CRED_BASIC_AUTH_USER_INFO_LITERAL.
    public void credBasicAuthUserInfoLiteral() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("basic.auth.user.info", "sr-user:sr-secret");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CRED_SSL_KEYSTORE_PASSWORD_LITERAL.
    public void credSslKeystorePasswordLiteral() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("ssl.keystore.password", "changeit");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
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
