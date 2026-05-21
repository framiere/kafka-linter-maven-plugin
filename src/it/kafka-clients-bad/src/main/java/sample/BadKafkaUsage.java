package sample;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
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

    // RULE: SR_VALUE_SUBJECT_NAME_STRATEGY_NON_DEFAULT.
    public void srValueSubjectNameStrategyNonDefault() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("schema.registry.url", "https://schema-registry:8081");
        p.put("value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: SR_KEY_SUBJECT_NAME_STRATEGY_NON_DEFAULT.
    public void srKeySubjectNameStrategyNonDefault() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("schema.registry.url", "https://schema-registry:8081");
        p.put("key.subject.name.strategy", "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy");
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

    // RULE: PRODUCER_TRANSACTION_TIMEOUT_MS_TOO_HIGH.
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

    // RULE: PRODUCER_PARTITIONER_CLASS_DEPRECATED.
    public void producerPartitionerClassDeprecated() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("partitioner.class", "org.apache.kafka.clients.producer.UniformStickyPartitioner");
        p.put("compression.type", "snappy");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_FETCH_MAX_BYTES_TOO_LOW.
    public void consumerFetchMaxBytesTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "g");
        p.put("fetch.max.bytes", "65536");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: CONSUMER_CHECK_CRCS_FALSE.
    public void consumerCheckCrcsFalse() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "g");
        p.put("check.crcs", "false");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: PRODUCER_MAX_BLOCK_MS_TOO_LOW.
    public void producerMaxBlockMsTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("max.block.ms", "1000");
        p.put("compression.type", "snappy");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_DEFAULT_API_TIMEOUT_MS_TOO_LOW.
    public void consumerDefaultApiTimeoutMsTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "g");
        p.put("default.api.timeout.ms", "5000");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: CONSUMER_GROUP_ID_GENERIC.
    public void consumerGroupIdGeneric() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "my-group");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: PRODUCER_DELIVERY_TIMEOUT_MS_TOO_HIGH.
    public void producerDeliveryTimeoutMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("delivery.timeout.ms", "3600000");
        p.put("compression.type", "snappy");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_FETCH_MIN_BYTES_TOO_HIGH.
    public void consumerFetchMinBytesTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "g");
        p.put("fetch.min.bytes", "52428800");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: CONSUMER_HEARTBEAT_INTERVAL_MS_TOO_LOW.
    public void consumerHeartbeatIntervalMsTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "g");
        p.put("heartbeat.interval.ms", "100");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: PRODUCER_RECONNECT_BACKOFF_MS_TOO_LOW.
    public void producerReconnectBackoffMsTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("reconnect.backoff.ms", "10");
        p.put("compression.type", "snappy");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: PRODUCER_RETRY_BACKOFF_MS_TOO_LOW.
    public void producerRetryBackoffMsTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("retry.backoff.ms", "5");
        p.put("compression.type", "snappy");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_AUTO_COMMIT_INTERVAL_MS_TOO_HIGH.
    public void consumerAutoCommitIntervalMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "g");
        p.put("auto.commit.interval.ms", "300000");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: PRODUCER_MAX_REQUEST_SIZE_TOO_HIGH.
    public void producerMaxRequestSizeTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("max.request.size", "104857600");
        p.put("compression.type", "snappy");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: KAFKA_METADATA_MAX_AGE_MS_TOO_LOW.
    public void metadataMaxAgeMsTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("metadata.max.age.ms", "5000");
        p.put("compression.type", "snappy");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_FETCH_MAX_WAIT_MS_TOO_HIGH.
    public void consumerFetchMaxWaitMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "consumer-fetch-wait-test");
        p.put("fetch.max.wait.ms", "30000");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: PRODUCER_TRANSACTIONAL_ID_GENERIC.
    public void producerTransactionalIdGeneric() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("transactional.id", "txn");
        p.put("enable.idempotence", "true");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: SECURITY_SSL_PROTOCOL_LEGACY.
    public void sslProtocolLegacy() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("ssl.protocol", "TLSv1.1");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_EXCLUDE_INTERNAL_TOPICS_FALSE.
    public void consumerExcludeInternalTopicsFalse() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "consumer-internal-topics");
        p.put("exclude.internal.topics", "false");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: PRODUCER_COMPRESSION_GZIP.
    public void producerCompressionGzip() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "gzip");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_PARTITION_ASSIGNMENT_LEGACY.
    public void consumerPartitionAssignmentLegacy() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "consumer-legacy-assignor");
        p.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: PRODUCER_BUFFER_MEMORY_TOO_HIGH.
    public void producerBufferMemoryTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("buffer.memory", "536870912");
        p.put("compression.type", "snappy");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: SECURITY_SASL_MECHANISM_PLAIN.
    public void securitySaslMechanismPlain() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("sasl.mechanism", "PLAIN");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: SECURITY_SSL_KEYSTORE_TYPE_JKS.
    public void sslKeystoreTypeJks() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("ssl.keystore.type", "JKS");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_AUTO_OFFSET_RESET_NONE_EXPLICIT.
    public void consumerAutoOffsetResetNone() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "consumer-offset-none");
        p.put("auto.offset.reset", "none");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: KAFKA_CLIENT_ID_GENERIC.
    public void kafkaClientIdGeneric() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("client.id", "producer");
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

    // RULE: KAFKA_CONNECTIONS_MAX_IDLE_MS_TOO_LOW.
    public void kafkaConnectionsMaxIdleMsTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("connections.max.idle.ms", "5000");
        p.put("compression.type", "snappy");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_MAX_PARTITION_FETCH_BYTES_TOO_HIGH.
    public void consumerMaxPartitionFetchBytesTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "my-distinct-consumer-group-v1");
        p.put("max.partition.fetch.bytes", "104857600");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: CONSUMER_GROUP_INSTANCE_ID_GENERIC.
    public void consumerGroupInstanceIdGeneric() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        p.put("group.instance.id", "consumer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: PRODUCER_SEND_BUFFER_BYTES_TOO_SMALL.
    public void producerSendBufferBytesTooSmall() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("send.buffer.bytes", "8192");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_RECEIVE_BUFFER_BYTES_TOO_SMALL.
    public void consumerReceiveBufferBytesTooSmall() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        p.put("receive.buffer.bytes", "4096");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: CONSUMER_AUTO_COMMIT_INTERVAL_MS_TOO_LOW.
    public void consumerAutoCommitIntervalMsTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        p.put("auto.commit.interval.ms", "100");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: PRODUCER_BATCH_SIZE_TOO_LARGE.
    public void producerBatchSizeTooLarge() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("batch.size", "4194304");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: PRODUCER_REQUEST_TIMEOUT_MS_TOO_HIGH.
    public void producerRequestTimeoutMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("request.timeout.ms", "900000");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_MAX_POLL_INTERVAL_MS_TOO_HIGH.
    public void consumerMaxPollIntervalMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        p.put("max.poll.interval.ms", "3600000");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: CONSUMER_FETCH_MAX_BYTES_TOO_HIGH.
    public void consumerFetchMaxBytesTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        p.put("fetch.max.bytes", "209715200");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: CONSUMER_SESSION_TIMEOUT_MS_TOO_HIGH.
    public void consumerSessionTimeoutMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        p.put("session.timeout.ms", "300000");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: CONSUMER_MAX_PARTITION_FETCH_BYTES_TOO_LOW.
    public void consumerMaxPartitionFetchBytesTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "broker-1.prod.example.com:9092,broker-2.prod.example.com:9092,broker-3.prod.example.com:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        p.put("max.partition.fetch.bytes", "524288");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: CONSUMER_DEFAULT_API_TIMEOUT_MS_TOO_HIGH.
    public void consumerDefaultApiTimeoutMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "broker-1.prod.example.com:9092,broker-2.prod.example.com:9092,broker-3.prod.example.com:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        p.put("default.api.timeout.ms", "600000");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: KAFKA_BOOTSTRAP_SERVERS_SINGLE_BROKER.
    public void bootstrapServersSingleBroker() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "broker-1.prod.example.com:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: SCHEMA_REGISTRY_URL_LOCALHOST.
    public void schemaRegistryUrlLocalhost() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "broker-1.prod.example.com:9092,broker-2.prod.example.com:9092,broker-3.prod.example.com:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        p.put("schema.registry.url", "http://localhost:8081");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: CONSUMER_MAX_POLL_RECORDS_TOO_LOW.
    public void consumerMaxPollRecordsTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        p.put("max.poll.records", "1");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: PRODUCER_MAX_REQUEST_SIZE_TOO_LOW.
    public void producerMaxRequestSizeTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "zstd");
        p.put("max.request.size", "32768");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: KAFKA_METADATA_MAX_AGE_MS_TOO_HIGH.
    public void kafkaMetadataMaxAgeMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        p.put("metadata.max.age.ms", "3600000");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: CONSUMER_HEARTBEAT_INTERVAL_MS_TOO_HIGH.
    public void consumerHeartbeatIntervalMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        p.put("heartbeat.interval.ms", "20000");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: PRODUCER_MAX_BLOCK_MS_TOO_HIGH.
    public void producerMaxBlockMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "zstd");
        p.put("max.block.ms", "600000");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_FETCH_MAX_WAIT_MS_TOO_LOW.
    public void consumerFetchMaxWaitMsTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        p.put("fetch.max.wait.ms", "10");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: PRODUCER_RETRY_BACKOFF_MS_TOO_HIGH.
    public void producerRetryBackoffMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "zstd");
        p.put("retry.backoff.ms", "60000");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: PRODUCER_PARTITIONER_IGNORE_KEYS_TRUE.
    public void producerPartitionerIgnoreKeysTrue() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "zstd");
        p.put("partitioner.ignore.keys", "true");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: PRODUCER_CLIENT_DNS_LOOKUP_DEFAULT.
    public void producerClientDnsLookupDefault() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "zstd");
        p.put("client.dns.lookup", "default");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: KAFKA_AUTO_INCLUDE_JMX_REPORTER_FALSE.
    public void kafkaAutoIncludeJmxReporterFalse() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "zstd");
        p.put("auto.include.jmx.reporter", "false");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_AUTO_OFFSET_RESET_INVALID.
    public void consumerAutoOffsetResetInvalid() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        p.put("auto.offset.reset", "earleist");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: PRODUCER_ACKS_INVALID.
    public void producerAcksInvalid() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "zstd");
        p.put("acks", "true");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_INTERCEPTOR_CLASSES_LEGACY.
    public void consumerInterceptorClassesLegacy() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        p.put("interceptor.classes",
                "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: CONSUMER_GROUP_PROTOCOL_CLASSIC.
    public void consumerGroupProtocolClassic() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        p.put("group.protocol", "classic");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: KAFKA_ENABLE_METRICS_PUSH_FALSE.
    public void enableMetricsPushFalse() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("enable.metrics.push", "false");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_PARTITION_ASSIGNMENT_STRATEGY_MIXED.
    public void partitionAssignmentStrategyMixed() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        p.put("partition.assignment.strategy",
                "org.apache.kafka.clients.consumer.RangeAssignor,"
                + "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: CONSUMER_GROUP_ID_PLACEHOLDER.
    public void consumerGroupIdPlaceholder() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "${SERVICE_NAME}");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: KAFKA_CLIENT_ID_PLACEHOLDER.
    public void clientIdPlaceholder() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("client.id", "${POD_NAME}");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: KAFKA_BOOTSTRAP_SERVERS_PLACEHOLDER.
    public void bootstrapServersPlaceholder() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "${KAFKA_BROKERS}");
        p.put("compression.type", "snappy");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: PRODUCER_TRANSACTIONAL_ID_PLACEHOLDER.
    public void transactionalIdPlaceholder() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("transactional.id", "${POD_NAME}");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: KAFKA_METRICS_RECORDING_LEVEL_DEBUG.
    public void metricsRecordingLevelDebug() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("metrics.recording.level", "DEBUG");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: KAFKA_RECONNECT_BACKOFF_MAX_MS_TOO_LOW.
    public void reconnectBackoffMaxMsTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("reconnect.backoff.max.ms", "200");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT_MS_TOO_LOW.
    public void socketConnectionSetupTimeoutMsTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("socket.connection.setup.timeout.ms", "1000");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: CONSUMER_GROUP_INSTANCE_ID_PLACEHOLDER — unresolved ${...} reaches static-membership identity.
    public void consumerGroupInstanceIdPlaceholder() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "orders-fraud-detection-v3");
        p.put("group.instance.id", "${POD_NAME}");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.close();
    }

    // RULE: SECURITY_PROTOCOL_PLACEHOLDER — unresolved ${...} reaches security.protocol validator.
    public void securityProtocolPlaceholder() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("security.protocol", "${KAFKA_SECURITY_PROTOCOL}");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: KAFKA_CONNECTIONS_MAX_IDLE_MS_TOO_HIGH — above 600 000 ms exceeds broker default / LB idle timeouts.
    public void connectionsMaxIdleMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("connections.max.idle.ms", "1800000");
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.close();
    }

    // RULE: JACKSON_DEFAULT_TYPING_ENABLED — both legacy enableDefaultTyping and modern activateDefaultTyping forms.
    @SuppressWarnings("deprecation")
    public ObjectMapper jacksonDefaultTypingEnabled() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enableDefaultTyping();
        return mapper;
    }

    public ObjectMapper jacksonActivateDefaultTypingLaissezFaire() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.activateDefaultTyping(LaissezFaireSubTypeValidator.instance);
        return mapper;
    }

    // RULE: KAFKA_RETRY_BACKOFF_MAX_MS_TOO_LOW.
    public Properties retryBackoffMaxMsTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("retry.backoff.max.ms", "200");
        return p;
    }

    // RULE: PRODUCER_PARTITIONER_ADAPTIVE_PARTITIONING_DISABLED.
    public Properties partitionerAdaptiveDisabled() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("partitioner.adaptive.partitioning.enable", "false");
        return p;
    }

    // RULE: KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_TOO_LOW.
    public Properties socketConnectionSetupTimeoutMaxMsTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("socket.connection.setup.timeout.max.ms", "5000");
        return p;
    }

    // RULE: KAFKA_SASL_LOGIN_CONNECT_TIMEOUT_MS_TOO_LOW.
    public Properties saslLoginConnectTimeoutMsTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("sasl.login.connect.timeout.ms", "2000");
        return p;
    }

    // RULE: KAFKA_SASL_LOGIN_READ_TIMEOUT_MS_TOO_LOW.
    public Properties saslLoginReadTimeoutMsTooLow() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("sasl.login.read.timeout.ms", "2000");
        return p;
    }

    // RULE: SECURITY_SASL_OAUTHBEARER_TOKEN_ENDPOINT_HTTP.
    public Properties saslOauthbearerTokenEndpointHttp() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("sasl.oauthbearer.token.endpoint.url", "http://idp.internal:8080/oauth2/token");
        return p;
    }

    // RULE: SECURITY_SSL_KEYSTORE_LOCATION_TMP.
    public Properties sslKeystoreLocationTmp() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("security.protocol", "SSL");
        p.put("ssl.keystore.location", "/tmp/kafka-client.jks");
        return p;
    }

    // RULE: SECURITY_SSL_TRUSTSTORE_LOCATION_TMP.
    public Properties sslTruststoreLocationTmp() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("security.protocol", "SSL");
        p.put("ssl.truststore.location", "/tmp/kafka-truststore.jks");
        return p;
    }

    // RULE: KAFKA_CLIENT_RACK_PLACEHOLDER.
    public Properties clientRackPlaceholder() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("client.rack", "${POD_TOPOLOGY_ZONE}");
        return p;
    }

    // RULE: PRODUCER_SEND_BUFFER_BYTES_TOO_HIGH.
    public Properties sendBufferBytesTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("send.buffer.bytes", "33554432");
        return p;
    }

    // RULE: CONSUMER_RECEIVE_BUFFER_BYTES_TOO_HIGH.
    public Properties receiveBufferBytesTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("group.id", "g");
        p.put("receive.buffer.bytes", "33554432");
        return p;
    }

    // RULE: KAFKA_RETRY_BACKOFF_MAX_MS_TOO_HIGH.
    public Properties retryBackoffMaxMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("retry.backoff.max.ms", "300000");
        return p;
    }

    // RULE: PRODUCER_RECONNECT_BACKOFF_MS_TOO_HIGH.
    public Properties reconnectBackoffMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("reconnect.backoff.ms", "60000");
        return p;
    }

    // RULE: KAFKA_RECONNECT_BACKOFF_MAX_MS_TOO_HIGH.
    public Properties reconnectBackoffMaxMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("reconnect.backoff.max.ms", "300000");
        return p;
    }

    // RULE: KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT_MS_TOO_HIGH.
    public Properties socketConnectionSetupTimeoutMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("socket.connection.setup.timeout.ms", "300000");
        return p;
    }

    // RULE: KAFKA_SASL_LOGIN_CONNECT_TIMEOUT_MS_TOO_HIGH.
    public Properties saslLoginConnectTimeoutMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("sasl.login.connect.timeout.ms", "300000");
        return p;
    }

    // RULE: KAFKA_SASL_LOGIN_READ_TIMEOUT_MS_TOO_HIGH.
    public Properties saslLoginReadTimeoutMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("sasl.login.read.timeout.ms", "300000");
        return p;
    }

    // RULE: KAFKA_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_TOO_HIGH.
    public Properties socketConnectionSetupTimeoutMaxMsTooHigh() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("socket.connection.setup.timeout.max.ms", "600000");
        return p;
    }

    // RULE: SECURITY_SSL_ENABLED_PROTOCOLS_LEGACY.
    public Properties sslEnabledProtocolsLegacy() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("ssl.enabled.protocols", "TLSv1.2,TLSv1.1");
        return p;
    }

    // RULE: SECURITY_SSL_CIPHER_SUITES_LEGACY.
    public Properties sslCipherSuitesLegacy() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("ssl.cipher.suites", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_RSA_WITH_RC4_128_SHA");
        return p;
    }

    // RULE: CRED_SR_BEARER_AUTH_TOKEN_LITERAL.
    public Properties srBearerAuthTokenLiteral() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("bearer.auth.token", "eyJhbGciOiJIUzI1NiJ9.fakejwt.payload");
        return p;
    }

    // RULE: SR_LATEST_COMPATIBILITY_STRICT_FALSE.
    public Properties latestCompatibilityStrictFalse() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka:9092");
        p.put("compression.type", "snappy");
        p.put("schema.registry.url", "https://sr:8081");
        p.put("latest.compatibility.strict", "false");
        return p;
    }

    // RULE: CONSUMER_POLL_LONG_DEPRECATED.
    @SuppressWarnings("deprecation")
    public void consumerPollLongDeprecated() {
        Properties p = consumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.subscribe(java.util.List.of("topic"));
        consumer.poll(1000L);
        consumer.close();
    }

    // RULE: CONSUMER_COMMITSYNC_NO_TIMEOUT.
    public void consumerCommitSyncNoTimeout() {
        Properties p = consumerProps();
        p.put("enable.auto.commit", "false");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.subscribe(java.util.List.of("topic"));
        consumer.poll(Duration.ofMillis(500));
        consumer.commitSync();
        consumer.close();
    }

    // RULE: CONSUMER_END_OFFSETS_NO_TIMEOUT.
    public void consumerEndOffsetsNoTimeout() {
        Properties p = consumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        java.util.List<org.apache.kafka.common.TopicPartition> tps = java.util.List.of(
                new org.apache.kafka.common.TopicPartition("topic", 0));
        // Unbounded — blocks for up to default.api.timeout.ms on broker outage.
        java.util.Map<org.apache.kafka.common.TopicPartition, Long> ends = consumer.endOffsets(tps);
        ends.forEach((tp, off) -> { /* compute lag here */ });
        consumer.close();
    }

    // RULE: CONSUMER_BEGINNING_OFFSETS_NO_TIMEOUT.
    public void consumerBeginningOffsetsNoTimeout() {
        Properties p = consumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        java.util.List<org.apache.kafka.common.TopicPartition> tps = java.util.List.of(
                new org.apache.kafka.common.TopicPartition("topic", 0));
        java.util.Map<org.apache.kafka.common.TopicPartition, Long> begins = consumer.beginningOffsets(tps);
        begins.forEach((tp, off) -> { /* find earliest offset */ });
        consumer.close();
    }

    // RULE: CONSUMER_OFFSETS_FOR_TIMES_NO_TIMEOUT.
    public void consumerOffsetsForTimesNoTimeout() {
        Properties p = consumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        java.util.Map<org.apache.kafka.common.TopicPartition, Long> q = new java.util.HashMap<>();
        q.put(new org.apache.kafka.common.TopicPartition("topic", 0), System.currentTimeMillis() - 86_400_000L);
        java.util.Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> r = consumer.offsetsForTimes(q);
        r.forEach((tp, off) -> { /* replay-from-yesterday entry point */ });
        consumer.close();
    }

    // RULE: CONSUMER_POSITION_NO_TIMEOUT.
    public void consumerPositionNoTimeout() {
        Properties p = consumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        org.apache.kafka.common.TopicPartition tp = new org.apache.kafka.common.TopicPartition("topic", 0);
        consumer.assign(java.util.List.of(tp));
        long pos = consumer.position(tp); // unbounded — blocks on coordinator outage when cache is stale
        System.out.println("position=" + pos);
        consumer.close();
    }

    // RULE: CONSUMER_PARTITIONS_FOR_NO_TIMEOUT.
    public void consumerPartitionsForNoTimeout() {
        Properties p = consumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        // Startup health check — hangs 60 s on bad bootstrap.servers.
        java.util.List<org.apache.kafka.common.PartitionInfo> parts = consumer.partitionsFor("topic");
        parts.forEach(pi -> System.out.println(pi.topic() + ":" + pi.partition()));
        consumer.close();
    }

    // RULE: CONSUMER_LIST_TOPICS_NO_TIMEOUT.
    public void consumerListTopicsNoTimeout() {
        Properties p = consumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        // Full-cluster metadata snapshot — expensive even when healthy, hangs on outage.
        java.util.Map<String, java.util.List<org.apache.kafka.common.PartitionInfo>> topics = consumer.listTopics();
        topics.forEach((t, parts) -> System.out.println(t + ": " + parts.size() + " partitions"));
        consumer.close();
    }

    // RULE: CONSUMER_COMMITTED_NO_TIMEOUT.
    public void consumerCommittedNoTimeout() {
        Properties p = consumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        java.util.Set<org.apache.kafka.common.TopicPartition> tps = java.util.Set.of(
                new org.apache.kafka.common.TopicPartition("topic", 0));
        // Lag-monitor pattern: hangs on group-coordinator outage.
        java.util.Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> off = consumer.committed(tps);
        off.forEach((tp, om) -> { /* compute lag */ });
        consumer.close();
    }

    // RULE: PRODUCER_CLOSE_NO_TIMEOUT.
    public void producerCloseNoTimeout() {
        Properties p = props();
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        producer.send(new ProducerRecord<>("topic", "k", "v"));
        // No-Duration close — blocks for Long.MAX_VALUE during a broker outage.
        producer.close();
    }

    // RULE: ADMIN_CLOSE_NO_TIMEOUT.
    public void adminCloseNoTimeout() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        admin.listTopics();
        // No-Duration close — blocks for Long.MAX_VALUE waiting on every outstanding future.
        admin.close();
    }

    // RULE: CONSUMER_SUBSCRIBE_WITHOUT_REBALANCE_LISTENER.
    public void subscribeWithoutListener() {
        Properties p = consumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        // No ConsumerRebalanceListener — partition revoke has no flush/commit hook.
        consumer.subscribe(java.util.List.of("topic"));
        consumer.close(Duration.ofSeconds(5));
    }

    // RULE: CONSUMER_SUBSCRIBE_WITHOUT_REBALANCE_LISTENER (Pattern overload).
    public void subscribePatternWithoutListener() {
        Properties p = consumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        // Pattern-based subscribe with no listener — same hazard as the Collection overload.
        consumer.subscribe(java.util.regex.Pattern.compile("topic-.*"));
        consumer.close(Duration.ofSeconds(5));
    }

    // RULE: CONSUMER_COMMIT_ASYNC_NO_CALLBACK — failures silently swallowed.
    public void commitAsyncNoCallback() {
        Properties p = consumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.subscribe(java.util.List.of("topic"));
        consumer.poll(Duration.ofMillis(500));
        // No callback — rebalance/coordinator-down failures vanish.
        consumer.commitAsync();
        consumer.close(Duration.ofSeconds(5));
    }

    // RULE: PRODUCER_RECORD_NO_KEY — 2-arg ctor with null key.
    public void noKeyProducerRecord() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props());
        // 2-arg ctor — null key, no per-key ordering, no compaction.
        producer.send(new ProducerRecord<>("events", "{\"payload\":\"x\"}"), (md, ex) -> {});
        producer.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_CREATE_TOPICS_NO_OPTIONS — createTopics with no CreateTopicsOptions.
    public void adminCreateTopicsNoOptions() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        admin.createTopics(java.util.List.of(
                new org.apache.kafka.clients.admin.NewTopic("events-v1", 6, (short) 3)));
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_DELETE_TOPICS_NO_OPTIONS — deleteTopics with no DeleteTopicsOptions.
    public void adminDeleteTopicsNoOptions() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        admin.deleteTopics(java.util.List.of("events-v0"));
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_DESCRIBE_TOPICS_NO_OPTIONS — describeTopics with no options leaves authorizedOperations() == null.
    public void adminDescribeTopicsNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        java.util.Map<String, org.apache.kafka.clients.admin.TopicDescription> td =
                admin.describeTopics(java.util.List.of("events-v1", "events-v0")).allTopicNames().get();
        td.forEach((name, desc) -> System.out.println(name + " -> partitions=" + desc.partitions().size()));
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_DESCRIBE_CONFIGS_NO_OPTIONS — describeConfigs with no options gets surface values with no synonym chain.
    public void adminDescribeConfigsNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        org.apache.kafka.common.config.ConfigResource topic =
                new org.apache.kafka.common.config.ConfigResource(
                        org.apache.kafka.common.config.ConfigResource.Type.TOPIC, "events-v1");
        java.util.Map<org.apache.kafka.common.config.ConfigResource, org.apache.kafka.clients.admin.Config> cfg =
                admin.describeConfigs(java.util.List.of(topic)).all().get();
        cfg.forEach((r, c) -> c.entries().forEach(e -> System.out.println(e.name() + "=" + e.value())));
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_LIST_TOPICS_NO_OPTIONS — listTopics with default timeout & listInternal=false silently excludes __consumer_offsets etc.
    public void adminListTopicsNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        java.util.Set<String> names = admin.listTopics().names().get();
        for (String name : names) {
            System.out.println(name);
        }
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_ALTER_CONFIGS_DEPRECATED — KIP-339: full-replacement alterConfigs resets every key not in payload.
    @SuppressWarnings("deprecation")
    public void adminAlterConfigsDeprecated() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        org.apache.kafka.common.config.ConfigResource topic =
                new org.apache.kafka.common.config.ConfigResource(
                        org.apache.kafka.common.config.ConfigResource.Type.TOPIC, "events-v1");
        org.apache.kafka.clients.admin.Config cfg = new org.apache.kafka.clients.admin.Config(
                java.util.List.of(new org.apache.kafka.clients.admin.ConfigEntry("retention.ms", "604800000")));
        // Deprecated since Kafka 2.3 — every unlisted key (cleanup.policy, segment.ms, ...) resets to broker default.
        admin.alterConfigs(java.util.Map.of(topic, cfg));
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_ALTER_PARTITION_REASSIGNMENTS_NO_OPTIONS — no AlterPartitionReassignmentsOptions: ack timeout fires on busy controller.
    public void adminAlterPartitionReassignmentsNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        java.util.Map<org.apache.kafka.common.TopicPartition,
                java.util.Optional<org.apache.kafka.clients.admin.NewPartitionReassignment>> plan = java.util.Map.of(
                new org.apache.kafka.common.TopicPartition("events-v1", 0),
                java.util.Optional.of(new org.apache.kafka.clients.admin.NewPartitionReassignment(java.util.List.of(2, 3, 4))));
        // No AlterPartitionReassignmentsOptions — controller ack timeout under default ~30s; retry hits duplicate-rejection.
        admin.alterPartitionReassignments(plan).all().get();
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_CREATE_ACLS_NO_OPTIONS — no CreateAclsOptions: security mutation under default timeout, masking all() future.
    public void adminCreateAclsNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        java.util.List<org.apache.kafka.common.acl.AclBinding> bindings = java.util.List.of(
                new org.apache.kafka.common.acl.AclBinding(
                        new org.apache.kafka.common.resource.ResourcePattern(
                                org.apache.kafka.common.resource.ResourceType.TOPIC, "events-v1",
                                org.apache.kafka.common.resource.PatternType.LITERAL),
                        new org.apache.kafka.common.acl.AccessControlEntry(
                                "User:new-tenant", "*",
                                org.apache.kafka.common.acl.AclOperation.READ,
                                org.apache.kafka.common.acl.AclPermissionType.ALLOW)));
        // No CreateAclsOptions — security mutation under default ~30s timeout.
        admin.createAcls(bindings).all().get();
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_DELETE_ACLS_NO_OPTIONS — no DeleteAclsOptions: IRREVERSIBLE security wipe with broad-filter semantics.
    public void adminDeleteAclsNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        java.util.List<org.apache.kafka.common.acl.AclBindingFilter> filters = java.util.List.of(
                new org.apache.kafka.common.acl.AclBindingFilter(
                        new org.apache.kafka.common.resource.ResourcePatternFilter(
                                org.apache.kafka.common.resource.ResourceType.TOPIC, "legacy-events",
                                org.apache.kafka.common.resource.PatternType.LITERAL),
                        org.apache.kafka.common.acl.AccessControlEntryFilter.ANY));
        // No DeleteAclsOptions — irreversible mutation under default ~30s timeout.
        admin.deleteAcls(filters).all().get();
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_INCREMENTAL_ALTER_CONFIGS_NO_OPTIONS — no AlterConfigsOptions: validateOnly defaults false, no dry-run.
    public void adminIncrementalAlterConfigsNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        org.apache.kafka.common.config.ConfigResource topic =
                new org.apache.kafka.common.config.ConfigResource(
                        org.apache.kafka.common.config.ConfigResource.Type.TOPIC, "events-v1");
        java.util.Collection<org.apache.kafka.clients.admin.AlterConfigOp> ops = java.util.List.of(
                new org.apache.kafka.clients.admin.AlterConfigOp(
                        new org.apache.kafka.clients.admin.ConfigEntry("retention.ms", "2592000000"),
                        org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET));
        // No AlterConfigsOptions — applied immediately with no validateOnly preview.
        admin.incrementalAlterConfigs(java.util.Map.of(topic, ops)).all().get();
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_ELECT_LEADERS_NO_OPTIONS — no ElectLeadersOptions: default timeout too short for election propagation.
    public void adminElectLeadersNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        java.util.Set<org.apache.kafka.common.TopicPartition> partitions = java.util.Set.of(
                new org.apache.kafka.common.TopicPartition("events-v1", 0),
                new org.apache.kafka.common.TopicPartition("events-v1", 1));
        // No ElectLeadersOptions — controller-driven election under default ~30s timeout.
        admin.electLeaders(org.apache.kafka.common.ElectionType.PREFERRED, partitions).all().get();
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_ALTER_REPLICA_LOG_DIRS_NO_OPTIONS — no AlterReplicaLogDirsOptions: ack timeout under default ~30s on busy broker.
    public void adminAlterReplicaLogDirsNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        java.util.Map<org.apache.kafka.common.TopicPartitionReplica, String> moves = java.util.Map.of(
                new org.apache.kafka.common.TopicPartitionReplica("events-v1", 0, 1), "/var/lib/kafka/disk1",
                new org.apache.kafka.common.TopicPartitionReplica("events-v1", 1, 1), "/var/lib/kafka/disk1");
        // No AlterReplicaLogDirsOptions — async migration on broker but synchronous ack times out at default ~30s.
        admin.alterReplicaLogDirs(moves).all().get();
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_DELETE_CONSUMER_GROUPS_NO_OPTIONS — no DeleteConsumerGroupsOptions: irreversible group deletion under default ~30s timeout.
    public void adminDeleteConsumerGroupsNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        // No DeleteConsumerGroupsOptions — half-deletion on timeout leaves some groups gone, others alive.
        admin.deleteConsumerGroups(java.util.List.of("legacy-group-1", "legacy-group-2", "legacy-group-3")).all().get();
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_DESCRIBE_LOG_DIRS_NO_OPTIONS — no DescribeLogDirsOptions: broker disk scan often exceeds default ~30s timeout.
    public void adminDescribeLogDirsNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        // No DescribeLogDirsOptions — multi-TB log-dir scan blows past default timeout on large brokers.
        java.util.Map<Integer, java.util.Map<String, org.apache.kafka.clients.admin.LogDirDescription>> logDirs =
                admin.describeLogDirs(java.util.List.of(1, 2, 3, 4, 5)).allDescriptions().get();
        for (java.util.Map.Entry<Integer, java.util.Map<String, org.apache.kafka.clients.admin.LogDirDescription>> e : logDirs.entrySet()) {
            System.out.println("broker=" + e.getKey() + " dirs=" + e.getValue().keySet());
        }
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_DESCRIBE_ACLS_NO_OPTIONS — no DescribeAclsOptions: full ACL-store scan often exceeds default ~30s timeout.
    public void adminDescribeAclsNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        // No DescribeAclsOptions — controller-side full ACL scan; multi-tenant clusters with tens of thousands of bindings time out.
        java.util.Collection<org.apache.kafka.common.acl.AclBinding> acls =
                admin.describeAcls(org.apache.kafka.common.acl.AclBindingFilter.ANY).values().get();
        System.out.println("acl-count=" + acls.size());
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_DESCRIBE_CONSUMER_GROUPS_NO_OPTIONS — no DescribeConsumerGroupsOptions: includeAuthorizedOperations defaults to false.
    public void adminDescribeConsumerGroupsNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        java.util.Map<String, org.apache.kafka.clients.admin.ConsumerGroupDescription> descs =
                admin.describeConsumerGroups(java.util.List.of("g1", "g2")).all().get();
        for (org.apache.kafka.clients.admin.ConsumerGroupDescription d : descs.values()) {
            System.out.println(d.groupId() + " state=" + d.state());
        }
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_LIST_CONSUMER_GROUP_OFFSETS_NO_OPTIONS — no ListConsumerGroupOffsetsOptions: returns ALL historical partitions.
    public void adminListConsumerGroupOffsetsNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        // No ListConsumerGroupOffsetsOptions — returns every partition the group has committed to in the retention window.
        java.util.Map<org.apache.kafka.common.TopicPartition,
                org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets =
                        admin.listConsumerGroupOffsets("my-group").partitionsToOffsetAndMetadata().get();
        for (java.util.Map.Entry<org.apache.kafka.common.TopicPartition,
                org.apache.kafka.clients.consumer.OffsetAndMetadata> e : offsets.entrySet()) {
            System.out.println(e.getKey() + " -> " + e.getValue());
        }
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_ALTER_CONSUMER_GROUP_OFFSETS_NO_OPTIONS — no AlterConsumerGroupOffsetsOptions: IRREVERSIBLE rewrite with default ~30s timeout.
    public void adminAlterConsumerGroupOffsetsNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        java.util.Map<org.apache.kafka.common.TopicPartition,
                org.apache.kafka.clients.consumer.OffsetAndMetadata> resets =
                        java.util.Map.of(new org.apache.kafka.common.TopicPartition("events", 0),
                                new org.apache.kafka.clients.consumer.OffsetAndMetadata(0L));
        // No AlterConsumerGroupOffsetsOptions — IRREVERSIBLE per-partition rewrite with default timeout.
        admin.alterConsumerGroupOffsets("my-group", resets).all().get();
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_DELETE_RECORDS_NO_OPTIONS — no DeleteRecordsOptions: timeout defaults to ~30s, half-truncation hazard.
    public void adminDeleteRecordsNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        java.util.Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.admin.RecordsToDelete> specs =
                java.util.Map.of(new org.apache.kafka.common.TopicPartition("events", 0),
                        org.apache.kafka.clients.admin.RecordsToDelete.beforeOffset(1_000_000L));
        // No DeleteRecordsOptions — IRREVERSIBLE truncation with default ~30s timeout.
        admin.deleteRecords(specs).all().get();
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_LIST_CONSUMER_GROUPS_NO_OPTIONS — no ListConsumerGroupsOptions: returns all states, multi-MB on big clusters.
    public void adminListConsumerGroupsNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        // No ListConsumerGroupsOptions — returns Stable + Empty + Dead + in-rebalance, all in one payload.
        java.util.Collection<org.apache.kafka.clients.admin.ConsumerGroupListing> groups =
                admin.listConsumerGroups().all().get();
        for (org.apache.kafka.clients.admin.ConsumerGroupListing g : groups) {
            System.out.println(g.groupId());
        }
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_LIST_TRANSACTIONS_NO_OPTIONS — no ListTransactionsOptions: returns all states, 10MB+ on EOS-v2 clusters.
    public void adminListTransactionsNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        // No ListTransactionsOptions — returns Ongoing + Complete* + Empty + Dead, full historical population.
        java.util.Collection<org.apache.kafka.clients.admin.TransactionListing> txns =
                admin.listTransactions().all().get();
        for (org.apache.kafka.clients.admin.TransactionListing t : txns) {
            System.out.println(t.transactionalId());
        }
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_DESCRIBE_CLUSTER_NO_OPTIONS — no DescribeClusterOptions: includeAuthorizedOperations defaults to false; cluster-level ACL state hidden.
    public void adminDescribeClusterNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        String clusterId = admin.describeCluster().clusterId().get();
        System.out.println("cluster: " + clusterId);
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_LIST_OFFSETS_NO_OPTIONS — no ListOffsetsOptions: isolationLevel defaults to READ_UNCOMMITTED; transactional topics over-report lag.
    public void adminListOffsetsNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        java.util.Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.admin.OffsetSpec> specs =
                java.util.Map.of(new org.apache.kafka.common.TopicPartition("orders", 0),
                        org.apache.kafka.clients.admin.OffsetSpec.latest());
        // No ListOffsetsOptions — defaults to READ_UNCOMMITTED, misreporting end-offsets on transactional topics.
        java.util.Map<org.apache.kafka.common.TopicPartition,
                org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> r = admin.listOffsets(specs).all().get();
        System.out.println(r);
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_CREATE_PARTITIONS_NO_OPTIONS — no CreatePartitionsOptions: validateOnly dry-run unavailable; partition-add is irreversible.
    public void adminCreatePartitionsNoOptions() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        java.util.Map<String, org.apache.kafka.clients.admin.NewPartitions> specs =
                java.util.Map.of("payments",
                        org.apache.kafka.clients.admin.NewPartitions.increaseTo(24));
        // No CreatePartitionsOptions — no way to dry-run with validateOnly(true); change is applied immediately.
        admin.createPartitions(specs).all().get();
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: PRODUCER_SEND_OFFSETS_TO_TXN_GROUP_ID_DEPRECATED — String-groupId overload bypasses KIP-447 fencing.
    public void sendOffsetsToTxnDeprecatedGroupId() {
        Properties pp = props();
        pp.put("transactional.id", "tx-app-1");
        KafkaProducer<String, String> producer = new KafkaProducer<>(pp);
        producer.initTransactions();
        producer.beginTransaction();
        java.util.Map<org.apache.kafka.common.TopicPartition,
                org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets =
                        java.util.Map.of(new org.apache.kafka.common.TopicPartition("in", 0),
                                new org.apache.kafka.clients.consumer.OffsetAndMetadata(42L));
        // Deprecated since Kafka 3.0 — use sendOffsetsToTransaction(offsets, consumer.groupMetadata()).
        producer.sendOffsetsToTransaction(offsets, "consumer-group-id");
        producer.commitTransaction();
        producer.close(Duration.ofSeconds(5));
    }

    // RULE: CONSUMER_COMMITTED_SINGLE_PARTITION_DEPRECATED — single-partition committed() calls deprecated since Kafka 2.4.
    public void committedSinglePartitionDeprecated() {
        Properties p = consumerProps();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            org.apache.kafka.common.TopicPartition tp =
                    new org.apache.kafka.common.TopicPartition("orders", 0);
            // Bug 1: single-partition no-timeout form — each call is one OFFSET_FETCH round trip.
            org.apache.kafka.clients.consumer.OffsetAndMetadata om1 = consumer.committed(tp);
            // Bug 2: single-partition explicit-timeout form — same trap with an explicit per-call timeout.
            org.apache.kafka.clients.consumer.OffsetAndMetadata om2 =
                    consumer.committed(tp, Duration.ofSeconds(5));
            if (om1 != null && om2 != null) {
                om1.offset();
                om2.offset();
            }
        }
    }

    // RULE: ADMIN_DESCRIBE_TOPICS_RESULT_LEGACY_DEPRECATED — DescribeTopicsResult.values()/.all() deprecated since 3.1 (KIP-516).
    public void describeTopicsLegacyAccessors() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        org.apache.kafka.clients.admin.DescribeTopicsResult r = admin.describeTopics(java.util.List.of("orders"));
        // Bug 1: legacy values() — silently empty if the underlying call was made via TopicCollection.ofTopicIds(...).
        java.util.Map<String, org.apache.kafka.common.KafkaFuture<org.apache.kafka.clients.admin.TopicDescription>> v = r.values();
        v.size();
        // Bug 2: legacy all() — same problem; use allTopicNames() or allTopicIds() per the underlying query key-space.
        java.util.Map<String, org.apache.kafka.clients.admin.TopicDescription> a = r.all().get();
        a.size();
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_FEATURE_UPDATE_ALLOW_DOWNGRADE_DEPRECATED — boolean-flag constructor + allowDowngrade() getter deprecated since 3.3 (KIP-778).
    public void featureUpdateAllowDowngradeDeprecated() {
        // Bug 1: deprecated (short, boolean) constructor — cannot express UNSAFE_DOWNGRADE; binary flag conflates safety regimes.
        org.apache.kafka.clients.admin.FeatureUpdate u = new org.apache.kafka.clients.admin.FeatureUpdate((short) 5, true);
        // Bug 2: deprecated allowDowngrade() getter — replace reads with upgradeType() != UpgradeType.UPGRADE.
        boolean allow = u.allowDowngrade();
        System.out.println(allow);
    }

    // RULE: ADMIN_LIST_CONSUMER_GROUP_OFFSETS_TOPIC_PARTITIONS_DEPRECATED — per-Options topicPartitions filter deprecated since 3.3 (KIP-709).
    public void listConsumerGroupOffsetsOptionsTopicPartitionsDeprecated() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions opts =
                new org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions();
        // Bug 1: deprecated topicPartitions(List) setter — silently ignored by the batched Map-based overload.
        opts.topicPartitions(java.util.List.of(new org.apache.kafka.common.TopicPartition("orders", 0)));
        // Bug 2: deprecated topicPartitions() getter — read-side wiring on the legacy field.
        java.util.List<org.apache.kafka.common.TopicPartition> filter = opts.topicPartitions();
        if (filter != null) filter.size();
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_DESCRIBE_LOG_DIRS_RESULT_LEGACY_DEPRECATED — values()/.all() return internal LogDirInfo (KIP-743).
    public void describeLogDirsResultLegacyAccessors() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(p);
        org.apache.kafka.clients.admin.DescribeLogDirsResult r = admin.describeLogDirs(java.util.List.of(0));
        // Bug 1: legacy values() — returns Map<Integer, KafkaFuture<Map<String, DescribeLogDirsResponse$LogDirInfo>>>.
        java.util.Map<Integer, org.apache.kafka.common.KafkaFuture<java.util.Map<String, org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo>>> v = r.values();
        v.size();
        // Bug 2: legacy all() — same internal-type leak; use allDescriptions() returning LogDirDescription.
        java.util.Map<Integer, java.util.Map<String, org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo>> a = r.all().get();
        a.size();
        admin.close(Duration.ofSeconds(5));
    }

    // RULE: ADMIN_UPDATE_FEATURES_OPTIONS_DRY_RUN_DEPRECATED — dryRun() renamed to validateOnly() (KIP-919).
    public void updateFeaturesOptionsDryRunDeprecated() {
        org.apache.kafka.clients.admin.UpdateFeaturesOptions opts =
                new org.apache.kafka.clients.admin.UpdateFeaturesOptions();
        // Bug 1: deprecated dryRun(boolean) setter — rename to validateOnly(true).
        opts.dryRun(true);
        // Bug 2: deprecated dryRun() getter — rename to validateOnly().
        boolean dry = opts.dryRun();
        System.out.println(dry);
    }

    // RULE: ADMIN_TOPIC_LISTING_NAME_INTERNAL_CTOR_DEPRECATED — two-arg constructor predates topic IDs (KIP-516).
    public void topicListingNameInternalCtorDeprecated() {
        // Bug: deprecated (String, boolean) constructor — topicId() defaults to Uuid.ZERO_UUID.
        org.apache.kafka.clients.admin.TopicListing tl =
                new org.apache.kafka.clients.admin.TopicListing("orders", false);
        System.out.println(tl.name() + " internal=" + tl.isInternal());
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

    // RULE: CONSUMER_RECORD_LEGACY_CHECKSUM_CTOR_DEPRECATED — KIP-101 / KIP-82, deprecated since Kafka 2.0.
    public ConsumerRecord<String, String> consumerRecordLegacyChecksumCtor() {
        // Bug: ConsumerRecord constructor with `long checksum` after TimestampType — descriptor segment
        // `Lorg/apache/kafka/common/record/TimestampType;J`. The v2 message format moved CRCs to the batch level;
        // per-record checksum is meaningless. Replace with the no-checksum constructor.
        return new ConsumerRecord<>(
                "topic", 0, 0L, 0L,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                0L,    // deprecated checksum field (primitive long)
                0, 0,
                "k", "v");
    }

    // RULE: CONSUMER_RECORD_LEGACY_CHECKSUM_CTOR_DEPRECATED — boxed Long variant.
    public ConsumerRecord<String, String> consumerRecordLegacyChecksumCtorBoxedLong() {
        // Bug: same as above but with boxed Long checksum + headers. Descriptor segment
        // `Lorg/apache/kafka/common/record/TimestampType;Ljava/lang/Long;`.
        return new ConsumerRecord<>(
                "topic", 0, 0L, 0L,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                Long.valueOf(0L),    // deprecated checksum field (boxed Long)
                0, 0,
                "k", "v",
                new org.apache.kafka.common.header.internals.RecordHeaders());
    }

    // RULE: ADMIN_DELETE_TOPICS_RESULT_VALUES_DEPRECATED — KIP-516, deprecated since Kafka 3.0.
    public void deleteTopicsResultLegacyValues(org.apache.kafka.clients.admin.Admin admin) throws Exception {
        // Bug: DeleteTopicsResult.values() — name-keyed Map<String, KafkaFuture<Void>>. The unqualified
        // .values() throws UnsupportedOperationException at runtime if delete-by-id was used.
        org.apache.kafka.clients.admin.DeleteTopicsResult result =
                admin.deleteTopics(java.util.List.of("topic-a", "topic-b"));
        java.util.Map<String, org.apache.kafka.common.KafkaFuture<Void>> values = result.values();
        for (org.apache.kafka.common.KafkaFuture<Void> f : values.values()) {
            f.get();
        }
    }

    // RULE: PRODUCER_RECORD_METADATA_LEGACY_CHECKSUM_CTOR_DEPRECATED — KIP-101 / KIP-82, deprecated since Kafka 2.0.
    public org.apache.kafka.clients.producer.RecordMetadata recordMetadataLegacyChecksumCtor() {
        // Bug: 7-arg RecordMetadata constructor with `Long checksum` field. Descriptor
        // `(Lorg/apache/kafka/common/TopicPartition;JJJLjava/lang/Long;II)V`.
        // Replace with the 6-arg constructor that omits checksum.
        return new org.apache.kafka.clients.producer.RecordMetadata(
                new org.apache.kafka.common.TopicPartition("topic", 0),
                0L,    // baseOffset
                0L,    // batchIndex (long, deprecated form)
                0L,    // timestamp
                Long.valueOf(0L),    // deprecated checksum field
                0, 0);
    }

    // RULE: KAFKA_FUTURE_THENAPPLY_FUNCTION_DEPRECATED — KIP-707, deprecated since Kafka 3.0.
    public org.apache.kafka.common.KafkaFuture<Integer> kafkaFutureThenApplyLegacyFunction(
            org.apache.kafka.common.KafkaFuture<String> future) {
        // Bug: KafkaFuture.thenApply(KafkaFuture.Function) — legacy interface with checked-exception apply().
        // The compiler picks this overload because we declare a KafkaFuture.Function variable.
        org.apache.kafka.common.KafkaFuture.Function<String, Integer> legacyFn =
                new org.apache.kafka.common.KafkaFuture.Function<String, Integer>() {
                    @Override public Integer apply(String s) { return s == null ? 0 : s.length(); }
                };
        return future.thenApply(legacyFn);
    }
}
