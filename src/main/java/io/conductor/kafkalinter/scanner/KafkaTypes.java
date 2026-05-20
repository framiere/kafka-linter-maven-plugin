package io.conductor.kafkalinter.scanner;

import java.util.Set;

public final class KafkaTypes {
    private KafkaTypes() {}

    public static final String PRODUCER_RECORD = "org/apache/kafka/clients/producer/ProducerRecord";
    public static final String KAFKA_PRODUCER = "org/apache/kafka/clients/producer/KafkaProducer";
    public static final String PRODUCER_INTERFACE = "org/apache/kafka/clients/producer/Producer";
    public static final String KAFKA_CONSUMER = "org/apache/kafka/clients/consumer/KafkaConsumer";
    public static final String CONSUMER_INTERFACE = "org/apache/kafka/clients/consumer/Consumer";
    public static final String CALLBACK = "org/apache/kafka/clients/producer/Callback";
    public static final String KAFKA_STREAMS = "org/apache/kafka/streams/KafkaStreams";
    public static final String KSTREAM = "org/apache/kafka/streams/kstream/KStream";

    public static final Set<String> PRODUCER_OWNERS = Set.of(KAFKA_PRODUCER, PRODUCER_INTERFACE);
    public static final Set<String> CONSUMER_OWNERS = Set.of(KAFKA_CONSUMER, CONSUMER_INTERFACE);

    public static final String FUTURE = "java/util/concurrent/Future";
    public static final String DURATION = "java/time/Duration";
    public static final String PROPERTIES = "java/util/Properties";
    public static final String MAP = "java/util/Map";
    public static final String HASHMAP = "java/util/HashMap";

    public static final Set<String> CONFIG_HOLDERS = Set.of(PROPERTIES, MAP, HASHMAP);
    public static final Set<String> CONFIG_PUT_METHODS = Set.of("put", "setProperty");

    // kafka-clients config keys
    public static final String COMPRESSION_TYPE_KEY = "compression.type";
    public static final String ENABLE_AUTO_COMMIT_KEY = "enable.auto.commit";
    public static final String ACKS_KEY = "acks";
    public static final String TRANSACTIONAL_ID_KEY = "transactional.id";
    public static final String ENABLE_IDEMPOTENCE_KEY = "enable.idempotence";
    public static final String MAX_IN_FLIGHT_KEY = "max.in.flight.requests.per.connection";
    public static final String ALLOW_AUTO_CREATE_TOPICS_KEY = "allow.auto.create.topics";
    public static final String GROUP_ID_KEY = "group.id";
    public static final String SCHEMA_REGISTRY_URL_KEY = "schema.registry.url";
    public static final String SR_AUTO_REGISTER_SCHEMAS_KEY = "auto.register.schemas";
    public static final String SR_USE_LATEST_VERSION_KEY = "use.latest.version";
    public static final String AUTO_OFFSET_RESET_KEY = "auto.offset.reset";
    public static final String RETRIES_KEY = "retries";
    public static final String LINGER_MS_KEY = "linger.ms";
    public static final String ISOLATION_LEVEL_KEY = "isolation.level";
    public static final String MAX_POLL_RECORDS_KEY = "max.poll.records";
    public static final String DELIVERY_TIMEOUT_MS_KEY = "delivery.timeout.ms";
    public static final String BUFFER_MEMORY_KEY = "buffer.memory";
    public static final String REQUEST_TIMEOUT_MS_KEY = "request.timeout.ms";
    public static final String SESSION_TIMEOUT_MS_KEY = "session.timeout.ms";
    public static final String TRANSACTION_TIMEOUT_MS_KEY = "transaction.timeout.ms";
    public static final String MAX_POLL_INTERVAL_MS_KEY = "max.poll.interval.ms";
    public static final String BATCH_SIZE_KEY = "batch.size";
    public static final String PARTITIONER_CLASS_KEY = "partitioner.class";
    public static final String FETCH_MAX_BYTES_KEY = "fetch.max.bytes";
    public static final String CHECK_CRCS_KEY = "check.crcs";
    public static final String MAX_BLOCK_MS_KEY = "max.block.ms";
    public static final String DEFAULT_API_TIMEOUT_MS_KEY = "default.api.timeout.ms";
    public static final String FETCH_MIN_BYTES_KEY = "fetch.min.bytes";
    public static final String HEARTBEAT_INTERVAL_MS_KEY = "heartbeat.interval.ms";
    public static final String RECONNECT_BACKOFF_MS_KEY = "reconnect.backoff.ms";
    public static final String RETRY_BACKOFF_MS_KEY = "retry.backoff.ms";
    public static final String AUTO_COMMIT_INTERVAL_MS_KEY = "auto.commit.interval.ms";
    public static final String MAX_REQUEST_SIZE_KEY = "max.request.size";
    public static final String METADATA_MAX_AGE_MS_KEY = "metadata.max.age.ms";
    public static final String FETCH_MAX_WAIT_MS_KEY = "fetch.max.wait.ms";
    public static final Set<String> PRODUCER_GENERIC_TRANSACTIONAL_IDS = Set.of(
            "tx", "txn", "transaction", "my-tx", "my-txn", "my-transaction",
            "test", "test-tx", "test-txn", "demo", "demo-tx",
            "app", "application", "default", "producer", "kafka-producer"
    );
    public static final Set<String> CONSUMER_GENERIC_GROUP_IDS = Set.of(
            "group", "consumer", "consumer-group", "kafka-consumer",
            "my-group", "my-consumer", "test", "test-group", "demo", "demo-group",
            "app", "application", "default", "tmp", "tmp-group"
    );
    /** Partitioner classes deprecated by KIP-794 since 3.3 — the default strategy is now strictly better. */
    public static final Set<String> PARTITIONER_DEPRECATED_FQCNS = Set.of(
            "org.apache.kafka.clients.producer.internals.DefaultPartitioner",
            "org.apache.kafka.clients.producer.UniformStickyPartitioner"
    );
    public static final String SECURITY_PROTOCOL_KEY = "security.protocol";
    public static final String SSL_ENDPOINT_ID_ALGO_KEY = "ssl.endpoint.identification.algorithm";
    public static final String SASL_JAAS_CONFIG_KEY = "sasl.jaas.config";
    public static final String BASIC_AUTH_USER_INFO_KEY = "basic.auth.user.info";
    public static final String SSL_KEYSTORE_PASSWORD_KEY = "ssl.keystore.password";
    public static final String SSL_TRUSTSTORE_PASSWORD_KEY = "ssl.truststore.password";
    public static final String SSL_KEY_PASSWORD_KEY = "ssl.key.password";

    // kafka-streams config keys
    public static final String STREAMS_APPLICATION_ID_KEY = "application.id";
    public static final Set<String> STREAMS_GENERIC_APPLICATION_IDS = Set.of(
            "streams-app", "kafka-streams", "kafka-streams-app", "streams",
            "my-streams-app", "my-app", "myapp",
            "test", "test-app", "demo", "demo-app", "example", "example-app",
            "app", "application", "dev", "default"
    );
    public static final String STREAMS_REPLICATION_FACTOR_KEY = "replication.factor";
    public static final String STREAMS_STATE_DIR_KEY = "state.dir";
    public static final String STREAMS_PROCESSING_GUARANTEE_KEY = "processing.guarantee";
    public static final Set<String> STREAMS_EOS_V1_VALUES = Set.of("exactly_once", "exactly_once_beta");
    public static final String STREAMS_COMMIT_INTERVAL_MS_KEY = "commit.interval.ms";
    public static final String STREAMS_TASK_TIMEOUT_MS_KEY = "task.timeout.ms";
    public static final String STREAMS_CACHE_MAX_BYTES_BUFFERING_KEY = "cache.max.bytes.buffering";
    public static final String STREAMS_STATESTORE_CACHE_MAX_BYTES_KEY = "statestore.cache.max.bytes";
    public static final String STREAMS_NUM_STANDBY_REPLICAS_KEY = "num.standby.replicas";
    public static final String STREAMS_DEFAULT_DESER_HANDLER_KEY = "default.deserialization.exception.handler";
    public static final String STREAMS_LOG_AND_CONTINUE_HANDLER_FQCN = "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler";

    // Schema-registry deserializer FQCNs
    public static final Set<String> SR_SERIALIZER_OWNERS = Set.of(
            "io/confluent/kafka/serializers/KafkaAvroSerializer",
            "io/confluent/kafka/serializers/KafkaAvroDeserializer",
            "io/confluent/kafka/serializers/protobuf/KafkaProtobufSerializer",
            "io/confluent/kafka/serializers/protobuf/KafkaProtobufDeserializer",
            "io/confluent/kafka/serializers/json/KafkaJsonSchemaSerializer",
            "io/confluent/kafka/serializers/json/KafkaJsonSchemaDeserializer"
    );

    // Spring annotations (descriptors are Lpath/To/Annotation;)
    public static final String SPRING_KAFKA_LISTENER_ANNOTATION = "Lorg/springframework/kafka/annotation/KafkaListener;";
    public static final String SPRING_ASYNC_ANNOTATION = "Lorg/springframework/scheduling/annotation/Async;";

    // Jackson polymorphic-typing annotations
    public static final String JACKSON_TYPE_INFO_ANNOTATION = "Lcom/fasterxml/jackson/annotation/JsonTypeInfo;";
    public static final String JACKSON_ACTIVATE_DEFAULT_TYPING_METHOD = "activateDefaultTyping";
    public static final String JACKSON_ENABLE_DEFAULT_TYPING_METHOD = "enableDefaultTyping";
    public static final String OBJECT_MAPPER = "com/fasterxml/jackson/databind/ObjectMapper";
}
