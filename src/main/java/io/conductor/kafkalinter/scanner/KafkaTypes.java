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
    public static final String TIME_WINDOWS = "org/apache/kafka/streams/kstream/TimeWindows";
    public static final String JOIN_WINDOWS = "org/apache/kafka/streams/kstream/JoinWindows";

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
    public static final String PARTITIONER_IGNORE_KEYS_KEY = "partitioner.ignore.keys";
    public static final String CLIENT_DNS_LOOKUP_KEY = "client.dns.lookup";
    public static final String AUTO_INCLUDE_JMX_REPORTER_KEY = "auto.include.jmx.reporter";
    public static final String ENABLE_METRICS_PUSH_KEY = "enable.metrics.push";
    public static final String GROUP_PROTOCOL_KEY = "group.protocol";
    public static final String GROUP_PROTOCOL_CLASSIC_VALUE = "classic";
    public static final String STREAMS_APPLICATION_SERVER_KEY = "application.server";
    public static final Set<String> LOCALHOST_HOST_TOKENS = Set.of("localhost", "127.0.0.1", "0.0.0.0", "::1");
    public static final String STREAMS_MAX_WARMUP_REPLICAS_KEY = "max.warmup.replicas";
    public static final String STREAMS_ACCEPTABLE_RECOVERY_LAG_KEY = "acceptable.recovery.lag";
    public static final String METRICS_RECORDING_LEVEL_KEY = "metrics.recording.level";
    public static final Set<String> METRICS_RECORDING_LEVEL_VERBOSE_VALUES = Set.of("DEBUG", "TRACE");
    public static final String RECONNECT_BACKOFF_MAX_MS_KEY = "reconnect.backoff.max.ms";
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_KEY = "socket.connection.setup.timeout.ms";
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_KEY = "socket.connection.setup.timeout.max.ms";
    public static final String RETRY_BACKOFF_MAX_MS_KEY = "retry.backoff.max.ms";
    public static final String PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_KEY = "partitioner.adaptive.partitioning.enable";
    public static final String SASL_LOGIN_CONNECT_TIMEOUT_MS_KEY = "sasl.login.connect.timeout.ms";
    public static final String SASL_LOGIN_READ_TIMEOUT_MS_KEY = "sasl.login.read.timeout.ms";
    public static final String SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_KEY = "sasl.oauthbearer.token.endpoint.url";
    public static final String STREAMS_GLOBAL_CONSUMER_AUTO_OFFSET_RESET_KEY = "global.consumer.auto.offset.reset";
    public static final String STREAMS_RESTORE_CONSUMER_AUTO_OFFSET_RESET_KEY = "restore.consumer.auto.offset.reset";
    public static final String STREAMS_DEFAULT_DSL_STORE_KEY = "default.dsl.store";
    public static final String SSL_KEYSTORE_LOCATION_KEY = "ssl.keystore.location";
    public static final String SSL_TRUSTSTORE_LOCATION_KEY = "ssl.truststore.location";
    public static final String CLIENT_RACK_KEY = "client.rack";
    public static final Set<String> CONSUMER_AUTO_OFFSET_RESET_VALID_VALUES = Set.of("earliest", "latest", "none");
    public static final Set<String> PRODUCER_ACKS_VALID_VALUES = Set.of("0", "1", "-1", "all");
    public static final String STREAMS_PROBING_REBALANCE_INTERVAL_MS_KEY = "probing.rebalance.interval.ms";
    public static final String INTERCEPTOR_CLASSES_KEY = "interceptor.classes";
    /** Confluent legacy Control Center monitoring interceptors — replaced by Health+ since CP 7. */
    public static final Set<String> LEGACY_MONITORING_INTERCEPTOR_FQCNS = Set.of(
            "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor",
            "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor"
    );
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
    public static final String SSL_PROTOCOL_KEY = "ssl.protocol";
    public static final Set<String> SSL_PROTOCOL_LEGACY_VALUES = Set.of(
            "TLSv1", "TLSv1.0", "TLSv1.1", "SSLv2", "SSLv3", "SSL"
    );
    public static final String SSL_ENABLED_PROTOCOLS_KEY = "ssl.enabled.protocols";
    public static final String SSL_CIPHER_SUITES_KEY = "ssl.cipher.suites";
    public static final Set<String> SSL_CIPHER_SUITES_LEGACY_TOKENS = Set.of(
            "RC4", "MD5", "DES", "3DES", "NULL", "EXPORT", "ANON"
    );
    public static final String SR_BEARER_AUTH_TOKEN_KEY = "bearer.auth.token";
    public static final String EXCLUDE_INTERNAL_TOPICS_KEY = "exclude.internal.topics";
    public static final String PARTITION_ASSIGNMENT_STRATEGY_KEY = "partition.assignment.strategy";
    public static final Set<String> LEGACY_PARTITION_ASSIGNORS = Set.of(
            "org.apache.kafka.clients.consumer.RangeAssignor",
            "org.apache.kafka.clients.consumer.RoundRobinAssignor",
            "org.apache.kafka.clients.consumer.StickyAssignor"
    );
    public static final String COOPERATIVE_STICKY_ASSIGNOR_FQCN =
            "org.apache.kafka.clients.consumer.CooperativeStickyAssignor";
    public static final String SASL_MECHANISM_KEY = "sasl.mechanism";
    public static final String STREAMS_TOPOLOGY_OPTIMIZATION_KEY = "topology.optimization";
    public static final String SSL_KEYSTORE_TYPE_KEY = "ssl.keystore.type";
    public static final String CLIENT_ID_KEY = "client.id";
    public static final Set<String> KAFKA_GENERIC_CLIENT_IDS = Set.of(
            "client", "my-client", "kafka-client", "test", "test-client", "demo", "demo-client",
            "producer", "my-producer", "kafka-producer", "test-producer",
            "consumer", "my-consumer", "kafka-consumer", "test-consumer",
            "app", "application", "default", "tmp"
    );

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
    public static final String STREAMS_MAX_TASK_IDLE_MS_KEY = "max.task.idle.ms";
    public static final String STREAMS_DEFAULT_PRODUCTION_HANDLER_KEY = "default.production.exception.handler";
    public static final String STREAMS_ALWAYS_CONTINUE_PRODUCTION_HANDLER_FQCN = "org.apache.kafka.streams.errors.AlwaysContinueProductionExceptionHandler";
    public static final String STREAMS_NUM_STREAM_THREADS_KEY = "num.stream.threads";
    public static final String CONNECTIONS_MAX_IDLE_MS_KEY = "connections.max.idle.ms";
    public static final String MAX_PARTITION_FETCH_BYTES_KEY = "max.partition.fetch.bytes";
    public static final String BOOTSTRAP_SERVERS_KEY = "bootstrap.servers";
    public static final String STREAMS_DEFAULT_TIMESTAMP_EXTRACTOR_KEY = "default.timestamp.extractor";
    public static final String STREAMS_WALLCLOCK_TIMESTAMP_EXTRACTOR_FQCN =
            "org.apache.kafka.streams.processor.WallclockTimestampExtractor";
    public static final String GROUP_INSTANCE_ID_KEY = "group.instance.id";
    public static final String SEND_BUFFER_BYTES_KEY = "send.buffer.bytes";
    public static final String RECEIVE_BUFFER_BYTES_KEY = "receive.buffer.bytes";
    public static final String STREAMS_BUFFERED_RECORDS_PER_PARTITION_KEY = "buffered.records.per.partition";
    public static final String STREAMS_RACK_AWARE_ASSIGNMENT_STRATEGY_KEY = "rack.aware.assignment.strategy";

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
