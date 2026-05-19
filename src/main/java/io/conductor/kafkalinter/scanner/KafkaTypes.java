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

    public static final Set<String> PRODUCER_OWNERS = Set.of(KAFKA_PRODUCER, PRODUCER_INTERFACE);
    public static final Set<String> CONSUMER_OWNERS = Set.of(KAFKA_CONSUMER, CONSUMER_INTERFACE);

    public static final String FUTURE = "java/util/concurrent/Future";
    public static final String DURATION = "java/time/Duration";
    public static final String PROPERTIES = "java/util/Properties";
    public static final String MAP = "java/util/Map";
    public static final String HASHMAP = "java/util/HashMap";

    public static final String COMPRESSION_TYPE_KEY = "compression.type";
    public static final String ENABLE_AUTO_COMMIT_KEY = "enable.auto.commit";
}
