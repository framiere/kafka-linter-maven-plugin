package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Control fixture: each method below constructs a Kafka client locally (so the
 * {@code *NotClosed} rules track it), then registers close via a JVM shutdown
 * hook using either a method reference ({@code client::close}) or a lambda
 * body ({@code () -> client.close()}).
 *
 * <p>The method reference is compiled by javac to
 * {@code ALOAD slot; DUP; INVOKESTATIC Objects.requireNonNull; POP; INVOKEDYNAMIC}
 * — the captured slot escapes through the indy bootstrap call. The lambda body
 * is compiled to a synthetic {@code lambda$N} method that the outer scan never
 * looks inside; the slot escapes via the indy capture in the outer method.
 *
 * <p>Either way, the {@code *NotClosed} rules must treat the slot as ESCAPED
 * (another path may close it) and stay silent. The fixture asserts that
 * behavior structurally: the build is expected to succeed with zero
 * error-severity violations from PRODUCER_NOT_CLOSED, CONSUMER_NOT_CLOSED,
 * ADMIN_NOT_CLOSED, or STREAMS_NOT_CLOSED.
 *
 * <p>This is the control case for the false-positive that the
 * {@code AsmUtil.indyCapturedSlots} escape detection fixes.
 */
public final class GoodShutdownHookClose {

    private static Properties producerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092");
        p.put("client.id", "shutdown-hook-producer");
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("acks", "all");
        p.put("enable.idempotence", "true");
        p.put("compression.type", "lz4");
        return p;
    }

    private static Properties consumerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092");
        p.put("client.id", "shutdown-hook-consumer");
        p.put("group.id", "shutdown-hook-group");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("enable.auto.commit", "false");
        return p;
    }

    private static Properties adminProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092");
        p.put("client.id", "shutdown-hook-admin");
        return p;
    }

    private static Properties streamsProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "shutdown-hook-streams");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        return p;
    }

    /** Method reference close on a method-local producer — must NOT fire PRODUCER_NOT_CLOSED. */
    public void producerMethodRefClose(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps());
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
        producer.send(record);
    }

    /** Lambda body close on a method-local producer — must NOT fire PRODUCER_NOT_CLOSED. */
    public void producerLambdaClose(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> producer.close(Duration.ofSeconds(20))));
        producer.send(record);
    }

    /** Method reference close on a method-local consumer — must NOT fire CONSUMER_NOT_CLOSED. */
    public void consumerMethodRefClose() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps());
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));
        consumer.subscribe(List.of("t"));
        consumer.poll(Duration.ofSeconds(1));
    }

    /** Lambda body close on a method-local consumer — must NOT fire CONSUMER_NOT_CLOSED. */
    public void consumerLambdaClose() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> consumer.close(Duration.ofSeconds(20))));
        consumer.subscribe(List.of("t"));
        consumer.poll(Duration.ofSeconds(1));
    }

    /** Method reference close on a method-local admin client — must NOT fire ADMIN_NOT_CLOSED. */
    public void adminMethodRefClose() {
        Admin admin = AdminClient.create(adminProps());
        Runtime.getRuntime().addShutdownHook(new Thread(admin::close));
        admin.describeCluster();
    }

    /** Lambda body close on a method-local admin client — must NOT fire ADMIN_NOT_CLOSED. */
    public void adminLambdaClose() {
        Admin admin = AdminClient.create(adminProps());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> admin.close(Duration.ofSeconds(20))));
        admin.describeCluster();
    }

    private static Topology topology() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in").to("out");
        return b.build();
    }

    /** Method reference close on a method-local KafkaStreams — must NOT fire STREAMS_NOT_CLOSED. */
    public void streamsMethodRefClose() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }

    /** Lambda body close on a method-local KafkaStreams — must NOT fire STREAMS_NOT_CLOSED. */
    public void streamsLambdaClose() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close(Duration.ofSeconds(20))));
        streams.start();
    }
}
