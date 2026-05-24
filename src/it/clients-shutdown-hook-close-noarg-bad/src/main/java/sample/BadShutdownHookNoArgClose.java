package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

/**
 * Anti-pattern fixture: shutdown-hook close via the no-argument {@code close()}
 * overload, captured as a method reference.
 *
 * <p>The pattern below is dangerously common in production code:
 *
 * <pre>{@code
 *   Runtime.getRuntime().addShutdownHook(new Thread(client::close));
 * }</pre>
 *
 * <p>It looks like a clean idiom, but {@code client::close} captures the
 * <em>no-argument</em> {@code close()} overload — the one that delegates to
 * {@code close(Duration.ofMillis(Long.MAX_VALUE))} (or
 * {@code defaultApiTimeoutMs} on Consumer). When the shutdown thread runs, it
 * parks for as long as the in-flight broker state allows — minutes in the
 * worst case — long after Kubernetes has stopped waiting and dispatched
 * SIGKILL. In-flight EOS transactions are torn mid-commit, LeaveGroup
 * requests never arrive, and rebalances stall on
 * {@code session.timeout.ms} instead of triggering promptly.
 *
 * <p>The correct shape is the bounded overload — passed either via a lambda
 * body or via a wrapper:
 *
 * <pre>{@code
 *   Runtime.getRuntime().addShutdownHook(
 *       new Thread(() -> client.close(Duration.ofSeconds(20))));
 * }</pre>
 *
 * <h2>What makes this a tricky linting target</h2>
 *
 * javac compiles {@code client::close} to an {@code INVOKEDYNAMIC} whose
 * bootstrap-method arguments include a direct {@code REF_invokeVirtual}
 * (or {@code REF_invokeInterface}) handle pointing at the target method.
 * <strong>The outer method's bytecode contains zero
 * {@code INVOKE*} instructions for the {@code close()} call.</strong> A
 * naive {@code MethodInsnNode} scan misses the callsite entirely. The
 * {@code *_CLOSE_NO_TIMEOUT} rules must additionally inspect the indy
 * bootstrap-method arg list and match {@code owner.close:()V} handles —
 * that recovery is what this fixture asserts.
 *
 * <p>The fixture has four methods (one per client type). The build is
 * expected to fail with exactly four error-severity violations:
 *
 * <ul>
 *   <li>PRODUCER_CLOSE_NO_TIMEOUT</li>
 *   <li>CONSUMER_CLOSE_NO_TIMEOUT</li>
 *   <li>ADMIN_CLOSE_NO_TIMEOUT</li>
 *   <li>STREAMS_CLOSE_NO_TIMEOUT</li>
 * </ul>
 */
public final class BadShutdownHookNoArgClose {

    private static Properties producerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092");
        p.put("client.id", "bad-no-arg-close-producer");
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return p;
    }

    private static Properties consumerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092");
        p.put("client.id", "bad-no-arg-close-consumer");
        p.put("group.id", "bad-no-arg-close-group");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return p;
    }

    private static Properties adminProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092");
        p.put("client.id", "bad-no-arg-close-admin");
        return p;
    }

    private static Properties streamsProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "bad-no-arg-close-streams");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        return p;
    }

    /** FIRES PRODUCER_CLOSE_NO_TIMEOUT — method ref to KafkaProducer.close()V. */
    public void producerNoArgClose() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps());
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
    }

    /** FIRES CONSUMER_CLOSE_NO_TIMEOUT — method ref to KafkaConsumer.close()V. */
    public void consumerNoArgClose() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps());
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));
    }

    /** FIRES ADMIN_CLOSE_NO_TIMEOUT — method ref to Admin.close()V (REF_invokeInterface). */
    public void adminNoArgClose() {
        Admin admin = AdminClient.create(adminProps());
        Runtime.getRuntime().addShutdownHook(new Thread(admin::close));
    }

    private static Topology topology() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in").to("out");
        return b.build();
    }

    /** FIRES STREAMS_CLOSE_NO_TIMEOUT — method ref to KafkaStreams.close()V. */
    public void streamsNoArgClose() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
