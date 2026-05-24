package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.util.Properties;

/**
 * Silent: KafkaStreams.start() is called AND KafkaStreams.setUncaughtExceptionHandler(...)
 * is called directly as INVOKEVIRTUAL on org/apache/kafka/streams/KafkaStreams,
 * so the project-scoped scan sets hasHandler=true and the rule returns no
 * violations.
 *
 * <p>This is the canonical KIP-671 shape: register a
 * {@link StreamsUncaughtExceptionHandler} that returns one of the three
 * {@code StreamThreadExceptionResponse} enum values before calling
 * {@code start()}. The handler picked here returns {@code REPLACE_THREAD}, which
 * is the right default for most apps — transient errors (broker hiccup,
 * intermittent network blip, occasional poison record after a deserializer
 * upgrade) get a fresh StreamThread on the same instance, and the application
 * keeps processing. For state-store corruption or other instance-local
 * non-recoverables, {@code SHUTDOWN_CLIENT} would force the KafkaStreams client
 * to ERROR and let a state listener call {@code System.exit(1)} so Kubernetes
 * restarts the pod (triggering a clean rebalance to healthy peers). For
 * topology-wide poison (a schema-incompatible upstream that every instance will
 * hit), {@code SHUTDOWN_APPLICATION} coordinates a multi-instance shutdown.
 *
 * <p>The rule deliberately does NOT inspect WHICH enum the handler returns —
 * picking the right response is a topology-specific decision. The rule's only
 * job is to ensure that SOME handler is registered; the project-scoped scan
 * walks every {@code .class} file for any {@code setUncaughtExceptionHandler}
 * call (any descriptor — both legacy {@code Thread.UncaughtExceptionHandler}
 * and modern {@code StreamsUncaughtExceptionHandler} overloads are accepted).
 */
public final class GoodStreamsWithDirectHandler {

    private static Properties streamsProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "good-direct-handler-app");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        return p;
    }

    private static Topology topology() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("orders").mapValues(v -> v.toUpperCase()).to("orders-upper");
        return b.build();
    }

    public void startWithRegisteredHandler() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());
        streams.setUncaughtExceptionHandler(
                (StreamsUncaughtExceptionHandler) exception ->
                        StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD);
        streams.start();  // SILENT — setUncaughtExceptionHandler(...) registered above on the same instance
    }
}
