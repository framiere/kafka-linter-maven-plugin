package sample;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;

/**
 * RULE: STREAMS_NO_UNCAUGHT_EXCEPTION_HANDLER.
 *
 * KafkaStreams.start() is called in this project but
 * KafkaStreams.setUncaughtExceptionHandler(...) is never called anywhere.
 *
 * Without an explicit handler, a StreamThread that dies on an unrecoverable
 * exception (deserialization error, processor exception, ProducerFencedException)
 * is NOT replaced. The KafkaStreams client only transitions to ERROR after the
 * LAST StreamThread dies. With num.stream.threads > 1, the app sits in
 * RUNNING with reduced parallelism — silent throughput degradation, no
 * exception, no metric to alarm on.
 *
 * The rule is project-scoped: walks target/classes for KafkaStreams.start()
 * and setUncaughtExceptionHandler(...) sites. Every start() site fires when
 * no handler is set anywhere in the project. Two start() sites in this file,
 * one across the two methods → two violations.
 */
public final class BadStreamsNoUncaughtHandler {

    private static Properties streamsProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "no-handler-app");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        return p;
    }

    private static Topology topology() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in").to("out");
        return b.build();
    }

    public void startWithoutHandler() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());
        streams.start();  // FIRES — no setUncaughtExceptionHandler() anywhere in this project
    }

    public void startAgainElsewhereWithoutHandler() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());
        streams.start();  // FIRES — second site
    }
}
