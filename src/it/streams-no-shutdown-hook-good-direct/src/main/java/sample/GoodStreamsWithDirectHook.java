package sample;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Duration;
import java.util.Properties;

/**
 * Silent: KafkaStreams.start() is called AND
 * Runtime.getRuntime().addShutdownHook(Thread) is called directly (as
 * INVOKEVIRTUAL on java/lang/Runtime), so the project-scoped scan sets
 * hasHook=true and the rule returns no violations.
 *
 * The Thread is constructed with a Runnable lambda that calls
 * streams.close(Duration.ofSeconds(30)) — the canonical orderly-shutdown
 * shape. The rule deliberately does NOT verify what the hook body does
 * (cross-method body inspection through Thread constructors into Runnable
 * lambda bodies would multiply the false-negative surface without
 * meaningfully improving the signal). Any registered hook anywhere in the
 * project suppresses the rule — accepting that an unrelated shutdown hook
 * for a metrics flush would also suppress, in exchange for zero false
 * positives in apps that DO wire a streams-close hook.
 */
public final class GoodStreamsWithDirectHook {

    private static Properties streamsProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "good-direct-hook-app");
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

    public void startWithRegisteredHook() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close(Duration.ofSeconds(30))));
        streams.start();  // SILENT — addShutdownHook(...) is called above on the same instance
    }
}
