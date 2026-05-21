package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

/**
 * RULE: STREAMS_NOT_CLOSED.
 *
 * Each method below constructs a {@link KafkaStreams} instance, uses it (start()),
 * and then exits without calling close() on the same local slot. JVM exit without
 * an orderly close kills the StreamThreads mid-batch, never releases the consumer-
 * group membership, and the broker waits session.timeout.ms before reassigning
 * the partitions — downstream consumers see lag spike on every restart.
 *
 * Three fires expected; a control method at the bottom closes its slot via
 * try-with-resources and must NOT fire.
 */
public final class BadStreamsNotClosed {

    private static Properties streamsProps(String appId) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
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

    public void firstSiteStartNoClose() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps("not-closed-app-1"));
        streams.start();   // FIRES — start() used, close() never called on this slot
    }

    public void secondSiteWithSetupNoClose() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps("not-closed-app-2"));
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.ERROR) {
                System.exit(1);
            }
        });
        streams.start();   // FIRES — start()/setStateListener used, close() never called
    }

    public void thirdSiteCleanUpButNoClose() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps("not-closed-app-3"));
        streams.cleanUp();
        streams.start();   // FIRES — cleanUp()/start() used, close() never called
    }

    /** Control: try-with-resources closes via javac-emitted synthetic close() — must NOT fire. */
    public void controlTryWithResourcesCloses() {
        try (KafkaStreams streams = new KafkaStreams(topology(), streamsProps("not-closed-app-control"))) {
            streams.start();
        }
    }
}
