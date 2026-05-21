package sample;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;

/**
 * RULE: STREAMS_NO_STATE_LISTENER.
 *
 * This project DOES register a {@link StreamsUncaughtExceptionHandler} (so the
 * sibling rule STREAMS_NO_UNCAUGHT_EXCEPTION_HANDLER stays silent), but it
 * NEVER calls {@link KafkaStreams#setStateListener(KafkaStreams.StateListener)}.
 *
 * Without a state listener, when the Streams client transitions to ERROR
 * (every StreamThread died, or the uncaught handler returned SHUTDOWN_CLIENT),
 * the application has no in-process hook to call System.exit(1) — the JVM keeps
 * running, holds the consumer-group membership, and starves the cluster from
 * rebalancing the partitions to a healthy peer.
 *
 * The rule is project-scoped: walks target/classes for KafkaStreams.start()
 * and setStateListener(...) sites. Every start() site fires when no listener
 * is set anywhere in the project. Two start() sites in this file → two
 * violations.
 */
public final class BadStreamsNoStateListener {

    private static Properties streamsProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "no-state-listener-app");
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

    public void startWithHandlerButNoListener() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());
        streams.setUncaughtExceptionHandler(
                (StreamsUncaughtExceptionHandler) e ->
                        StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD);
        streams.start();  // FIRES — handler set, but no setStateListener() anywhere
    }

    public void secondStartSiteStillNoListener() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());
        streams.setUncaughtExceptionHandler(
                (StreamsUncaughtExceptionHandler) e ->
                        StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
        streams.start();  // FIRES — second site
    }
}
