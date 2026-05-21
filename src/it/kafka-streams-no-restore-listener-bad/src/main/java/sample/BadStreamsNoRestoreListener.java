package sample;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;

/**
 * RULE: STREAMS_NO_GLOBAL_STATE_RESTORE_LISTENER.
 *
 * This project registers BOTH a {@link StreamsUncaughtExceptionHandler}
 * (silencing STREAMS_NO_UNCAUGHT_EXCEPTION_HANDLER) AND a
 * {@link KafkaStreams.StateListener} (silencing STREAMS_NO_STATE_LISTENER),
 * but NEVER calls {@link KafkaStreams#setGlobalStateRestoreListener}.
 *
 * Without a restore listener, operators have no in-process metric for how
 * far through state-store restoration the application is. On a 200 GB
 * RocksDB store, restoration can take 25 minutes; the app sits in
 * REBALANCING / RESTORING the whole time with no operator-visible signal.
 *
 * Rule is project-scoped: walks target/classes for KafkaStreams.start() and
 * setGlobalStateRestoreListener(...) sites. Two start() sites here → two
 * violations.
 */
public final class BadStreamsNoRestoreListener {

    private static Properties streamsProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "no-restore-listener-app");
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

    public void startWithHandlerAndListenerButNoRestoreListener() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());
        streams.setUncaughtExceptionHandler(
                (StreamsUncaughtExceptionHandler) e ->
                        StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD);
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.ERROR) {
                System.exit(1);
            }
        });
        streams.start();  // FIRES — handler + state listener set, but no restore listener anywhere
    }

    public void secondStartSiteStillNoRestoreListener() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());
        streams.setUncaughtExceptionHandler(
                (StreamsUncaughtExceptionHandler) e ->
                        StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
        streams.setStateListener((newState, oldState) -> {});
        streams.start();  // FIRES — second site
    }
}
