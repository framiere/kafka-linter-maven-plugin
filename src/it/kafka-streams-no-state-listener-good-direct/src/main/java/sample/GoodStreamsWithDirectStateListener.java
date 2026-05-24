package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

/**
 * Silent: KafkaStreams.start() is called AND a StateListener is wired via a
 * direct INVOKEVIRTUAL on KafkaStreams.setStateListener(...). This is the
 * canonical, straight-line shape — the user-class bytecode at the registration
 * site contains a {@code MethodInsnNode} with
 * {@code owner=org/apache/kafka/streams/KafkaStreams},
 * {@code name=setStateListener},
 * {@code desc=(Lorg/apache/kafka/streams/KafkaStreams$StateListener;)V}, which
 * the project-walk picks up trivially and flips {@code hasListener=true}.
 *
 * <p><strong>What this controls for:</strong> proves the rule does NOT fire when
 * a listener exists in the project — i.e. that {@code hasListener} suppression
 * works end-to-end. Without this control, the bad fixture alone cannot
 * distinguish "rule fires correctly" from "rule fires unconditionally on any
 * KafkaStreams.start() it sees". The pair (bad → 2 errors, good-direct → 0)
 * pins the boolean behaviour.
 *
 * <p><strong>Why System.exit(1) on ERROR is the canonical body:</strong> when a
 * Streams client transitions to {@code ERROR} (every {@code StreamThread} died,
 * or the uncaught-exception handler returned {@code SHUTDOWN_CLIENT}), the JVM
 * keeps running with the consumer-group membership held — the broker still
 * counts this dead client as alive and refuses to rebalance its partitions to a
 * healthy peer until the session times out (default 45 s, often raised to
 * minutes in production). Calling {@code System.exit(1)} on ERROR forces the
 * process to die, the TCP socket to the broker closes immediately, the group
 * coordinator notices the missing heartbeat on the next poll, and the partitions
 * rebalance away on a sub-second deadline. Kubernetes / systemd then restarts
 * the pod from a clean slate (with whatever backoff policy the operator
 * configured). The alternative — staying alive in a broken state — looks like
 * "service up, throughput zero" on dashboards and is one of the highest-impact
 * stream-processing outages possible.
 */
public final class GoodStreamsWithDirectStateListener {

    private static Properties streamsProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "good-direct-state-listener-app");
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
        b.<String, String>stream("orders").to("orders-mirror");
        return b.build();
    }

    public void startWithRegisteredStateListener() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.ERROR) {
                System.exit(1);  // canonical safe default — let the supervisor restart us
            }
        });
        streams.start();  // SILENT — direct INVOKEVIRTUAL setStateListener wired in this project
    }
}
