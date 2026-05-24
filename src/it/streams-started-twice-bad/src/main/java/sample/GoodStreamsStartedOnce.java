package sample;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 * Control for STREAMS_STARTED_TWICE.
 *
 * <p>Three methods that each avoid the rule by following the only correct
 * shape: call {@code start()} EXACTLY ONCE per {@code KafkaStreams}
 * instance.
 *
 * <ol>
 *   <li>{@code singleStartOnly} — the canonical shape: construct, start
 *       once, close in finally.</li>
 *   <li>{@code closeThenFreshInstanceStart} — the documented restart shape:
 *       close the old instance, construct a NEW one in a DIFFERENT local
 *       slot, start the new one. Each slot is started exactly once.</li>
 *   <li>{@code startThenCleanUpWithoutRestart} — {@code start()} followed
 *       by {@code cleanUp()} but NO second {@code start()}. cleanUp() is
 *       safe (it does not restart); the slot is started exactly once.</li>
 * </ol>
 */
public final class GoodStreamsStartedOnce {

    private static final String BROKERS = "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092";

    private static Properties streamsProps(String applicationId) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "3");
        p.put(StreamsConfig.STATE_DIR_CONFIG, "/var/lib/streams/" + applicationId);
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        return p;
    }

    private static Topology topology() {
        StreamsBuilder b = new StreamsBuilder();
        b.stream("in").to("out");
        return b.build();
    }

    /** Correct: construct, start once, close in finally. */
    public void singleStartOnly() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps("good-single-start"));
        try {
            streams.start();
        } finally {
            streams.close(Duration.ofSeconds(30));
        }
    }

    /** Correct: close old, construct fresh in a DIFFERENT slot, start the new one. */
    public void closeThenFreshInstanceStart() {
        KafkaStreams oldStreams = new KafkaStreams(topology(), streamsProps("good-restart-old"));
        oldStreams.start();
        oldStreams.close(Duration.ofSeconds(30));

        // Documented restart shape: construct a brand-new instance.
        // The new slot is started exactly once; no second start() on the
        // old slot, and the rule never sees a duplicate start on any slot.
        KafkaStreams freshStreams = new KafkaStreams(topology(), streamsProps("good-restart-fresh"));
        try {
            freshStreams.start();
        } finally {
            freshStreams.close(Duration.ofSeconds(30));
        }
    }

    /** Correct: start() then cleanUp() (NOT start) — single start. */
    public void startThenCleanUpWithoutRestart() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps("good-start-cleanup"));
        try {
            streams.start();
        } finally {
            streams.close(Duration.ofSeconds(30));
            // cleanUp() is safe post-close (documented as NOT_RUNNING-safe);
            // no second start() — the slot is started exactly once.
            streams.cleanUp();
        }
    }
}
