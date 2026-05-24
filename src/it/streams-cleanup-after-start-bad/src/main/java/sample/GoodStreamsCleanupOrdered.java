package sample;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Control for STREAMS_CLEANUP_AFTER_START.
 *
 * <p>Six methods that each AVOID the rule by following one of the two
 * documented-legal windows for {@code cleanUp()}: BEFORE {@code start()}
 * (instance still in CREATED), or AFTER {@code close()} (instance in
 * NOT_RUNNING). Any cleanUp() call in either window is silent for our rule.
 *
 * <ol>
 *   <li>{@code cleanupBeforeStart} — the canonical pre-start reset shape:
 *       cleanUp(), then start(). The slot is not in startedSlots when the
 *       cleanUp() is checked, so the rule does not fire.</li>
 *   <li>{@code cleanupAfterClose} — the managed reset window: start(),
 *       close(), cleanUp(). The close() removes the slot from
 *       startedSlots, so the cleanUp() check sees an empty/non-matching
 *       slot set.</li>
 *   <li>{@code closeAndReconstructWithCleanup} — the full 'reset state and
 *       restart' flow: close the running instance, cleanUp on it, then
 *       construct a fresh KafkaStreams and start it. Both legal windows
 *       cooperate.</li>
 *   <li>{@code cleanupBeforeStartTwice} — two cleanUp() calls before
 *       start(); both legal (slot not yet started).</li>
 *   <li>{@code captureCleanupBeforeStart} — method-reference
 *       {@code streams::cleanUp} captured BEFORE start() and passed to an
 *       Executor. The capture site sees the slot not yet in startedSlots,
 *       so the rule does not fire. (At runtime, whether the executor
 *       actually runs the Runnable before start() is a separate question
 *       — the static signal is the capture-site ordering, which is legal.)</li>
 *   <li>{@code captureCleanupAfterClose} — method-reference
 *       {@code streams::cleanUp} captured AFTER close(). The slot is no
 *       longer in startedSlots (the close removed it), so the rule does
 *       not fire.</li>
 * </ol>
 */
public final class GoodStreamsCleanupOrdered {

    private static final String BROKERS = "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092";

    private static Properties streamsProps(String appId) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.CLIENT_ID_CONFIG, "good-cleanup-" + appId);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return p;
    }

    private static StreamsBuilder topology(String appId) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("in-" + appId).to("out-" + appId);
        return builder;
    }

    /** Correct: cleanUp() BEFORE start() — canonical pre-start reset (CREATED state). */
    public void cleanupBeforeStart() {
        KafkaStreams streams = new KafkaStreams(topology("good-pre-start").build(),
                streamsProps("good-cleanup-before-start"));
        try {
            streams.cleanUp();
            streams.start();
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: cleanUp() AFTER close() — managed-reset window (NOT_RUNNING state). */
    public void cleanupAfterClose() {
        KafkaStreams streams = new KafkaStreams(topology("good-post-close").build(),
                streamsProps("good-cleanup-after-close"));
        streams.start();
        streams.close(Duration.ofSeconds(10));
        streams.cleanUp();
    }

    /**
     * Correct: the full reset-and-restart flow. close() the running instance,
     * cleanUp() on it (legal — NOT_RUNNING), then construct a FRESH KafkaStreams
     * (note: the cleanUp is on the OLD instance, the start is on the NEW
     * instance — those are different slots in this method, so neither call
     * fires the rule).
     */
    public void closeAndReconstructWithCleanup() {
        KafkaStreams oldStreams = new KafkaStreams(topology("good-old").build(),
                streamsProps("good-old-instance"));
        oldStreams.start();
        oldStreams.close(Duration.ofSeconds(10));
        oldStreams.cleanUp(); // legal — oldStreams is in NOT_RUNNING

        KafkaStreams newStreams = new KafkaStreams(topology("good-new").build(),
                streamsProps("good-new-instance"));
        try {
            newStreams.start();
        } finally {
            newStreams.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: two cleanUp() calls before start; both legal (CREATED state both times). */
    public void cleanupBeforeStartTwice() {
        KafkaStreams streams = new KafkaStreams(topology("good-double-pre").build(),
                streamsProps("good-cleanup-before-start-twice"));
        try {
            streams.cleanUp();
            streams.cleanUp();
            streams.start();
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: method-reference ::cleanUp captured BEFORE start (slot not yet in startedSlots). */
    public void captureCleanupBeforeStart(Executor executor) {
        KafkaStreams streams = new KafkaStreams(topology("good-capture-before").build(),
                streamsProps("good-capture-cleanup-before-start"));
        try {
            executor.execute(streams::cleanUp);
            streams.start();
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: method-reference ::cleanUp captured AFTER close (slot removed from startedSlots). */
    public void captureCleanupAfterClose(Executor executor) {
        KafkaStreams streams = new KafkaStreams(topology("good-capture-after-close").build(),
                streamsProps("good-capture-cleanup-after-close"));
        streams.start();
        streams.close(Duration.ofSeconds(10));
        executor.execute(streams::cleanUp);
    }
}
