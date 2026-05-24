package sample;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 * RULE: STREAMS_STARTED_TWICE.
 *
 * <p>Fires when {@code KafkaStreams.start()} is invoked more than once on
 * the same local slot in the same method. The Streams state-machine
 * documents {@code start()} as exactly-once: the call atomically transitions
 * the instance from {@code CREATED} to {@code REBALANCING}, and the internal
 * {@code setState(...)} call throws {@code IllegalStateException("Streams is
 * not in CREATED state, cannot start.")} if the current state is anything
 * other than {@code CREATED} at entry. There is NO documented path back to
 * {@code CREATED} from any other state — not from {@code RUNNING}, not from
 * {@code ERROR}, not even from {@code NOT_RUNNING} after {@code close()}.
 *
 * <p>The bug shape is uniquely common in Streams code because the
 * asynchronous nature of {@code start()} defeats the developer's mental
 * model. {@code start()} returns immediately (the broker handshake and
 * partition assignment happen on background StreamThreads, AFTER the call
 * returns), so developers reach for defensive patterns — retry loops,
 * watchdog timers, idempotent-init guards — that all assume start can be
 * retried. None of them work, and most actively break the working
 * application by triggering the {@code IllegalStateException} and confusing
 * the operator into a destructive recovery action.
 *
 * <p>What this rule catches:
 * <ol>
 *   <li>{@code straightDoubleStart} — two literal {@code streams.start()}
 *       calls on the same slot back-to-back. The classic shape after a
 *       refactor that moved init code into a method and forgot to remove
 *       the old start.</li>
 *   <li>{@code startThenRetryStart} — {@code start()} followed by a
 *       'fallback' second {@code start()} that's intended to recover from a
 *       perceived failure. The first call succeeded; the second throws
 *       because the instance has already left {@code CREATED}.</li>
 *   <li>{@code startThenCaptureStartReference} — {@code start()} followed
 *       by capturing {@code streams::start} as a method reference passed to
 *       an executor (the 'safety net restart' pattern). When the executor
 *       runs the captured reference, the second invocation throws.</li>
 * </ol>
 *
 * <p>Correct pattern: call {@code start()} exactly once per
 * {@code KafkaStreams} instance. If you need to restart, you must construct
 * a fresh instance after {@code close()} on the old one; the
 * {@code GoodStreamsStartedOnce} silent controls cover this shape.
 */
public final class BadStreamsStartedTwice {

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

    /** Anti-pattern: two literal start() calls back-to-back — FIRES. */
    public void straightDoubleStart() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps("bad-double-start"));
        try {
            streams.start();
            streams.start(); // FIRES — second start on same slot
        } finally {
            streams.close(Duration.ofSeconds(30));
        }
    }

    /** Anti-pattern: defensive 'retry' second start() — FIRES. */
    public void startThenRetryStart() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps("bad-retry-start"));
        try {
            streams.start();
            // Misguided 'idempotent restart' — start() returns immediately
            // (broker handshake is async); the developer reads the early
            // return as 'success' and adds a defensive second call.
            streams.start(); // FIRES — IllegalStateException at runtime
        } finally {
            streams.close(Duration.ofSeconds(30));
        }
    }

    /** Anti-pattern: start() then executor.submit(streams::start) — FIRES on the indy capture. */
    public void startThenCaptureStartReference() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps("bad-start-capture"));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            streams.start();
            // 'Safety net restart' — when the executor runs streams::start,
            // the second invocation throws IllegalStateException; the
            // executor's uncaught-handler typically swallows it.
            executor.submit(streams::start); // FIRES — ::start captured after start()
        } finally {
            executor.shutdownNow();
            streams.close(Duration.ofSeconds(30));
        }
    }
}
