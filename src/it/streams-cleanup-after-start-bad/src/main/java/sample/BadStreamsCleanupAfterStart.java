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
 * RULE: STREAMS_CLEANUP_AFTER_START.
 *
 * <p>Fires when {@code KafkaStreams.cleanUp()} is called on the same local
 * slot AFTER {@code start()} with no intervening {@code close()} in the same
 * method. {@code cleanUp()} is documented as legal only BEFORE {@code start()}
 * (instance is in CREATED) or AFTER {@code close()} (instance is in
 * NOT_RUNNING). Calling it on a running instance throws
 * {@code IllegalStateException("Cannot clean up while running.")} before any
 * state-store deletion happens — so the developer's intent ('reset state')
 * is silently defeated: state.dir on disk is unchanged.
 *
 * <p>The exception fires on the calling thread but is typically caught by a
 * generic try/catch around 'startup' or 'reset' code, logged as a single
 * 'failed to reset state' warning, and forgotten. The application continues
 * running on the OLD state stores; the bug the cleanUp() was meant to recover
 * from re-occurs on the next record; the application enters a thread-replace
 * or restart-loop with no visible signal that the reset was a no-op.
 *
 * <p>What this rule catches (four direct-call shapes + one method-reference
 * capture, all on the same slot in the same method):
 * <ol>
 *   <li>{@code cleanupAfterStart} — the canonical bug: {@code start()} then
 *       {@code cleanUp()} in sequence. The most common origin: a developer
 *       wires a 'reset state if previous run crashed' branch INSIDE the
 *       startup flow and types the calls in the wrong order.</li>
 *   <li>{@code restartFlowCleanupAfterRestart} — a 'restart' method that
 *       calls {@code close()}, then {@code start()} on the same instance (a
 *       no-op because start() is single-shot — see STREAMS_STARTED_TWICE for
 *       the orthogonal bug), then {@code cleanUp()} thinking it will reset
 *       the freshly-restarted state. After the close(), the slot is dropped
 *       from startedSlots; the start() puts it back; the cleanUp() fires.</li>
 *   <li>{@code conditionalCleanupAfterStart} — {@code cleanUp()} guarded by
 *       a boolean parameter, placed after {@code start()}. The rule fires
 *       regardless of the guard because the bytecode signal is the call
 *       site; the guard does not change the call's runtime semantics when it
 *       does execute.</li>
 *   <li>{@code dualCleanupAfterStart} — two {@code cleanUp()} calls after a
 *       single {@code start()} (the second one a 'safety net'). Both fire
 *       at runtime; the rule reports both call sites.</li>
 *   <li>{@code captureCleanupAfterStart} — method-reference
 *       {@code streams::cleanUp} captured AFTER {@code start()} and passed
 *       to {@code Executor.execute}. The capturing INVOKEDYNAMIC bsm-args
 *       hold a {@code REF_invokeVirtual} handle to
 *       {@code KafkaStreams.cleanUp()V} and the captured streams reference;
 *       when the executor later runs the Runnable, the instance is RUNNING
 *       and cleanUp() throws on the worker thread, with the executor's
 *       default uncaught-handler swallowing the exception. The rule fires
 *       at the indy site, where the static signal is.</li>
 * </ol>
 *
 * <p>Correct pattern: every {@code cleanUp()} call must be BEFORE
 * {@code start()} (canonical pre-start reset) or AFTER {@code close()}
 * (managed reset before reconstruct). The
 * {@code GoodStreamsCleanupOrdered} silent controls cover the canonical
 * correct shapes.
 */
public final class BadStreamsCleanupAfterStart {

    private static final String BROKERS = "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092";

    private static Properties streamsProps(String appId) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.CLIENT_ID_CONFIG, "cleanup-after-start-" + appId);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return p;
    }

    private static StreamsBuilder topology(String appId) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("in-" + appId).to("out-" + appId);
        return builder;
    }

    /**
     * Anti-pattern #1: cleanUp() called immediately after start() in the
     * same method. The most common origin shape: a developer wires a
     * 'reset state if previous run crashed' branch INSIDE the startup flow
     * and types the calls in the wrong order — they typed {@code start} then
     * {@code cleanUp} thinking 'start it up, then wipe state' would do what
     * they want. The runtime semantics are the opposite: cleanUp() must be
     * called BEFORE start (or after close).
     */
    public void cleanupAfterStart() {
        KafkaStreams streams = new KafkaStreams(topology("bad-cleanup-after").build(),
                streamsProps("bad-cleanup-after-start"));
        try {
            streams.start();
            streams.cleanUp(); // FIRES — cleanUp on a running instance throws
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #2: a 'restart' flow that calls close(), then start() on
     * the SAME instance (orthogonal bug — see STREAMS_STARTED_TWICE), then
     * cleanUp() expecting to reset the freshly-restarted state. After
     * close(), the slot leaves startedSlots; start() re-adds it; the
     * cleanUp() on the still-running slot fires.
     */
    public void restartFlowCleanupAfterRestart() {
        KafkaStreams streams = new KafkaStreams(topology("bad-restart-cleanup").build(),
                streamsProps("bad-restart-flow-cleanup-after-restart"));
        try {
            streams.start();
            streams.close(Duration.ofSeconds(10));
            streams.start(); // throws ISE at runtime (STREAMS_STARTED_TWICE — silenced in IT pom)
            streams.cleanUp(); // FIRES — cleanUp on a started slot (re-added by the second start)
        } catch (Exception ignored) {
        }
    }

    /**
     * Anti-pattern #3: cleanUp() inside a conditional branch placed after
     * start(). The rule fires regardless of the guard because the static
     * bytecode signal is the call site location, not whether it can be
     * proven to execute. At runtime, when the guard is satisfied, the call
     * runs and throws.
     */
    public void conditionalCleanupAfterStart(boolean resetState) {
        KafkaStreams streams = new KafkaStreams(topology("bad-conditional").build(),
                streamsProps("bad-conditional-cleanup-after-start"));
        try {
            streams.start();
            if (resetState) {
                streams.cleanUp(); // FIRES — cleanUp on a running instance throws
            }
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #4: two cleanUp() calls after a single start(). The
     * second is sometimes added as a 'safety net' (typical comment:
     * 'second clean-up in case the first didn't take effect'). Both fire
     * because the slot stays in startedSlots between the two calls (no
     * close() in between). At runtime, both throw IllegalStateException.
     */
    public void dualCleanupAfterStart() {
        KafkaStreams streams = new KafkaStreams(topology("bad-dual").build(),
                streamsProps("bad-dual-cleanup-after-start"));
        try {
            streams.start();
            streams.cleanUp(); // FIRES — cleanUp #1 on a running instance
            streams.cleanUp(); // FIRES — cleanUp #2 on a still-running instance
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #5: bound method-reference {@code streams::cleanUp}
     * captured AFTER start() and passed to {@code Executor.execute(Runnable)}.
     * The capturing INVOKEDYNAMIC bsm-args hold a {@code REF_invokeVirtual}
     * handle to {@code KafkaStreams.cleanUp()V} and the captured
     * {@code streams} reference. When the executor later picks up the
     * Runnable on a worker thread, the instance is still RUNNING (no
     * close() happened in this method before the capture), cleanUp()
     * throws on the worker thread, and the executor's default
     * uncaught-handler swallows the exception. The 'scheduled state-store
     * maintenance' is a silent no-op for the lifetime of the process.
     */
    public void captureCleanupAfterStart(Executor executor) {
        KafkaStreams streams = new KafkaStreams(topology("bad-capture").build(),
                streamsProps("bad-capture-cleanup-after-start"));
        try {
            streams.start();
            executor.execute(streams::cleanUp); // FIRES — ::cleanUp captured after start
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }
}
