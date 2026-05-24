package sample;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StandbyUpdateListener;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.TaskId;

/**
 * RULE: STREAMS_SET_LISTENER_AFTER_START.
 *
 * <p>Fires when {@code KafkaStreams.setStateListener},
 * {@code setUncaughtExceptionHandler},
 * {@code setGlobalStateRestoreListener}, or
 * {@code setStandbyUpdateListener} is called AFTER {@code start()} on the
 * same local slot in the same method. All four setters are
 * CREATED-state-only by contract; once {@code start()} has transitioned the
 * instance out of CREATED, the setter throws
 * {@code IllegalStateException("Can only set <listener> in CREATED state.
 * Current state is <state>.")} before storing the reference.
 *
 * <p>The bug shape is structurally common in code that grew incrementally:
 * a monitoring requirement landed mid-sprint and a developer added a
 * listener-setter call in a {@code @PostConstruct} or in an HTTP handler
 * that runs after the streams instance is already running. The application
 * keeps processing records, the setter exception is caught by a generic
 * try/catch around 'init' or 'admin' code, logged as a single warning, and
 * the listener is never registered. The runtime symptom is the absence of
 * the listener's observability — state transitions invisible, uncaught
 * exceptions unhandled, restore progress unreported — for the lifetime of
 * the process.
 *
 * <p>What this rule catches (five method-scope shapes, one per setter +
 * one method-reference capture variant):
 * <ol>
 *   <li>{@code stateListenerAfterStart} — start(), then setStateListener.
 *       The state listener is never installed; downstream alerting on
 *       state==ERROR never fires.</li>
 *   <li>{@code uncaughtExceptionHandlerAfterStart} — start(), then
 *       setUncaughtExceptionHandler with the modern
 *       StreamsUncaughtExceptionHandler. The default handler
 *       (SHUTDOWN_CLIENT on most errors) remains active; the custom
 *       REPLACE_THREAD policy is never wired up.</li>
 *   <li>{@code globalStateRestoreListenerAfterStart} — start(), then
 *       setGlobalStateRestoreListener. During the next big-state restore
 *       window, no progress is reported and the operator concludes the
 *       application is hung.</li>
 *   <li>{@code standbyUpdateListenerAfterStart} — start(), then
 *       setStandbyUpdateListener (Kafka 3.5+). Standby-replication progress
 *       is invisible; standby-task lag monitoring is silently broken.</li>
 *   <li>{@code captureSetStateListenerAfterStart} — bound
 *       method-reference {@code streams::setStateListener} captured AFTER
 *       an in-line start() and passed to {@code Optional.ifPresent(...)}.
 *       When the Optional resolves and invokes the consumer, the setter
 *       throws on a non-CREATED instance and the listener is never
 *       registered.</li>
 * </ol>
 *
 * <p>Correct pattern: every {@code set*Listener} /
 * {@code setUncaughtExceptionHandler} call must be on a CREATED-state
 * instance, i.e. ABOVE the {@code start()} call in the same method. The
 * {@code GoodStreamsListenerBeforeStart} silent controls cover the
 * canonical correct shapes.
 */
public final class BadStreamsListenerAfterStart {

    private static final String BROKERS = "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092";

    private static Properties streamsProps(String appId) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.CLIENT_ID_CONFIG, "listener-after-start-" + appId);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return p;
    }

    private static StreamsBuilder topology(String appId) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("in-" + appId).to("out-" + appId);
        return builder;
    }

    /**
     * Anti-pattern #1: setStateListener installed AFTER start().
     * Throws IllegalStateException; the alert-on-ERROR hook never fires.
     */
    public void stateListenerAfterStart() {
        KafkaStreams streams = new KafkaStreams(topology("bad-state-after").build(),
                streamsProps("bad-state-listener-after-start"));
        try {
            streams.start();
            streams.setStateListener((newState, oldState) -> {
                if (newState == KafkaStreams.State.ERROR) {
                    System.err.println("alert: streams ERROR");
                }
            }); // FIRES — instance is no longer in CREATED
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #2: setUncaughtExceptionHandler installed AFTER start().
     * The custom REPLACE_THREAD-then-fallback-to-SHUTDOWN_APPLICATION policy
     * is never wired; the default handler stays in effect.
     */
    public void uncaughtExceptionHandlerAfterStart() {
        KafkaStreams streams = new KafkaStreams(topology("bad-uncaught-after").build(),
                streamsProps("bad-uncaught-handler-after-start"));
        try {
            streams.start();
            streams.setUncaughtExceptionHandler(
                    throwable -> org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                            .StreamThreadExceptionResponse.REPLACE_THREAD); // FIRES
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #3: setGlobalStateRestoreListener installed AFTER start().
     * Restore progress is never reported during the next restore window;
     * operators watching JMX think the app is hung and force-restart it.
     */
    public void globalStateRestoreListenerAfterStart() {
        KafkaStreams streams = new KafkaStreams(topology("bad-global-after").build(),
                streamsProps("bad-global-restore-after-start"));
        try {
            streams.start();
            streams.setGlobalStateRestoreListener(new StateRestoreListener() {
                @Override
                public void onRestoreStart(TopicPartition tp, String store, long start, long end) {}

                @Override
                public void onBatchRestored(TopicPartition tp, String store, long offset, long n) {}

                @Override
                public void onRestoreEnd(TopicPartition tp, String store, long total) {}
            }); // FIRES
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #4: setStandbyUpdateListener installed AFTER start()
     * (Kafka 3.5+). Standby-replication progress is invisible; standby-task
     * lag dashboards stay flat regardless of actual lag.
     */
    public void standbyUpdateListenerAfterStart() {
        KafkaStreams streams = new KafkaStreams(topology("bad-standby-after").build(),
                streamsProps("bad-standby-after-start"));
        try {
            streams.start();
            streams.setStandbyUpdateListener(new StandbyUpdateListener() {
                @Override
                public void onUpdateStart(TopicPartition tp, String store, long start) {}

                @Override
                public void onBatchLoaded(TopicPartition tp, String store, TaskId taskId,
                                          long offset, long n, long current) {}

                @Override
                public void onUpdateSuspended(TopicPartition tp, String store, long current,
                                              long limit, SuspendReason reason) {}
            }); // FIRES
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }

    /**
     * Anti-pattern #5: bound method-reference {@code streams::setStateListener}
     * captured AFTER an in-line {@code start()}, passed to
     * {@code Optional.ifPresent(...)} as the canonical 'install if
     * configured' shape. The capturing INVOKEDYNAMIC bsmArgs hold a
     * {@code REF_invokeVirtual} handle to
     * {@code KafkaStreams.setStateListener(L...StateListener;)V} and a
     * captured {@code streams} reference. When the {@code Optional}
     * resolves and invokes the consumer, the instance is no longer in
     * CREATED, the setter throws
     * {@code IllegalStateException("Can only set StateListener in CREATED
     * state. ...")}, and the listener is never registered. The exception
     * surfaces on the caller's thread but is typically caught by a generic
     * 'failed to apply optional config' handler around init code.
     */
    public void captureSetStateListenerAfterStart(Optional<KafkaStreams.StateListener> maybeListener) {
        KafkaStreams streams = new KafkaStreams(topology("bad-capture-after").build(),
                streamsProps("bad-capture-set-listener-after-start"));
        try {
            streams.start();
            // Bound method-reference captured AFTER start — fires on
            // ifPresent invocation. The static lint signal is at the
            // INVOKEDYNAMIC site (this line), because that's where the
            // ::setStateListener handle is wired to the already-started
            // slot.
            maybeListener.ifPresent(streams::setStateListener); // FIRES — ::setStateListener captured after start
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }
}
