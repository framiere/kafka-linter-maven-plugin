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
 * Control for STREAMS_SET_LISTENER_AFTER_START.
 *
 * <p>Six methods that each avoid the rule by following the only correct
 * shape: every {@code set*Listener} /
 * {@code setUncaughtExceptionHandler} call is placed BEFORE the
 * {@code start()} call in the same method, while the instance is still in
 * CREATED.
 *
 * <ol>
 *   <li>{@code stateListenerBeforeStart} — setStateListener, then start.</li>
 *   <li>{@code uncaughtExceptionHandlerBeforeStart} —
 *       setUncaughtExceptionHandler, then start.</li>
 *   <li>{@code globalStateRestoreListenerBeforeStart} —
 *       setGlobalStateRestoreListener, then start.</li>
 *   <li>{@code standbyUpdateListenerBeforeStart} —
 *       setStandbyUpdateListener, then start.</li>
 *   <li>{@code captureSetStateListenerBeforeStart} — bound
 *       method-reference {@code streams::setStateListener} passed to
 *       {@code Optional.ifPresent} BEFORE {@code start()}. Rule does NOT
 *       fire because the slot is not yet in startedSlots.</li>
 *   <li>{@code allListenersBeforeStart} — all four setters in sequence
 *       before start; rule honors the correct lifecycle.</li>
 * </ol>
 */
public final class GoodStreamsListenerBeforeStart {

    private static final String BROKERS = "kafka-1.prod.example.com:9092,kafka-2.prod.example.com:9092,kafka-3.prod.example.com:9092";

    private static Properties streamsProps(String appId) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        p.put(StreamsConfig.CLIENT_ID_CONFIG, "good-listener-" + appId);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return p;
    }

    private static StreamsBuilder topology(String appId) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("in-" + appId).to("out-" + appId);
        return builder;
    }

    /** Correct: setStateListener BEFORE start. */
    public void stateListenerBeforeStart() {
        KafkaStreams streams = new KafkaStreams(topology("good-state").build(),
                streamsProps("good-state-listener"));
        try {
            streams.setStateListener((newState, oldState) -> {});
            streams.start();
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: setUncaughtExceptionHandler BEFORE start. */
    public void uncaughtExceptionHandlerBeforeStart() {
        KafkaStreams streams = new KafkaStreams(topology("good-uncaught").build(),
                streamsProps("good-uncaught-handler"));
        try {
            streams.setUncaughtExceptionHandler(throwable ->
                    org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                            .StreamThreadExceptionResponse.REPLACE_THREAD);
            streams.start();
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: setGlobalStateRestoreListener BEFORE start. */
    public void globalStateRestoreListenerBeforeStart() {
        KafkaStreams streams = new KafkaStreams(topology("good-global").build(),
                streamsProps("good-global-restore"));
        try {
            streams.setGlobalStateRestoreListener(new StateRestoreListener() {
                @Override
                public void onRestoreStart(TopicPartition tp, String store, long start, long end) {}

                @Override
                public void onBatchRestored(TopicPartition tp, String store, long offset, long n) {}

                @Override
                public void onRestoreEnd(TopicPartition tp, String store, long total) {}
            });
            streams.start();
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: setStandbyUpdateListener BEFORE start (Kafka 3.5+). */
    public void standbyUpdateListenerBeforeStart() {
        KafkaStreams streams = new KafkaStreams(topology("good-standby").build(),
                streamsProps("good-standby-listener"));
        try {
            streams.setStandbyUpdateListener(new StandbyUpdateListener() {
                @Override
                public void onUpdateStart(TopicPartition tp, String store, long start) {}

                @Override
                public void onBatchLoaded(TopicPartition tp, String store, TaskId taskId,
                                          long offset, long n, long current) {}

                @Override
                public void onUpdateSuspended(TopicPartition tp, String store, long current,
                                              long limit, SuspendReason reason) {}
            });
            streams.start();
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: bound method-reference captured BEFORE start — fully legal. */
    public void captureSetStateListenerBeforeStart(Optional<KafkaStreams.StateListener> maybeListener) {
        KafkaStreams streams = new KafkaStreams(topology("good-capture-before").build(),
                streamsProps("good-capture-set-listener-before-start"));
        try {
            maybeListener.ifPresent(streams::setStateListener);
            streams.start();
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }

    /** Correct: all four setters before start, in any order. */
    public void allListenersBeforeStart() {
        KafkaStreams streams = new KafkaStreams(topology("good-all").build(),
                streamsProps("good-all-listeners"));
        try {
            streams.setStateListener((newState, oldState) -> {});
            streams.setUncaughtExceptionHandler(throwable ->
                    org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                            .StreamThreadExceptionResponse.REPLACE_THREAD);
            streams.setGlobalStateRestoreListener(new StateRestoreListener() {
                @Override
                public void onRestoreStart(TopicPartition tp, String store, long start, long end) {}

                @Override
                public void onBatchRestored(TopicPartition tp, String store, long offset, long n) {}

                @Override
                public void onRestoreEnd(TopicPartition tp, String store, long total) {}
            });
            streams.setStandbyUpdateListener(new StandbyUpdateListener() {
                @Override
                public void onUpdateStart(TopicPartition tp, String store, long start) {}

                @Override
                public void onBatchLoaded(TopicPartition tp, String store, TaskId taskId,
                                          long offset, long n, long current) {}

                @Override
                public void onUpdateSuspended(TopicPartition tp, String store, long current,
                                              long limit, SuspendReason reason) {}
            });
            streams.start();
        } finally {
            streams.close(Duration.ofSeconds(10));
        }
    }
}
