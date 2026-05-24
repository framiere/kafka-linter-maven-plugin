package sample;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.util.Properties;

/**
 * Silent: KafkaStreams.start() is called AND a StateRestoreListener is wired via
 * a direct INVOKEVIRTUAL on
 * {@code KafkaStreams.setGlobalStateRestoreListener(StateRestoreListener)}. The
 * user-class bytecode at the registration site contains a MethodInsnNode with
 * {@code owner=org/apache/kafka/streams/KafkaStreams},
 * {@code name=setGlobalStateRestoreListener}, which the project-walk picks up
 * and flips {@code hasListener=true}.
 *
 * <p><strong>What this controls for:</strong> proves the rule does NOT fire when
 * a restore listener exists somewhere in the project — i.e. that the
 * {@code hasListener} suppression works end-to-end. Without this control, the
 * BAD fixture alone cannot distinguish "rule fires correctly on missing
 * listener" from "rule fires unconditionally on any
 * {@code KafkaStreams.start()} call". The pair (BAD → 2 errors,
 * good-direct → 0) pins the boolean behaviour.
 *
 * <h2>Why a StateRestoreListener is operationally critical</h2>
 *
 * <p>When a Streams app starts (or rebalances onto new partitions), each
 * stateful task replays its changelog into the local state store. A stateful
 * Streams app with non-trivial state can spend MINUTES or HOURS in the
 * {@code REBALANCING} → {@code RUNNING} transition while changelogs replay —
 * on a 200 GB RocksDB-backed KTable, restoration commonly takes 25 minutes
 * end-to-end. Without a {@link StateRestoreListener} hook, the application
 * has no in-process signal to publish to dashboards or alerting:
 *
 * <ul>
 *   <li>Operators see "service is up but throughput is zero" with no way to
 *       distinguish "stuck" from "still restoring".</li>
 *   <li>Health probes either flap (k8s readiness fails because the app isn't
 *       RUNNING yet) or lie (probe returns 200 OK while the app is still
 *       2 GB into a 200 GB restore).</li>
 *   <li>Capacity-planning data is missing — there's no recorded
 *       restore-throughput history to size {@code num.standby.replicas} or
 *       {@code session.timeout.ms} from.</li>
 * </ul>
 *
 * <p>The canonical hook is {@code onBatchRestored(...)} — incrementing a
 * Prometheus counter on every restored batch gives a per-partition
 * "records-restored / records-remaining" gauge that drives both the
 * Kubernetes readiness gate and the operator dashboard.
 *
 * <p>The {@link StateRestoreListener} contract has four callbacks since Kafka
 * 2.5: {@code onRestoreStart}, {@code onBatchRestored}, {@code onRestoreEnd},
 * and (3.5+) {@code onRestoreSuspended}. This fixture implements all four
 * inline as an anonymous class — the simplest shape that compiles to a direct
 * MethodInsnNode call.
 */
public final class GoodStreamsWithDirectRestoreListener {

    private static Properties streamsProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "good-direct-restore-listener-app");
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

    public void startWithRegisteredRestoreListener() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());
        streams.setGlobalStateRestoreListener(new StateRestoreListener() {
            @Override
            public void onRestoreStart(TopicPartition topicPartition, String storeName,
                                       long startingOffset, long endingOffset) {
                // emit prometheus gauge: kafka_streams_restore_starting_offset{store, partition}
            }
            @Override
            public void onBatchRestored(TopicPartition topicPartition, String storeName,
                                        long batchEndOffset, long numRestored) {
                // emit prometheus counter: kafka_streams_restored_records_total += numRestored
            }
            @Override
            public void onRestoreEnd(TopicPartition topicPartition, String storeName,
                                     long totalRestored) {
                // emit prometheus counter: kafka_streams_restore_completions_total
            }
        });
        streams.start();  // SILENT — direct INVOKEVIRTUAL setGlobalStateRestoreListener wired
    }
}
