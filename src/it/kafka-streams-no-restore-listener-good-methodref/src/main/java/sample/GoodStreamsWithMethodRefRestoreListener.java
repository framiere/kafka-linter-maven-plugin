package sample;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.util.Optional;
import java.util.Properties;

/**
 * Silent: KafkaStreams.start() is called AND the restore listener is wired via
 * a deferred method-reference —
 * {@code Optional.of(listener).ifPresent(streams::setGlobalStateRestoreListener)}
 * compiles to an {@code INVOKEDYNAMIC} at the call site whose bootstrap-method
 * args include a {@code REF_invokeVirtual} handle to
 * {@code org/apache/kafka/streams/KafkaStreams.setGlobalStateRestoreListener}.
 *
 * <p>The user-class bytecode in this shape contains <strong>zero
 * {@code INVOKE*} instructions targeting
 * {@code setGlobalStateRestoreListener} directly</strong> — the setter call
 * lives inside the LambdaMetafactory-built {@code Consumer.accept} method,
 * which is synthesized at runtime and never appears in the project's
 * {@code .class} files. A MethodInsnNode-only scan would (incorrectly)
 * conclude {@code hasListener=false} and false-positive on every start() site.
 * The rule's {@code INVOKEDYNAMIC} walk inspects {@code bsmArgs} handles via
 * {@code AsmUtil.indyTargetHandle(indy, Set.of(KAFKA_STREAMS), SET_RESTORE_LISTENER, null)}
 * and treats any captured handle as evidence that the listener is wired
 * (accepting any descriptor).
 *
 * <p>This shape is uncommon in straight-line Streams code (most apps inline the
 * {@code streams.setGlobalStateRestoreListener(...)} call) but appears in
 * functional-style configuration helpers, conditional-registration patterns
 * (register only if the listener bean is present in the Spring context, only
 * when a metrics-registry is configured, etc.), and framework scaffolding —
 * the rule needs to recognise it to avoid noisy false-positives in those
 * codebases.
 *
 * <p><strong>Why the listener body emits metrics:</strong> the canonical use of
 * a {@link StateRestoreListener} is to publish per-batch restore progress so
 * the operator dashboard can show "restored 12.4 GB of 200 GB" instead of "app
 * is up but throughput is zero" during the long restoration window.
 */
public final class GoodStreamsWithMethodRefRestoreListener {

    private static Properties streamsProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "good-methodref-restore-listener-app");
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

    public void startWithMethodReferenceRestoreListener() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());
        StateRestoreListener listener = new StateRestoreListener() {
            @Override
            public void onRestoreStart(TopicPartition topicPartition, String storeName,
                                       long startingOffset, long endingOffset) {
                // emit kafka_streams_restore_starting_offset
            }
            @Override
            public void onBatchRestored(TopicPartition topicPartition, String storeName,
                                        long batchEndOffset, long numRestored) {
                // emit kafka_streams_restored_records_total
            }
            @Override
            public void onRestoreEnd(TopicPartition topicPartition, String storeName,
                                     long totalRestored) {
                // emit kafka_streams_restore_completions_total
            }
        };
        Optional.of(listener).ifPresent(streams::setGlobalStateRestoreListener);
        streams.start();  // SILENT — INVOKEDYNAMIC method-ref to setGlobalStateRestoreListener is recognised
    }
}
