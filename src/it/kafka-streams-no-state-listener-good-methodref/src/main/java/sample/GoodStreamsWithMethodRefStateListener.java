package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Optional;
import java.util.Properties;

/**
 * Silent: KafkaStreams.start() is called AND the StateListener is wired via a
 * deferred method-reference —
 * {@code Optional.of(listener).ifPresent(streams::setStateListener)} compiles
 * to an {@code INVOKEDYNAMIC} at the call site whose bootstrap-method args
 * include a {@code REF_invokeVirtual} handle to
 * {@code org/apache/kafka/streams/KafkaStreams.setStateListener}.
 *
 * <p>The user-class bytecode in this shape contains <strong>zero
 * {@code INVOKE*} instructions targeting {@code setStateListener}
 * directly</strong> — the setter call lives inside the LambdaMetafactory-built
 * Consumer's {@code accept} method, which is synthesized at runtime and never
 * appears in the project's {@code .class} files. A MethodInsnNode-only scan
 * would (incorrectly) conclude {@code hasListener=false} and false-positive on
 * the start() site. The rule's INVOKEDYNAMIC walk inspects {@code bsmArgs}
 * handles via
 * {@code AsmUtil.indyTargetHandle(indy, Set.of(KAFKA_STREAMS), SET_STATE_LISTENER, null)}
 * and treats any captured handle as evidence that the listener is wired
 * (accepting any descriptor).
 *
 * <p>This shape is uncommon in straight-line Streams code (most apps inline the
 * {@code streams.setStateListener(...)} call) but appears in functional-style
 * configuration helpers, conditional-registration patterns (register only if
 * the listener bean is present), and framework scaffolding — the rule needs to
 * recognise it to avoid noisy false-positives in those codebases.
 *
 * <p><strong>Why the listener body calls System.exit(1) on ERROR:</strong>
 * canonical safe default — when Streams transitions to {@code ERROR}, killing
 * the JVM is the fastest way to release the consumer-group membership so the
 * partitions rebalance away. Kubernetes / systemd restarts the pod from a
 * clean state.
 */
public final class GoodStreamsWithMethodRefStateListener {

    private static Properties streamsProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "good-methodref-state-listener-app");
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

    public void startWithMethodReferenceStateListener() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());
        KafkaStreams.StateListener listener = (newState, oldState) -> {
            if (newState == KafkaStreams.State.ERROR) {
                System.exit(1);
            }
        };
        Optional.of(listener).ifPresent(streams::setStateListener);
        streams.start();  // SILENT — INVOKEDYNAMIC method-ref to setStateListener is recognised
    }
}
