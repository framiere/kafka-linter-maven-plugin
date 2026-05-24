package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.util.Optional;
import java.util.Properties;

/**
 * Silent: KafkaStreams.start() is called AND the handler is wired via a
 * deferred method-reference — Optional.of(streams).ifPresent(s ->
 * s.setUncaughtExceptionHandler(handler)) compiles to a lambda; the equivalent
 * method-reference form
 * {@code Optional.of(handler).ifPresent(streams::setUncaughtExceptionHandler)}
 * compiles to an INVOKEDYNAMIC at the call site whose bootstrap-method args
 * include a REF_invokeVirtual handle to
 * {@code org/apache/kafka/streams/KafkaStreams.setUncaughtExceptionHandler}.
 *
 * <p>The user-class bytecode in this shape contains <strong>zero
 * {@code INVOKE*} instructions targeting {@code setUncaughtExceptionHandler}
 * directly</strong> — the handler call lives inside the LambdaMetafactory-built
 * Consumer's {@code accept} method, which is synthesized at runtime and never
 * appears in the project's {@code .class} files. A MethodInsnNode-only scan
 * would (incorrectly) conclude {@code hasHandler=false} and false-positive on
 * the start() site. The rule's INVOKEDYNAMIC walk inspects {@code bsmArgs}
 * handles via
 * {@code AsmUtil.indyTargetHandle(indy, Set.of(KAFKA_STREAMS), SET_HANDLER, null)}
 * and treats any captured handle as evidence that the handler is wired
 * (accepting any descriptor — both legacy and KIP-671 overloads).
 *
 * <p>This shape is uncommon in straight-line Streams code (most apps inline
 * the {@code streams.setUncaughtExceptionHandler(...)} call) but appears in
 * functional-style configuration helpers, conditional-registration patterns
 * (register only if the handler bean is present), and framework scaffolding —
 * the rule needs to recognise it to avoid noisy false-positives in those
 * codebases.
 */
public final class GoodStreamsWithMethodRefHandler {

    private static Properties streamsProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "good-methodref-handler-app");
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
        b.<String, String>stream("orders").mapValues(v -> v.toUpperCase()).to("orders-upper");
        return b.build();
    }

    public void startWithMethodReferenceHandler() {
        KafkaStreams streams = new KafkaStreams(topology(), streamsProps());
        StreamsUncaughtExceptionHandler handler =
                exception -> StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        Optional.of(handler).ifPresent(streams::setUncaughtExceptionHandler);
        streams.start();  // SILENT — INVOKEDYNAMIC method-ref to setUncaughtExceptionHandler is recognised
    }
}
