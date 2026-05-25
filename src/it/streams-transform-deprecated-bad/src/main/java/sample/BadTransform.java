package sample;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * RULE: STREAMS_TRANSFORM_DEPRECATED — must fire on all three methods.
 *
 * <p>The three methods below exercise the three distinct bytecode
 * shapes the rule is required to catch for the deprecated
 * {@code KStream.transform(...)} overloads:
 *
 * <ol>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code KStream.transform(TransformerSupplier, String...)} —
 *       descriptor
 *       {@code (Lorg/apache/kafka/streams/kstream/TransformerSupplier;[Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KStream;}.</li>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code KStream.transform(TransformerSupplier, Named, String...)} —
 *       descriptor
 *       {@code (Lorg/apache/kafka/streams/kstream/TransformerSupplier;Lorg/apache/kafka/streams/kstream/Named;[Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KStream;}.
 *       Same method name as above, distinct descriptor — both shapes
 *       are deprecated by KIP-820.</li>
 *   <li>{@code INVOKEDYNAMIC} unbound method-ref capture
 *       {@code KStream::transform} bound to a {@code Builder} SAM
 *       whose signature is {@code (KStream<K,V>, TransformerSupplier<K,
 *       V, KeyValue<K1, V1>>, String[]) -> KStream<K1, V1>} — javac
 *       resolves the method-ref to the unnamed legacy transform
 *       overload by matching the SAM's (receiver, arg-erasures,
 *       return-erasure) triple against the available transform
 *       overloads, picking the no-Named one. javac emits an
 *       {@code INVOKEDYNAMIC} whose bsm-args contain a
 *       {@code REF_invokeInterface} handle pointing at the legacy
 *       method. The user-class bytecode at this site contains ZERO
 *       direct {@code INVOKEINTERFACE} on the legacy method — only
 *       the {@code INVOKEDYNAMIC} + LambdaMetafactory bridge.</li>
 * </ol>
 *
 * <h2>Why this method is deprecated</h2>
 *
 * <p>{@code KStream.transform(TransformerSupplier, String...)} is the
 * legacy "stateful map" hook: it takes a
 * {@code Transformer.transform(K, V) -> KeyValue<K1, V1>} that the
 * framework forwards (return null = drop). KIP-820 (Kafka Streams
 * 3.3, August 2022) replaced it with
 * {@code KStream.process(ProcessorSupplier)} from
 * {@code org.apache.kafka.streams.processor.api}. The new API
 * receives a {@code Record<K, V>} (immutable, with an overridable
 * timestamp) and emits via {@code context.forward(Record)} zero,
 * one, or many times — making fan-out, drop, and re-keying
 * first-class instead of bolted onto the return value. The legacy
 * {@code org.apache.kafka.streams.processor.Processor} interface
 * that {@code TransformerSupplier} implements internally is itself
 * deprecated; the only forward-compatible interface is
 * {@code org.apache.kafka.streams.processor.api.Processor}.
 */
public final class BadTransform {

    @SuppressWarnings({"deprecation", "unchecked"})
    public KStream<String, Integer> stateMapDirect(KStream<String, String> stream) {
        // MUST FIRE — direct INVOKEINTERFACE on the deprecated
        // KStream.transform(TransformerSupplier, String...) with
        // descriptor (LTransformerSupplier;[Ljava/lang/String;)LKStream;.
        return stream.transform(LengthTransformer::new, "length-store");
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    public KStream<String, Integer> stateMapDirectNamed(KStream<String, String> stream) {
        // MUST FIRE — direct INVOKEINTERFACE on the deprecated
        // KStream.transform(TransformerSupplier, Named, String...)
        // with descriptor
        // (LTransformerSupplier;LNamed;[Ljava/lang/String;)LKStream;.
        // Same method name, distinct descriptor — both shapes are
        // deprecated.
        return stream.transform(LengthTransformer::new, Named.as("length-step"), "length-store");
    }

    @SuppressWarnings("deprecation")
    public Builder<String, String, String, Integer> capturedBuilder() {
        // MUST FIRE — INVOKEDYNAMIC unbound method-ref capture
        // targeting the deprecated transform(TransformerSupplier,
        // String...) overload. javac resolves `KStream::transform`
        // by matching the Builder SAM's (receiver, arg-erasures,
        // return-erasure) triple against the available transform
        // overloads, picking the no-Named legacy one. javac emits
        // an INVOKEDYNAMIC site whose bsm-args contain a
        // REF_invokeInterface handle on
        // (LTransformerSupplier;[Ljava/lang/String;)LKStream;.
        // The user-class bytecode here contains ZERO direct
        // INVOKEINTERFACE on the legacy method — only the
        // INVOKEDYNAMIC + LambdaMetafactory bridge.
        return KStream::transform;
    }

    @FunctionalInterface
    public interface Builder<K, V, K1, V1> {
        KStream<K1, V1> build(KStream<K, V> stream,
                              TransformerSupplier<K, V, KeyValue<K1, V1>> supplier,
                              String[] storeNames);
    }

    @SuppressWarnings("deprecation")
    public static final class LengthTransformer implements Transformer<String, String, KeyValue<String, Integer>> {
        @Override
        public void init(ProcessorContext context) { }

        @Override
        public KeyValue<String, Integer> transform(String key, String value) {
            return KeyValue.pair(key, value == null ? 0 : value.length());
        }

        @Override
        public void close() { }
    }
}
