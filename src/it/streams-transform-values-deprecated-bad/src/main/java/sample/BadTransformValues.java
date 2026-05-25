package sample;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * RULE: STREAMS_TRANSFORM_VALUES_DEPRECATED — must fire on all five
 * methods.
 *
 * <p>The five methods below exercise the five distinct bytecode
 * shapes the rule is required to catch for the four deprecated
 * {@code KStream.transformValues(...)} overloads plus one
 * {@code INVOKEDYNAMIC} method-ref capture:
 *
 * <ol>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code transformValues(ValueTransformerSupplier, String...)}.</li>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code transformValues(ValueTransformerSupplier, Named, String...)}.</li>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code transformValues(ValueTransformerWithKeySupplier, String...)}.</li>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code transformValues(ValueTransformerWithKeySupplier, Named, String...)}.</li>
 *   <li>{@code INVOKEDYNAMIC} unbound method-ref capture
 *       {@code KStream::transformValues} bound to a
 *       {@code ValueBuilder} SAM whose signature is
 *       {@code (KStream<K,V>, ValueTransformerSupplier<V,VR>,
 *       String[]) -> KStream<K,VR>} — javac resolves the method-ref
 *       to the unnamed {@code ValueTransformerSupplier} legacy
 *       overload. The user-class bytecode at this site contains
 *       ZERO direct {@code INVOKEINTERFACE} on the legacy method —
 *       only the {@code INVOKEDYNAMIC} + LambdaMetafactory bridge.</li>
 * </ol>
 *
 * <h2>Why this method is deprecated</h2>
 *
 * <p>{@code KStream.transformValues(...)} is the legacy value-only
 * stateful map. The {@code ValueTransformerWithKey} variant only
 * enforced the key-invariant by convention — the body could call
 * {@code context.forward(otherKey, value)} on the
 * {@code ProcessorContext} and the framework had no way to detect
 * the violation at the type level. KIP-820 (Kafka Streams 3.3,
 * August 2022) replaced it with
 * {@code KStream.processValues(FixedKeyProcessorSupplier)} which uses
 * {@code FixedKeyProcessor.process(FixedKeyRecord)} where
 * {@code FixedKeyRecord} has no setter for the key — the type
 * system statically prevents the violation, eliminating silent
 * partition-skew bugs from leaked-key forwards.
 */
public final class BadTransformValues {

    @SuppressWarnings({"deprecation", "unchecked"})
    public KStream<String, Integer> valueMapDirect(KStream<String, String> stream) {
        // MUST FIRE — direct INVOKEINTERFACE on the deprecated
        // KStream.transformValues(ValueTransformerSupplier, String...).
        return stream.transformValues(LengthValueTransformer::new, "length-store");
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    public KStream<String, Integer> valueMapDirectNamed(KStream<String, String> stream) {
        // MUST FIRE — direct INVOKEINTERFACE on the deprecated
        // KStream.transformValues(ValueTransformerSupplier, Named, String...).
        return stream.transformValues(LengthValueTransformer::new, Named.as("len-step"), "length-store");
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    public KStream<String, Integer> valueMapDirectWithKey(KStream<String, String> stream) {
        // MUST FIRE — direct INVOKEINTERFACE on the deprecated
        // KStream.transformValues(ValueTransformerWithKeySupplier, String...).
        return stream.transformValues(LengthValueTransformerWithKey::new, "length-store");
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    public KStream<String, Integer> valueMapDirectWithKeyNamed(KStream<String, String> stream) {
        // MUST FIRE — direct INVOKEINTERFACE on the deprecated
        // KStream.transformValues(ValueTransformerWithKeySupplier, Named, String...).
        return stream.transformValues(
                LengthValueTransformerWithKey::new, Named.as("len-step"), "length-store");
    }

    @SuppressWarnings("deprecation")
    public ValueBuilder<String, String, Integer> capturedValueBuilder() {
        // MUST FIRE — INVOKEDYNAMIC unbound method-ref capture
        // targeting the deprecated
        // transformValues(ValueTransformerSupplier, String...)
        // overload. javac resolves `KStream::transformValues` by
        // matching the ValueBuilder SAM's (receiver, arg-erasures,
        // return-erasure) triple against the available transformValues
        // overloads, picking the no-Named ValueTransformerSupplier
        // legacy one. javac emits an INVOKEDYNAMIC site whose
        // bsm-args contain a REF_invokeInterface handle on
        // (LValueTransformerSupplier;[Ljava/lang/String;)LKStream;.
        // The user-class bytecode here contains ZERO direct
        // INVOKEINTERFACE on the legacy method.
        return KStream::transformValues;
    }

    @FunctionalInterface
    public interface ValueBuilder<K, V, VR> {
        KStream<K, VR> build(KStream<K, V> stream,
                             ValueTransformerSupplier<V, VR> supplier,
                             String[] storeNames);
    }

    @SuppressWarnings("deprecation")
    public static final class LengthValueTransformer implements ValueTransformer<String, Integer> {
        @Override
        public void init(ProcessorContext context) { }

        @Override
        public Integer transform(String value) {
            return value == null ? 0 : value.length();
        }

        @Override
        public void close() { }
    }

    @SuppressWarnings("deprecation")
    public static final class LengthValueTransformerWithKey implements ValueTransformerWithKey<String, String, Integer> {
        @Override
        public void init(ProcessorContext context) { }

        @Override
        public Integer transform(String key, String value) {
            return value == null ? 0 : value.length();
        }

        @Override
        public void close() { }
    }
}
