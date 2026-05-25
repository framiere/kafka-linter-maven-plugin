package sample;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collections;
import java.util.List;

/**
 * RULE: STREAMS_FLAT_TRANSFORM_DEPRECATED — must fire on all seven
 * methods.
 *
 * <p>The seven methods below exercise the seven distinct bytecode
 * shapes the rule is required to catch — six direct calls for the
 * six deprecated overloads plus one {@code INVOKEDYNAMIC} method-ref
 * capture:
 *
 * <ol>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code flatTransform(TransformerSupplier, String...)}.</li>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code flatTransform(TransformerSupplier, Named, String...)}.</li>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code flatTransformValues(ValueTransformerSupplier, String...)}.</li>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code flatTransformValues(ValueTransformerSupplier, Named, String...)}.</li>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code flatTransformValues(ValueTransformerWithKeySupplier, String...)}.</li>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code flatTransformValues(ValueTransformerWithKeySupplier, Named, String...)}.</li>
 *   <li>{@code INVOKEDYNAMIC} unbound method-ref capture
 *       {@code KStream::flatTransform} bound to a
 *       {@code FlatBuilder} SAM whose signature is
 *       {@code (KStream<K,V>, TransformerSupplier<K,V,Iterable<KeyValue<K1,V1>>>,
 *       String[]) -> KStream<K1,V1>}. javac resolves the method-ref
 *       to the unnamed legacy flatTransform overload by matching the
 *       SAM's (receiver, arg-erasures, return-erasure) triple. The
 *       user-class bytecode at this site contains ZERO direct
 *       INVOKEINTERFACE on the legacy method — only the
 *       INVOKEDYNAMIC + LambdaMetafactory bridge.</li>
 * </ol>
 *
 * <h2>Why these methods are deprecated</h2>
 *
 * <p>These are the fan-out siblings of {@code transform} /
 * {@code transformValues}. The supplied transformer returns
 * {@code Iterable}, which forces an allocation-per-output design.
 * KIP-820 (Kafka Streams 3.3, August 2022) replaced the entire
 * quartet (transform/transformValues/flatTransform/flatTransformValues)
 * with the single {@code process(ProcessorSupplier)} /
 * {@code processValues(FixedKeyProcessorSupplier)} pair whose
 * no-return contract makes fan-out first-class via
 * {@code context.forward(...)} called many times in a loop — no
 * intermediate collection allocation, type-enforced key-invariant
 * via {@code FixedKeyRecord}, overridable timestamp at forward
 * time.
 */
public final class BadFlatTransform {

    @SuppressWarnings({"deprecation", "unchecked"})
    public KStream<String, Integer> fanoutDirect(KStream<String, String> stream) {
        // MUST FIRE — flatTransform(TransformerSupplier, String...).
        return stream.flatTransform(FanoutTransformer::new, "store");
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    public KStream<String, Integer> fanoutDirectNamed(KStream<String, String> stream) {
        // MUST FIRE — flatTransform(TransformerSupplier, Named, String...).
        return stream.flatTransform(FanoutTransformer::new, Named.as("fan"), "store");
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    public KStream<String, Integer> fanoutValuesDirect(KStream<String, String> stream) {
        // MUST FIRE — flatTransformValues(ValueTransformerSupplier, String...).
        return stream.flatTransformValues(FanoutValueTransformer::new, "store");
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    public KStream<String, Integer> fanoutValuesDirectNamed(KStream<String, String> stream) {
        // MUST FIRE — flatTransformValues(ValueTransformerSupplier, Named, String...).
        return stream.flatTransformValues(FanoutValueTransformer::new, Named.as("fan"), "store");
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    public KStream<String, Integer> fanoutValuesWithKeyDirect(KStream<String, String> stream) {
        // MUST FIRE — flatTransformValues(ValueTransformerWithKeySupplier, String...).
        return stream.flatTransformValues(FanoutValueTransformerWithKey::new, "store");
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    public KStream<String, Integer> fanoutValuesWithKeyDirectNamed(KStream<String, String> stream) {
        // MUST FIRE — flatTransformValues(ValueTransformerWithKeySupplier, Named, String...).
        return stream.flatTransformValues(FanoutValueTransformerWithKey::new, Named.as("fan"), "store");
    }

    @SuppressWarnings("deprecation")
    public FlatBuilder<String, String, String, Integer> capturedFlatBuilder() {
        // MUST FIRE — INVOKEDYNAMIC unbound method-ref capture
        // targeting the deprecated flatTransform(TransformerSupplier,
        // String...) overload. javac emits an INVOKEDYNAMIC site
        // whose bsm-args contain a REF_invokeInterface handle on
        // (LTransformerSupplier;[Ljava/lang/String;)LKStream;. The
        // user-class bytecode here contains ZERO direct
        // INVOKEINTERFACE on the legacy method.
        return KStream::flatTransform;
    }

    @FunctionalInterface
    public interface FlatBuilder<K, V, K1, V1> {
        KStream<K1, V1> build(KStream<K, V> stream,
                              TransformerSupplier<K, V, Iterable<KeyValue<K1, V1>>> supplier,
                              String[] storeNames);
    }

    @SuppressWarnings("deprecation")
    public static final class FanoutTransformer implements Transformer<String, String, Iterable<KeyValue<String, Integer>>> {
        @Override
        public void init(ProcessorContext context) { }

        @Override
        public Iterable<KeyValue<String, Integer>> transform(String key, String value) {
            if (value == null) return Collections.emptyList();
            return List.of(
                    KeyValue.pair(key + ":len", value.length()),
                    KeyValue.pair(key + ":raw", value.hashCode()));
        }

        @Override
        public void close() { }
    }

    @SuppressWarnings("deprecation")
    public static final class FanoutValueTransformer implements ValueTransformer<String, Iterable<Integer>> {
        @Override
        public void init(ProcessorContext context) { }

        @Override
        public Iterable<Integer> transform(String value) {
            if (value == null) return Collections.emptyList();
            return List.of(value.length(), value.hashCode());
        }

        @Override
        public void close() { }
    }

    @SuppressWarnings("deprecation")
    public static final class FanoutValueTransformerWithKey implements ValueTransformerWithKey<String, String, Iterable<Integer>> {
        @Override
        public void init(ProcessorContext context) { }

        @Override
        public Iterable<Integer> transform(String key, String value) {
            if (value == null) return Collections.emptyList();
            return List.of(value.length(), key.length() + value.length());
        }

        @Override
        public void close() { }
    }
}
