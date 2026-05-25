package sample;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

import java.util.function.BiFunction;

/**
 * RULE: STREAMS_TRANSFORM_VALUES_DEPRECATED — must NOT fire.
 *
 * <p>The three methods below exercise the supported migration
 * targets for {@code KStream.transformValues(...)}:
 *
 * <ol>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code KStream.processValues(FixedKeyProcessorSupplier, String...)} —
 *       the modern KIP-820 replacement. Distinct method name from
 *       the legacy {@code transformValues}, so the rule's name
 *       filter rejects the site.</li>
 *   <li>Direct {@code INVOKEINTERFACE} on the named-overload
 *       {@code KStream.processValues(FixedKeyProcessorSupplier, Named, String...)}.</li>
 *   <li>{@code INVOKEDYNAMIC} unbound method-ref capture
 *       {@code KStream::processValues} bound to a BiFunction-shaped
 *       builder — the bsm-arg handle's name is {@code processValues},
 *       not {@code transformValues}, so the rule's name filter
 *       rejects the site.</li>
 * </ol>
 *
 * <h2>Why this is the right migration</h2>
 *
 * <p>KIP-820 (Kafka Streams 3.3, August 2022) replaced
 * {@code transformValues(...)} with
 * {@code processValues(FixedKeyProcessorSupplier)} from
 * {@code org.apache.kafka.streams.processor.api}. The new
 * {@code FixedKeyProcessor.process(FixedKeyRecord)} contract has no
 * return — the processor calls
 * {@code context.forward(record.withValue(newV))} zero, one, or
 * many times. The key correctness win: {@code FixedKeyRecord} has
 * no setter or copy-constructor for the key, so the user code
 * physically cannot construct a new key — the type system
 * statically enforces the key-invariant that the legacy
 * {@code ValueTransformerWithKey} only enforced by convention.
 * Downstream {@code groupByKey} / join operations are guaranteed
 * co-partitioned with the source-topic partition assignment.
 */
public final class GoodTransformValues {

    public KStream<String, Integer> valueMapDirect(KStream<String, String> stream) {
        // DOES NOT FIRE — KStream.processValues(FixedKeyProcessorSupplier,
        // String...) is the supported KIP-820 replacement. Distinct
        // method name from the legacy `transformValues`, so the
        // name filter rejects this site.
        return stream.processValues(LengthFixedKeyProcessor::new, "length-store");
    }

    public KStream<String, Integer> valueMapDirectNamed(KStream<String, String> stream) {
        // DOES NOT FIRE — KStream.processValues(FixedKeyProcessorSupplier,
        // Named, String...) is the named-overload of the supported
        // replacement. Distinct method name from `transformValues`.
        return stream.processValues(LengthFixedKeyProcessor::new, Named.as("len-step"), "length-store");
    }

    public BiFunction<KStream<String, String>, FixedKeyProcessorSupplier<String, String, Integer>,
            KStream<String, Integer>> capturedValueBuilder() {
        // DOES NOT FIRE — INVOKEDYNAMIC unbound method-ref capture
        // resolving to KStream::processValues (the supported
        // method). The bsm-arg handle's name is `processValues`, not
        // `transformValues`, so the rule's name filter rejects the
        // site.
        return (s, supplier) -> s.processValues(supplier);
    }

    public static final class LengthFixedKeyProcessor implements FixedKeyProcessor<String, String, Integer> {
        private FixedKeyProcessorContext<String, Integer> ctx;

        @Override
        public void init(FixedKeyProcessorContext<String, Integer> context) {
            this.ctx = context;
        }

        @Override
        public void process(FixedKeyRecord<String, String> record) {
            int length = record.value() == null ? 0 : record.value().length();
            ctx.forward(record.withValue(length));
        }

        @Override
        public void close() { }
    }
}
