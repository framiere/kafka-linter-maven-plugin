package sample;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

import java.util.function.BiFunction;

/**
 * RULE: STREAMS_FLAT_TRANSFORM_DEPRECATED — must NOT fire.
 *
 * <p>The four methods below exercise the supported migration
 * targets for both {@code flatTransform(...)} and
 * {@code flatTransformValues(...)}:
 *
 * <ol>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code KStream.process(ProcessorSupplier, String...)} —
 *       distinct method name from {@code flatTransform}, so the
 *       rule's name filter rejects the site. Fan-out is achieved by
 *       calling {@code ctx.forward(...)} more than once in the
 *       loop body.</li>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code KStream.processValues(FixedKeyProcessorSupplier, Named,
 *       String...)} — distinct method name from
 *       {@code flatTransformValues}.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code KStream::process} — bsm-arg handle name is
 *       {@code process}, not {@code flatTransform*}.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code KStream::processValues} — bsm-arg handle name is
 *       {@code processValues}, not {@code flatTransformValues}.</li>
 * </ol>
 *
 * <h2>Why this is the right migration</h2>
 *
 * <p>KIP-820 (Kafka Streams 3.3, August 2022) replaced the entire
 * legacy {@code transform/transformValues/flatTransform/flatTransformValues}
 * quartet with the single
 * {@code process(ProcessorSupplier)} /
 * {@code processValues(FixedKeyProcessorSupplier)} pair. The no-return
 * {@code Processor.process(Record)} contract makes fan-out
 * first-class via {@code context.forward(...)} called many times in
 * a loop — no intermediate {@code Iterable} allocation, no
 * separate fan-out method needed. {@code FixedKeyRecord} statically
 * forbids constructing a new key, eliminating the silent
 * partition-skew bugs from the legacy
 * {@code ValueTransformerWithKey} key-invariant violation.
 */
public final class GoodFlatTransform {

    public KStream<String, Integer> fanoutByProcess(KStream<String, String> stream) {
        // DOES NOT FIRE — KStream.process(ProcessorSupplier, String...)
        // is the supported KIP-820 replacement for both
        // flatTransform and the no-fan-out transform — fan-out is
        // just calling ctx.forward(...) more than once in the loop.
        // Distinct method name from `flatTransform`, so the name
        // filter rejects this site.
        return stream.process(FanoutProcessor::new, "store");
    }

    public KStream<String, Integer> fanoutValuesByProcess(KStream<String, String> stream) {
        // DOES NOT FIRE — KStream.processValues(FixedKeyProcessorSupplier,
        // Named, String...) is the supported replacement for both
        // flatTransformValues and the no-fan-out transformValues.
        return stream.processValues(FanoutFixedKeyProcessor::new, Named.as("fan"), "store");
    }

    public BiFunction<KStream<String, String>, ProcessorSupplier<String, String, String, Integer>,
            KStream<String, Integer>> capturedProcess() {
        // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture resolving
        // to KStream::process. The bsm-arg handle's name is
        // `process`, not `flatTransform*`.
        return (s, supplier) -> s.process(supplier);
    }

    public BiFunction<KStream<String, String>, FixedKeyProcessorSupplier<String, String, Integer>,
            KStream<String, Integer>> capturedProcessValues() {
        // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture resolving
        // to KStream::processValues. The bsm-arg handle's name is
        // `processValues`, not `flatTransformValues`.
        return (s, supplier) -> s.processValues(supplier);
    }

    public static final class FanoutProcessor implements Processor<String, String, String, Integer> {
        private ProcessorContext<String, Integer> ctx;

        @Override
        public void init(ProcessorContext<String, Integer> context) {
            this.ctx = context;
        }

        @Override
        public void process(Record<String, String> record) {
            if (record.value() == null) return;
            ctx.forward(new Record<>(record.key() + ":len", record.value().length(), record.timestamp()));
            ctx.forward(new Record<>(record.key() + ":raw", record.value().hashCode(), record.timestamp()));
        }

        @Override
        public void close() { }
    }

    public static final class FanoutFixedKeyProcessor implements FixedKeyProcessor<String, String, Integer> {
        private FixedKeyProcessorContext<String, Integer> ctx;

        @Override
        public void init(FixedKeyProcessorContext<String, Integer> context) {
            this.ctx = context;
        }

        @Override
        public void process(FixedKeyRecord<String, String> record) {
            if (record.value() == null) return;
            ctx.forward(record.withValue(record.value().length()));
            ctx.forward(record.withValue(record.value().hashCode()));
        }

        @Override
        public void close() { }
    }
}
