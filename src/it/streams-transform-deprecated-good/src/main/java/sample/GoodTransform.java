package sample;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

import java.util.function.BiFunction;

/**
 * RULE: STREAMS_TRANSFORM_DEPRECATED — must NOT fire.
 *
 * <p>The three methods below exercise the supported migration
 * targets for {@code KStream.transform(...)}:
 *
 * <ol>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code KStream.process(ProcessorSupplier, String...)} — the
 *       modern KIP-820 replacement. Distinct method name from the
 *       legacy {@code transform}, so the rule's name filter rejects
 *       the site.</li>
 *   <li>Direct {@code INVOKEINTERFACE} on the named-overload
 *       {@code KStream.process(ProcessorSupplier, Named, String...)}.
 *       Same reasoning: distinct method name.</li>
 *   <li>{@code INVOKEDYNAMIC} unbound method-ref capture
 *       {@code KStream::process} bound to a {@code Builder} SAM —
 *       the bsm-arg handle's name is {@code process}, not
 *       {@code transform}, so the rule's name filter rejects the
 *       site.</li>
 * </ol>
 *
 * <h2>Why this is the right migration</h2>
 *
 * <p>KIP-820 (Kafka Streams 3.3, August 2022) replaced
 * {@code transform()} with {@code process(ProcessorSupplier)} from
 * {@code org.apache.kafka.streams.processor.api} — note the
 * {@code .api} package, distinct from the legacy
 * {@code org.apache.kafka.streams.processor} home of the deprecated
 * {@code Transformer}/{@code Processor} types. The new
 * {@code Processor.process(Record)} contract has no return — the
 * processor calls {@code context.forward(Record)} zero, one, or many
 * times. Fan-out, drop, and re-keying are first-class. The
 * {@code Record} value type carries an immutable
 * {@code key}/{@code value}/{@code timestamp}/{@code headers}, and
 * {@code Processor.process} receives both the immutable record and a
 * typed {@code ProcessorContext<KOut, VOut>} — no accessor-state
 * coupling, no {@code context.timestamp() == -1 between calls}
 * bugs, and an overridable timestamp at forward time.
 */
public final class GoodTransform {

    public KStream<String, Integer> stateMapDirect(KStream<String, String> stream) {
        // DOES NOT FIRE — KStream.process(ProcessorSupplier, String...)
        // is the supported KIP-820 replacement. Distinct method name
        // from the legacy `transform`, so the name filter rejects
        // this site.
        return stream.process(LengthProcessor::new, "length-store");
    }

    public KStream<String, Integer> stateMapDirectNamed(KStream<String, String> stream) {
        // DOES NOT FIRE — KStream.process(ProcessorSupplier, Named,
        // String...) is the named-overload of the supported
        // replacement. Distinct method name from `transform`.
        return stream.process(LengthProcessor::new, Named.as("length-step"), "length-store");
    }

    public BiFunction<KStream<String, String>, ProcessorSupplier<String, String, String, Integer>,
            KStream<String, Integer>> capturedBuilder() {
        // DOES NOT FIRE — INVOKEDYNAMIC unbound method-ref capture
        // resolving to KStream::process (the supported method). The
        // bsm-arg handle's name is `process`, not `transform`, so
        // the rule's name filter rejects the site. Uses a
        // store-less factory shape via a BiFunction so the call
        // wires through varargs at the supplier-application site
        // (the varargs array is supplied later by callers).
        return (s, supplier) -> s.process(supplier);
    }

    public static final class LengthProcessor implements Processor<String, String, String, Integer> {
        private ProcessorContext<String, Integer> ctx;

        @Override
        public void init(ProcessorContext<String, Integer> context) {
            this.ctx = context;
        }

        @Override
        public void process(Record<String, String> record) {
            int length = record.value() == null ? 0 : record.value().length();
            ctx.forward(new Record<>(record.key(), length, record.timestamp()));
        }

        @Override
        public void close() { }
    }
}
