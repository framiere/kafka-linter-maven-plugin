package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

/**
 * RULE: STREAMS_KSTREAM_PROCESS_LEGACY_DEPRECATED — must fire on the
 * three call sites below.
 *
 * <p>The three methods exercise the distinct bytecode shapes the rule
 * is required to catch — two direct {@code INVOKEINTERFACE} calls
 * (one per legacy {@code KStream.process} overload in the Kafka 3.7
 * API) plus one {@code INVOKEDYNAMIC} method-reference capture
 * targeting the legacy overload:
 *
 * <ol>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code KStream.process(ProcessorSupplier, String...)} where
 *       {@code ProcessorSupplier} is the legacy
 *       {@code org.apache.kafka.streams.processor.ProcessorSupplier}.
 *       The descriptor contains
 *       {@code Lorg/apache/kafka/streams/processor/ProcessorSupplier;}
 *       (NOT {@code .../processor/api/ProcessorSupplier;}).</li>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code KStream.process(ProcessorSupplier, Named, String...)}
 *       (the {@code Named}-overloaded form). Legacy supplier
 *       descriptor.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code KStream::process} bound to a custom 3-arg SAM whose
 *       supplier-typed argument erases to the legacy
 *       {@code org.apache.kafka.streams.processor.ProcessorSupplier}.
 *       javac resolves the method-ref to the legacy overload by
 *       matching the SAM's (receiver, arg-erasures, return) tuple.
 *       The user-class bytecode at this site contains ZERO direct
 *       {@code INVOKEINTERFACE} on the legacy {@code process} — only
 *       the {@code INVOKEDYNAMIC} + {@code LambdaMetafactory} bridge
 *       whose bsm-args contain a {@code REF_invokeInterface} handle
 *       pointing at the legacy method.</li>
 * </ol>
 *
 * <h2>Why these overloads are deprecated (KIP-820 summary)</h2>
 *
 * <p>The legacy
 * {@code org.apache.kafka.streams.processor.ProcessorSupplier} yields
 * a non-generic {@code Processor} whose {@code process(K, V)} is
 * erased to {@code Object/Object} at the bytecode level, and the
 * legacy {@code KStream.process} returns {@code void} — terminating
 * the DSL pipeline at the {@code process} call site. The new
 * {@code org.apache.kafka.streams.processor.api.ProcessorSupplier}
 * yields {@code Processor<KIn, VIn, KOut, VOut>} and the new
 * {@code KStream.process} returns {@code KStream<KOut, VOut>} so DSL
 * chaining is natural. Five concrete incident classes the legacy
 * shape caused before KIP-820:
 *
 * <ul>
 *   <li>Pipeline termination at a non-terminal node — legacy
 *       {@code KStream.process(...)} returns {@code void}, so
 *       {@code stream.process(supplier).to("out")} did not compile;
 *       the team-pressure workaround was to attach the downstream
 *       sink inside the processor via {@code context.forward(k, v)} +
 *       sibling {@code addSink} on the Topology, which mixed DSL and
 *       PAPI wiring in one builder, fragmented lineage in
 *       {@code Topology.describe()}, and broke repartition planning.</li>
 *   <li>Type-erased {@code context.forward(k, v)} silently accepts a
 *       downstream-type-incompatible value; the bug ships as a runtime
 *       {@code ClassCastException} inside the downstream node.</li>
 *   <li>Headers, timestamp, and partition exposed via thread-local
 *       context lookups whose semantics depend on whether the call
 *       comes from a punctuator or a record handler.</li>
 *   <li>No {@code processValues} variant — co-partitioned joins that
 *       assumed key preservation break silently with cross-partition
 *       writes when an implementor mutates the key inside
 *       {@code process(K, V)}.</li>
 *   <li>{@code INVOKEDYNAMIC KStream::process} captures silently
 *       bind to the deprecated overload — the user-class bytecode
 *       contains zero direct {@code INVOKEINTERFACE} on the legacy
 *       method and a name-only walk misses it.</li>
 * </ul>
 */
public final class BadKStreamProcessLegacy {

    /**
     * Custom 3-arg SAM that matches the legacy
     * {@code KStream.process(ProcessorSupplier, String...)}
     * signature. Used for shape (3) — an {@code INVOKEDYNAMIC}
     * method-ref capture of {@code KStream::process} bound to this
     * SAM. The supplier-typed argument erases to the legacy
     * {@code org.apache.kafka.streams.processor.ProcessorSupplier},
     * so javac resolves the method-ref to the legacy overload.
     */
    @FunctionalInterface
    public interface ProcessFn {
        void apply(
                KStream<String, String> stream,
                ProcessorSupplier<String, String> supplier,
                String[] parents);
    }

    @SuppressWarnings({"deprecation", "rawtypes", "unchecked"})
    public void directProcess(StreamsBuilder b) {
        // MUST FIRE — direct INVOKEINTERFACE on
        // KStream.process(ProcessorSupplier, String...) taking the
        // LEGACY ProcessorSupplier. The descriptor at the call site is
        // (Lorg/apache/kafka/streams/processor/ProcessorSupplier;[Ljava/lang/String;)V
        // — contains the legacy supplier substring and NOT the new
        // api-package substring.
        KStream<String, String> stream = b.stream("in", Consumed.with(Serdes.String(), Serdes.String()));
        ProcessorSupplier<String, String> legacy = LegacyProcessor::new;
        stream.process(legacy);
    }

    @SuppressWarnings({"deprecation", "rawtypes", "unchecked"})
    public void directProcessNamed(StreamsBuilder b) {
        // MUST FIRE — direct INVOKEINTERFACE on
        // KStream.process(ProcessorSupplier, Named, String...) taking
        // the LEGACY ProcessorSupplier. The descriptor at the call
        // site includes the legacy supplier substring and NOT the new
        // api-package substring.
        KStream<String, String> stream = b.stream("in2", Consumed.with(Serdes.String(), Serdes.String()));
        ProcessorSupplier<String, String> legacy = LegacyProcessor::new;
        stream.process(legacy, Named.as("legacy-named-process"));
    }

    @SuppressWarnings("deprecation")
    public ProcessFn capturedProcess() {
        // MUST FIRE — INVOKEDYNAMIC method-ref capture
        // `KStream::process` bound to a custom 3-arg SAM whose
        // supplier-typed argument erases to the LEGACY
        // org.apache.kafka.streams.processor.ProcessorSupplier. javac
        // resolves the method-ref to the legacy overload at this site
        // because the matching api-package overload would require the
        // SAM's supplier-typed argument to erase to
        // .../processor/api/ProcessorSupplier;. The user-class bytecode
        // at this site contains ZERO direct INVOKEINTERFACE on the
        // legacy process — only the INVOKEDYNAMIC bridge whose
        // bsm-args hold a REF_invokeInterface handle whose desc
        // contains the legacy supplier substring.
        return KStream::process;
    }

    /**
     * Legacy processor implementation — uses the old
     * {@code AbstractProcessor} + {@code process(K, V)} signature.
     * Used as a {@link ProcessorSupplier} target for the two direct
     * call sites above.
     */
    @SuppressWarnings("deprecation")
    static final class LegacyProcessor extends AbstractProcessor<String, String> {
        @Override
        public void process(String key, String value) {
            ProcessorContext ctx = context();
            ctx.forward(key, value == null ? "" : value.toUpperCase());
        }
    }
}
