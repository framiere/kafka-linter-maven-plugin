package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

/**
 * RULE: STREAMS_KSTREAM_PROCESS_LEGACY_DEPRECATED — must NOT fire on
 * any of the three call sites below.
 *
 * <p>Each method uses the post-KIP-820
 * {@code org.apache.kafka.streams.processor.api.ProcessorSupplier}.
 * None of the three call sites reaches the legacy
 * {@code org.apache.kafka.streams.processor.ProcessorSupplier}
 * overload of {@code KStream.process}.
 *
 * <ol>
 *   <li>{@code KStream.process(ProcessorSupplier, String...)} with
 *       the new api-package ProcessorSupplier. The descriptor contains
 *       {@code Lorg/apache/kafka/streams/processor/api/ProcessorSupplier;}
 *       — the new-package substring, so the rule's predicate (legacy
 *       AND NOT new) rejects this site.</li>
 *   <li>{@code KStream.process(ProcessorSupplier, Named, String...)}
 *       with the new api-package ProcessorSupplier.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code KStream::process} bound to a SAM whose supplier-typed
 *       argument erases to the NEW api-package ProcessorSupplier. The
 *       bsm-arg handle's desc contains the new-package substring, so
 *       the predicate rejects this site.</li>
 * </ol>
 *
 * <p>The KIP-820 replacement closes the five incident classes that
 * justified the deprecation: the new {@code KStream.process} returns
 * {@code KStream<KOut, VOut>} so DSL chaining is natural; typed
 * {@code Record} eliminates the forward-time
 * {@code ClassCastException}; typed
 * {@code ProcessorContext<KOut, VOut>} compile-time-checks the
 * forward arguments; named-child fan-out via
 * {@code context.forward(record, childName)} is reorder-safe; and the
 * separate {@code processValues} entry point with a
 * {@code FixedKeyProcessor} contract structurally prevents key
 * mutation.
 */
public final class GoodKStreamProcessLegacy {

    /**
     * Custom 3-arg SAM that matches the NEW
     * {@code KStream.process(ProcessorSupplier, String...)} overload.
     * The supplier-typed argument erases to the NEW api-package
     * {@code ProcessorSupplier}, so the method-ref capture below
     * resolves to the new overload and the rule's predicate (legacy
     * substring AND NOT new substring) rejects the call site.
     */
    @FunctionalInterface
    public interface ProcessFn {
        KStream<String, String> apply(
                KStream<String, String> stream,
                ProcessorSupplier<String, String, String, String> supplier,
                String[] parents);
    }

    public KStream<String, String> directProcess(StreamsBuilder b) {
        // DOES NOT FIRE — KStream.process with the NEW api-package
        // ProcessorSupplier. The descriptor contains
        // Lorg/apache/kafka/streams/processor/api/ProcessorSupplier;
        // and the rule's predicate (legacy AND NOT new) rejects.
        KStream<String, String> stream = b.stream("in", Consumed.with(Serdes.String(), Serdes.String()));
        ProcessorSupplier<String, String, String, String> newApi =
                () -> new Processor<>() {
                    @Override
                    public void process(Record<String, String> record) {
                        // no-op
                    }
                };
        return stream.process(newApi);
    }

    public KStream<String, String> directProcessNamed(StreamsBuilder b) {
        // DOES NOT FIRE — KStream.process(ProcessorSupplier, Named,
        // String...) with the NEW api-package ProcessorSupplier.
        KStream<String, String> stream = b.stream("in2", Consumed.with(Serdes.String(), Serdes.String()));
        ProcessorSupplier<String, String, String, String> newApi =
                () -> new Processor<>() {
                    @Override
                    public void process(Record<String, String> record) {
                        // no-op
                    }
                };
        return stream.process(newApi, Named.as("new-named-process"));
    }

    public ProcessFn capturedProcess() {
        // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture
        // `KStream::process` bound to a SAM whose supplier-typed
        // argument erases to the NEW api-package ProcessorSupplier.
        // javac resolves the method-ref to the new overload because
        // the SAM's supplier argument matches the new-package
        // signature; the bsm-arg handle's desc contains the
        // new-package substring, and the predicate rejects.
        return KStream::process;
    }
}
