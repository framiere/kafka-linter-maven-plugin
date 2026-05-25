package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * RULE: STREAMSBUILDER_ADDGLOBALSTORE_LEGACY_SUPPLIER — must fire
 * on both call sites below.
 *
 * <p>The two methods exercise the two distinct bytecode shapes the
 * rule is required to catch — direct {@code INVOKEVIRTUAL} on
 * {@code StreamsBuilder.addGlobalStore(...)} with a legacy
 * {@code org.apache.kafka.streams.processor.ProcessorSupplier}
 * argument, plus an {@code INVOKEDYNAMIC} method-ref capture
 * {@code builder::addGlobalStore} bound to a SAM whose
 * supplier-typed argument is the legacy ProcessorSupplier.
 *
 * <h2>Why these are deprecated (KIP-820 — DSL entry point)</h2>
 *
 * <p>KIP-820 (Kafka Streams 3.0, September 2021) added new
 * overloads to {@code StreamsBuilder.addGlobalStore} accepting
 * {@code api.ProcessorSupplier<KIn, VIn, Void, Void>}.
 * {@code StreamsBuilder.addGlobalStore} is the DSL-level entry
 * point for global stores; the deprecation timer is shared with the
 * PAPI-level {@code Topology.addGlobalStore}, but the DSL form is
 * sharper because:
 *
 * <ul>
 *   <li>It is the most common PAPI hook in an otherwise DSL-only
 *       Streams app — when the global-store registration uses the
 *       legacy untyped supplier, the resulting Processor lives in
 *       the otherwise-typed DSL pipeline as an island of
 *       Object/Object.</li>
 *   <li>The {@code Consumed<K, V>} parameter on the call site
 *       types the deserializers; with the legacy raw supplier,
 *       javac cannot enforce that the supplier's Processor<K, V>
 *       type-parameters match the Consumed<K, V> types.</li>
 * </ul>
 */
public final class BadStreamsBuilderAddGlobalStoreLegacy {

    /**
     * SAM whose supplier-typed argument erases to the LEGACY
     * {@code org.apache.kafka.streams.processor.ProcessorSupplier}.
     * A {@code builder::addGlobalStore} method reference bound to
     * this SAM compiles to {@code INVOKEDYNAMIC} whose bsm-args
     * contain a {@code REF_invokeVirtual} handle pointing at the
     * legacy overload.
     */
    @FunctionalInterface
    public interface AddGlobalStoreFn {
        StreamsBuilder apply(
                StoreBuilder<?> storeBuilder,
                String topic,
                Consumed<String, String> consumed,
                ProcessorSupplier<String, String> supplier);
    }

    @SuppressWarnings({"deprecation", "rawtypes", "unchecked"})
    private static ProcessorSupplier<String, String> legacySupplier() {
        return () -> new Processor<String, String>() {
            @Override
            public void init(ProcessorContext context) {
            }

            @Override
            public void process(String key, String value) {
            }

            @Override
            public void close() {
            }
        };
    }

    @SuppressWarnings("deprecation")
    public StreamsBuilder directAddGlobalStore(StreamsBuilder builder, StoreBuilder<?> storeBuilder) {
        // MUST FIRE — direct INVOKEVIRTUAL on
        // StreamsBuilder.addGlobalStore(StoreBuilder, String,
        // Consumed, ProcessorSupplier).
        // The descriptor at the call site contains
        // Lorg/apache/kafka/streams/processor/ProcessorSupplier;
        // (the LEGACY package, no `api/` segment).
        return builder.addGlobalStore(
                storeBuilder,
                "topic",
                Consumed.with(Serdes.String(), Serdes.String()),
                legacySupplier());
    }

    @SuppressWarnings("deprecation")
    public AddGlobalStoreFn capturedAddGlobalStore(StreamsBuilder builder) {
        // MUST FIRE — INVOKEDYNAMIC method-ref capture
        // `builder::addGlobalStore` bound to AddGlobalStoreFn (whose
        // supplier-erased argument is the LEGACY ProcessorSupplier).
        // The user-class bytecode contains ZERO direct INVOKEVIRTUAL
        // on the legacy method — only the INVOKEDYNAMIC + Lambda
        // Metafactory bridge whose bsm-args hold a REF_invokeVirtual
        // handle whose desc contains the legacy supplier type.
        return builder::addGlobalStore;
    }
}
