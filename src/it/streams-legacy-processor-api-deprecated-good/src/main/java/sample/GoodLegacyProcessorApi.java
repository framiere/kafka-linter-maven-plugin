package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * RULE: STREAMS_LEGACY_PROCESSOR_API_DEPRECATED — must NOT fire on
 * any of the four call sites below.
 *
 * <p>Each method uses the post-KIP-820
 * {@code org.apache.kafka.streams.processor.api.ProcessorSupplier}.
 * None of the four call sites reaches the legacy
 * {@code org.apache.kafka.streams.processor.ProcessorSupplier}
 * overload of {@code Topology.addProcessor},
 * {@code Topology.addGlobalStore}, or
 * {@code StreamsBuilder.addGlobalStore}.
 *
 * <ol>
 *   <li>{@code Topology.addProcessor(String, ProcessorSupplier,
 *       String...)} with the new api-package ProcessorSupplier. The
 *       descriptor contains
 *       {@code Lorg/apache/kafka/streams/processor/api/ProcessorSupplier;}
 *       — the new-package substring, so the rule's predicate (legacy
 *       AND NOT new) rejects this site.</li>
 *   <li>{@code Topology.addGlobalStore(...)} with the new api-package
 *       ProcessorSupplier.</li>
 *   <li>{@code StreamsBuilder.addGlobalStore(StoreBuilder, String,
 *       Consumed, ProcessorSupplier)} with the new api-package
 *       ProcessorSupplier.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code Topology::addProcessor} bound to a SAM whose
 *       supplier-typed argument erases to the NEW api-package
 *       ProcessorSupplier. The bsm-arg handle's desc contains the
 *       new-package substring, so the predicate rejects this site.</li>
 * </ol>
 *
 * <p>The KIP-820 replacement closes the five incident classes that
 * justified the deprecation: typed {@code Record} eliminates the
 * forward-time {@code ClassCastException}, typed
 * {@code ProcessorContext<KOut, VOut>} compile-time-checks the
 * forward arguments, named-child fan-out via
 * {@code context.forward(record, childName)} is reorder-safe, and the
 * separate {@code FixedKeyProcessor} contract structurally prevents
 * key mutation.
 */
public final class GoodLegacyProcessorApi {

    /**
     * Custom 4-arg SAM that matches the NEW
     * {@code Topology.addProcessor(String, ProcessorSupplier,
     * String...)} overload. The supplier-typed argument erases to the
     * NEW api-package {@code ProcessorSupplier}, so the method-ref
     * capture below resolves to the new overload and the rule's
     * predicate (legacy substring AND NOT new substring) rejects the
     * call site.
     */
    @FunctionalInterface
    public interface AddProcessorFn {
        Topology apply(Topology t, String name, ProcessorSupplier<?, ?, ?, ?> supplier, String[] parents);
    }

    public Topology directTopologyAddProcessor(Topology t) {
        // DOES NOT FIRE — Topology.addProcessor with the NEW
        // api-package ProcessorSupplier. The descriptor contains
        // Lorg/apache/kafka/streams/processor/api/ProcessorSupplier;
        // and the rule's predicate (legacy AND NOT new) rejects.
        ProcessorSupplier<String, String, String, String> newApi =
                () -> new Processor<>() {
                    @Override
                    public void process(Record<String, String> record) {
                        // no-op
                    }
                };
        return t.addProcessor("new-direct", newApi, "source");
    }

    public Topology directTopologyAddGlobalStore(Topology t) {
        // DOES NOT FIRE — Topology.addGlobalStore 7-arg overload with
        // the NEW api-package ProcessorSupplier.
        StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("new-global-store"),
                Serdes.String(),
                Serdes.String());
        ProcessorSupplier<String, String, Void, Void> newApi =
                () -> new Processor<>() {
                    @Override
                    public void process(Record<String, String> record) {
                        // no-op
                    }
                };
        return t.addGlobalStore(
                storeBuilder,
                "global-source",
                Serdes.String().deserializer(),
                Serdes.String().deserializer(),
                "global-topic",
                "global-processor",
                newApi);
    }

    public StreamsBuilder directStreamsBuilderAddGlobalStore(StreamsBuilder b) {
        // DOES NOT FIRE — StreamsBuilder.addGlobalStore with the NEW
        // api-package ProcessorSupplier.
        StoreBuilder<KeyValueStore<Bytes, byte[]>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("new-sb-global-store"),
                Serdes.Bytes(),
                Serdes.ByteArray());
        ProcessorSupplier<String, String, Void, Void> newApi =
                () -> new Processor<>() {
                    @Override
                    public void process(Record<String, String> record) {
                        // no-op
                    }
                };
        return b.addGlobalStore(
                storeBuilder,
                "sb-global-topic",
                Consumed.with(Serdes.String(), Serdes.String()),
                newApi);
    }

    public AddProcessorFn capturedTopologyAddProcessor() {
        // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture
        // `Topology::addProcessor` bound to a SAM whose supplier-typed
        // argument erases to the NEW api-package ProcessorSupplier.
        // javac resolves the method-ref to the new overload because
        // the SAM's supplier argument matches the new-package
        // signature; the bsm-arg handle's desc contains the
        // new-package substring, and the predicate rejects.
        return Topology::addProcessor;
    }

    /**
     * Throw-away to keep an unused-import-style nudge away from
     * {@link ProcessorContext} — the import documents the typed
     * forwarding context that the new API gives processors but is not
     * exercised directly by the four call sites above.
     */
    @SuppressWarnings("unused")
    private static void touchProcessorContextImport(ProcessorContext<String, String> ctx) {
        // no-op
    }
}
