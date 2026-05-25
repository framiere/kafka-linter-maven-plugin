package sample;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * RULE: TOPOLOGY_ADD_GLOBAL_STORE_LEGACY_SUPPLIER — must NOT fire
 * on the call sites below.
 *
 * <p>All shapes target the KIP-820 migration replacement
 * {@code Topology.addGlobalStore(StoreBuilder<?>, String,
 * Deserializer<KIn>, Deserializer<VIn>, String, String,
 * api.ProcessorSupplier<KIn, VIn, Void, Void>)}. Same owner
 * ({@code Topology}), same method name ({@code addGlobalStore}),
 * but the descriptor differs — the supplier-typed argument is the
 * new {@code api.ProcessorSupplier} (descriptor segment
 * {@code processor/api/ProcessorSupplier;}), so the rule's
 * {@code contains(legacy) && !contains(new)} predicate rejects every
 * site here.
 *
 * <p>The new supplier additionally constrains {@code KOut/VOut} to
 * {@code Void}, so any accidental {@code context.forward(...)} call
 * inside the global processor body fails at compile time — a
 * compile-time guarantee the legacy untyped supplier could not
 * provide.
 */
public final class GoodTopologyAddGlobalStoreLegacy {

    @FunctionalInterface
    public interface AddGlobalStoreFn {
        Topology apply(
                StoreBuilder<?> storeBuilder,
                String sourceName,
                Deserializer<String> keyDeser,
                Deserializer<String> valueDeser,
                String topic,
                String processorName,
                ProcessorSupplier<String, String, Void, Void> supplier);
    }

    private static ProcessorSupplier<String, String, Void, Void> newSupplier() {
        return () -> new Processor<String, String, Void, Void>() {
            @Override
            public void init(ProcessorContext<Void, Void> context) {
            }

            @Override
            public void process(Record<String, String> record) {
            }

            @Override
            public void close() {
            }
        };
    }

    public Topology directAddGlobalStore(Topology topology, StoreBuilder<?> storeBuilder) {
        return topology.addGlobalStore(
                storeBuilder,
                "src",
                new StringDeserializer(),
                new StringDeserializer(),
                "topic",
                "proc",
                newSupplier());
    }

    public AddGlobalStoreFn capturedAddGlobalStore(Topology topology) {
        return topology::addGlobalStore;
    }
}
