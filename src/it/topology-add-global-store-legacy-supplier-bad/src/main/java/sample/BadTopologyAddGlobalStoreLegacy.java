package sample;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * RULE: TOPOLOGY_ADD_GLOBAL_STORE_LEGACY_SUPPLIER — must fire on
 * both call sites below.
 *
 * <p>The two methods exercise the two distinct bytecode shapes the
 * rule is required to catch — direct {@code INVOKEVIRTUAL} on
 * {@code Topology.addGlobalStore(...)} with a legacy
 * {@code org.apache.kafka.streams.processor.ProcessorSupplier}
 * argument, plus an {@code INVOKEDYNAMIC} method-ref capture
 * {@code topology::addGlobalStore} bound to a SAM whose
 * supplier-typed argument is the legacy ProcessorSupplier.
 *
 * <h2>Why these are deprecated (KIP-820 global-store angle)</h2>
 *
 * <p>KIP-820 (Kafka Streams 3.0, September 2021) added new overloads
 * to {@code Topology.addGlobalStore} accepting
 * {@code org.apache.kafka.streams.processor.api.ProcessorSupplier<KIn,
 * VIn, Void, Void>} alongside the legacy untyped supplier. The
 * deprecation is sharper here than for regular {@code addProcessor}
 * because global stores are special in three ways:
 *
 * <ol>
 *   <li>Replay path runs on every Streams app instance — the global
 *       processor is invoked N times for every record, not once per
 *       partition assignment. Any ClassCastException from a
 *       type-erased {@code context.forward(k, v)} is multiplied
 *       across every replica during the global-store warmup phase
 *       on every container.</li>
 *   <li>Global stores are recovery-blocking — the global state
 *       replay is a startup step that runs before any partitioned
 *       task can be assigned. A failure in the global processor
 *       blocks recovery indefinitely; the Streams instance stays in
 *       REBALANCING and never reaches RUNNING.</li>
 *   <li>The new api supplier types {@code KOut/VOut} as {@code Void}
 *       — accidental forward calls inside the global processor (the
 *       global processor's "child" is typically nothing, the global
 *       store update IS the side effect) fail at compile time
 *       rather than silently propagating state to a stray sibling
 *       at runtime.</li>
 * </ol>
 */
public final class BadTopologyAddGlobalStoreLegacy {

    /**
     * SAM whose supplier-typed argument erases to the LEGACY
     * {@code org.apache.kafka.streams.processor.ProcessorSupplier}.
     * A {@code topology::addGlobalStore} method reference bound to
     * this SAM compiles to {@code INVOKEDYNAMIC} whose bsm-args
     * contain a {@code REF_invokeVirtual} handle pointing at the
     * legacy overload.
     */
    @FunctionalInterface
    public interface AddGlobalStoreFn {
        Topology apply(
                StoreBuilder<?> storeBuilder,
                String sourceName,
                Deserializer<String> keyDeser,
                Deserializer<String> valueDeser,
                String topic,
                String processorName,
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
    public Topology directAddGlobalStore(Topology topology, StoreBuilder<?> storeBuilder) {
        // MUST FIRE — direct INVOKEVIRTUAL on
        // Topology.addGlobalStore(StoreBuilder, String, Deserializer,
        // Deserializer, String, String, ProcessorSupplier).
        // The descriptor at the call site contains
        // Lorg/apache/kafka/streams/processor/ProcessorSupplier; (the
        // LEGACY package, no `api/` segment), so the rule's
        // (OWNER=Topology + NAME=addGlobalStore + DESC contains legacy
        // AND not new) predicate matches.
        return topology.addGlobalStore(
                storeBuilder,
                "src",
                new StringDeserializer(),
                new StringDeserializer(),
                "topic",
                "proc",
                legacySupplier());
    }

    @SuppressWarnings("deprecation")
    public AddGlobalStoreFn capturedAddGlobalStore(Topology topology) {
        // MUST FIRE — INVOKEDYNAMIC method-ref capture
        // `topology::addGlobalStore` bound to AddGlobalStoreFn (whose
        // supplier-erased argument is the LEGACY ProcessorSupplier).
        // The user-class bytecode contains ZERO direct INVOKEVIRTUAL
        // on the legacy method — only the INVOKEDYNAMIC + Lambda
        // Metafactory bridge whose bsm-args hold a REF_invokeVirtual
        // handle whose desc contains the legacy supplier type.
        return topology::addGlobalStore;
    }
}
