package sample;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

/**
 * RULE: TOPOLOGY_ADD_PROCESSOR_LEGACY_SUPPLIER — must fire on both
 * call sites below.
 *
 * <p>The two methods exercise the two distinct bytecode shapes the
 * rule is required to catch — direct {@code INVOKEVIRTUAL} on
 * {@code Topology.addProcessor(...)} with a legacy
 * {@code org.apache.kafka.streams.processor.ProcessorSupplier}
 * argument, plus an {@code INVOKEDYNAMIC} method-ref capture
 * {@code topology::addProcessor} bound to a SAM whose
 * supplier-typed argument is the legacy ProcessorSupplier.
 *
 * <h2>Why these are deprecated (KIP-820 summary)</h2>
 *
 * <p>KIP-820 (Kafka Streams 3.0, September 2021) added new overloads
 * to {@code Topology.addProcessor} taking
 * {@code org.apache.kafka.streams.processor.api.ProcessorSupplier<KIn,
 * VIn, KOut, VOut>} alongside the legacy untyped supplier. The
 * legacy interface uses {@code Processor#process(K key, V value)} —
 * two raw arguments, type-erased forward, and a thread-local
 * {@code ProcessorContext} that exposes headers, timestamp, and
 * partition as separate calls. The new interface uses
 * {@code Processor<KIn, VIn, KOut, VOut>#process(Record<KIn, VIn>
 * record)} — strongly-typed key and value, headers and timestamp
 * carried on the Record, and a typed
 * {@code ProcessorContext<KOut, VOut>} for forwarding.
 *
 * <p>Because the PAPI Topology is the user-visible base layer the
 * DSL is stacked on top of, any custom shop that builds its own DSL
 * over the Topology and stays on the legacy supplier inherits the
 * type-erased {@code context.forward(k, v)} call site at every
 * processor node — a refactor that changes a downstream child's
 * input types ships as a runtime ClassCastException deep inside a
 * sibling processor on the first non-test record after deploy.
 */
public final class BadTopologyAddProcessorLegacy {

    /**
     * SAM whose supplier-typed argument erases to the LEGACY
     * {@code org.apache.kafka.streams.processor.ProcessorSupplier}.
     * A {@code topology::addProcessor} method reference bound to this
     * SAM compiles to {@code INVOKEDYNAMIC} whose bsm-args contain a
     * {@code REF_invokeVirtual} handle pointing at the legacy overload.
     *
     * <p>Concrete type parameters (not wildcards) are required so the
     * compiler can resolve the method reference against the legacy
     * overload unambiguously.
     */
    @FunctionalInterface
    public interface AddProcessorFn {
        Topology apply(
                String name,
                ProcessorSupplier<String, String> supplier,
                String[] parents);
    }

    /**
     * Legacy processor that does nothing — only the supplier's TYPE
     * matters for the lint rule, not the body. Implementing the legacy
     * {@code org.apache.kafka.streams.processor.ProcessorSupplier}
     * directly is the cleanest way to force the supplier's bytecode
     * type onto the call-site descriptor.
     */
    @SuppressWarnings({"deprecation", "unchecked", "rawtypes"})
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
    public Topology directAddProcessor(Topology topology) {
        // MUST FIRE — direct INVOKEVIRTUAL on
        // Topology.addProcessor(String, ProcessorSupplier, String...).
        // The descriptor at the call site contains
        // Lorg/apache/kafka/streams/processor/ProcessorSupplier;
        // (the LEGACY package, no `api/` segment), so the rule's
        // (OWNER=Topology + NAME=addProcessor + DESC contains legacy
        // AND not new) predicate matches.
        return topology.addProcessor("p1", legacySupplier(), "src");
    }

    @SuppressWarnings("deprecation")
    public AddProcessorFn capturedAddProcessor(Topology topology) {
        // MUST FIRE — INVOKEDYNAMIC method-ref capture
        // `topology::addProcessor` bound to AddProcessorFn (whose
        // supplier-erased argument is the LEGACY ProcessorSupplier).
        // The user-class bytecode contains ZERO direct INVOKEVIRTUAL
        // on the legacy method — only the INVOKEDYNAMIC + Lambda
        // Metafactory bridge whose bsm-args hold a REF_invokeVirtual
        // handle whose desc contains the legacy supplier type.
        return topology::addProcessor;
    }
}
