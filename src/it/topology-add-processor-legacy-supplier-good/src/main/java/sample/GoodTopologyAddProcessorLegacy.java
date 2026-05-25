package sample;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

/**
 * RULE: TOPOLOGY_ADD_PROCESSOR_LEGACY_SUPPLIER — must NOT fire on
 * the call sites below.
 *
 * <p>All shapes target the KIP-820 migration replacement
 * {@code Topology.addProcessor(String name, api.ProcessorSupplier<KIn,
 * VIn, KOut, VOut> supplier, String... parentNames)}. The owner is
 * identical to the legacy call ({@code Topology}) and the method
 * name is identical too ({@code addProcessor}), but the descriptor
 * differs — the supplier-typed argument is the new
 * {@code org.apache.kafka.streams.processor.api.ProcessorSupplier}
 * (descriptor segment {@code processor/api/ProcessorSupplier;}),
 * which does NOT contain the legacy substring
 * {@code processor/ProcessorSupplier;}, so the rule's
 * {@code contains(legacy) && !contains(new)} predicate rejects every
 * site here. This proves the rule discriminates by supplier-type
 * descriptor and does not over-fire on the new API.
 *
 * <h2>Why these two shapes specifically</h2>
 *
 * <p>The shapes mirror the BAD fixture one-for-one so that the GOOD
 * fixture exercises the exact same bytecode paths the rule walks:
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code Topology.addProcessor(String, api.ProcessorSupplier,
 *       String...)} — same owner + name as the legacy site, only the
 *       descriptor's supplier type differs.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code topology::addProcessor} bound to a SAM whose
 *       supplier-typed argument is the new
 *       {@code api.ProcessorSupplier}. The bsm-args hold a
 *       {@code REF_invokeVirtual} handle whose desc contains
 *       {@code processor/api/ProcessorSupplier;} — DESC predicate
 *       rejects.</li>
 * </ol>
 *
 * <p>The indy case is the most important non-firing one: a rule that
 * only checked owner + name (skipping the descriptor predicate)
 * would fire here. The dedicated rule checks owner + name + desc on
 * both the direct and indy paths and so passes.
 */
public final class GoodTopologyAddProcessorLegacy {

    /**
     * SAM whose supplier-typed argument is the NEW typed
     * {@code api.ProcessorSupplier}. A {@code topology::addProcessor}
     * method reference bound to this SAM compiles to
     * {@code INVOKEDYNAMIC} whose bsm-args contain a
     * {@code REF_invokeVirtual} handle pointing at the new overload.
     */
    @FunctionalInterface
    public interface AddProcessorFn {
        Topology apply(
                String name,
                ProcessorSupplier<String, String, String, String> supplier,
                String[] parents);
    }

    private static ProcessorSupplier<String, String, String, String> newSupplier() {
        return () -> new Processor<String, String, String, String>() {
            @Override
            public void init(ProcessorContext<String, String> context) {
            }

            @Override
            public void process(Record<String, String> record) {
            }

            @Override
            public void close() {
            }
        };
    }

    public Topology directAddProcessor(Topology topology) {
        return topology.addProcessor("p1", newSupplier(), "src");
    }

    public AddProcessorFn capturedAddProcessor(Topology topology) {
        return topology::addProcessor;
    }
}
