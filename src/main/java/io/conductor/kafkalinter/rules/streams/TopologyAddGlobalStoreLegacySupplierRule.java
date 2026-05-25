package io.conductor.kafkalinter.rules.streams;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Handle;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Fires for every reach of the deprecated
 * {@link org.apache.kafka.streams.Topology#addGlobalStore} overload
 * taking the legacy
 * {@code org.apache.kafka.streams.processor.ProcessorSupplier}.
 *
 * <p>The rule catches both direct {@code INVOKEVIRTUAL} calls and
 * indirect {@code INVOKEDYNAMIC} method-reference captures (e.g.
 * {@code topology::addGlobalStore} bound to a topology-factory SAM)
 * via a dual walk over each method's instructions.
 *
 * <h2>Why this overload is deprecated (KIP-820 summary, global-store
 * angle)</h2>
 *
 * <p>KIP-820 (Kafka Streams 3.0, September 2021) added new overloads
 * to {@link org.apache.kafka.streams.Topology#addGlobalStore}
 * accepting
 * {@code org.apache.kafka.streams.processor.api.ProcessorSupplier<KIn,
 * VIn, Void, Void>} alongside the legacy untyped supplier. The
 * legacy interface inherits the same Processor#process(K, V) +
 * type-erased forward + thread-local ProcessorContext problems as
 * the regular addProcessor overload, but the global-store entry
 * point carries five additional failure modes specific to global
 * stores:
 *
 * <ul>
 *   <li><b>Replay path on every instance.</b> Global stores are
 *       populated by replaying the source topic through this
 *       processor on every Streams app instance — not partitioned,
 *       not co-partitioned, every instance reads the whole topic.
 *       A legacy untyped Processor running here means every Streams
 *       app instance is using the deprecated API on the replay path,
 *       so a type-related ClassCastException in the global processor
 *       is N-times-multiplied across every replica and surfaces in
 *       every container during the global-store warmup phase, not
 *       just on one partition's replay.</li>
 *   <li><b>Global stores are recovery-blocking — replay starts before
 *       any task assignment.</b> The global state replay is a
 *       startup-blocking step that runs before any partitioned task
 *       can be assigned. A {@code ClassCastException} from a
 *       type-erased {@code context.forward(k, v)} or a
 *       {@code context.headers()} thread-local call in the global
 *       processor blocks recovery indefinitely — the Streams
 *       instance stays in the REBALANCING state and never reaches
 *       RUNNING, even though the partitioned topology is otherwise
 *       healthy.</li>
 *   <li><b>Out-of-order keys reach the global processor by design.</b>
 *       Global stores replay the entire source topic in arbitrary
 *       partition order — keys are not co-partitioned with anything.
 *       A legacy {@code Processor#process(K, V)} that implicitly
 *       relied on key-ordered arrival (because the application that
 *       wrote the global topic ordered by key) sees out-of-order
 *       replay; the new {@code Record<KIn, VIn>} carries the
 *       producer's record timestamp on the Record itself, making
 *       event-time decisions explicit at the call site rather than
 *       depending on the thread-local context.</li>
 *   <li><b>Heterogeneous forward inside a global processor is
 *       almost always a bug.</b> A legacy global processor calling
 *       {@code context.forward(k, v)} to a downstream child node
 *       (the global processor's "child" is typically nothing — the
 *       global store update IS the side effect) silently propagates
 *       state to wherever a stray sibling node lands. The new
 *       {@code Processor<KIn, VIn, Void, Void>} types
 *       {@code KOut/VOut} as Void at compile time, making any
 *       accidental forward fail at compile time.</li>
 *   <li><b>INVOKEDYNAMIC {@code topology::addGlobalStore} captures
 *       silently bind to the deprecated overload.</b> A
 *       topology-factory abstraction (e.g. a config-driven global-
 *       store registration helper consuming a SAM whose supplier-
 *       erased argument is the legacy ProcessorSupplier) resolves
 *       the method-ref at link time. The user-class bytecode
 *       contains zero direct {@code INVOKEVIRTUAL} on the legacy
 *       method — only the {@code INVOKEDYNAMIC} +
 *       {@code LambdaMetafactory} bridge whose bsm-args contain a
 *       {@code REF_invokeVirtual} handle whose descriptor contains
 *       the legacy supplier type.</li>
 * </ul>
 *
 * <h2>Descriptor discrimination — legacy vs new ProcessorSupplier
 * package</h2>
 *
 * <p>The deprecated and supported overloads share method name and
 * argument arity, differing only in which {@code ProcessorSupplier}
 * type they accept:
 *
 * <ul>
 *   <li>Legacy:
 *       {@code Lorg/apache/kafka/streams/processor/ProcessorSupplier;}</li>
 *   <li>New:
 *       {@code Lorg/apache/kafka/streams/processor/api/ProcessorSupplier;}</li>
 * </ul>
 *
 * <p>The predicate is the conjunction:
 *
 * <ul>
 *   <li>{@code desc} contains the legacy descriptor substring AND</li>
 *   <li>{@code desc} does NOT contain the new (api-package)
 *       descriptor substring.</li>
 * </ul>
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code topology::addGlobalStore} bound to a SAM with the
 * matching arity / erasures compiles to {@code INVOKEDYNAMIC} whose
 * bsm-args contain a {@code REF_invokeVirtual} handle (Topology is a
 * class, not an interface). The rule walks the bsm-args at every
 * indy site, checks each Handle's {@code (owner, name)} against
 * {@code Topology × addGlobalStore}, and then applies the same
 * descriptor predicate as for direct calls.
 */
public final class TopologyAddGlobalStoreLegacySupplierRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.TOPOLOGY);
    private static final String METHOD_NAME = "addGlobalStore";
    private static final String LEGACY_SUPPLIER_DESC = "Lorg/apache/kafka/streams/processor/ProcessorSupplier;";
    private static final String NEW_SUPPLIER_DESC = "Lorg/apache/kafka/streams/processor/api/ProcessorSupplier;";

    private final Severity severity;

    public TopologyAddGlobalStoreLegacySupplierRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.TOPOLOGY_ADD_GLOBAL_STORE_LEGACY_SUPPLIER;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && isLegacyProcessorSupplierDesc(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy && indy.bsmArgs != null) {
                    for (Object arg : indy.bsmArgs) {
                        if (!(arg instanceof Handle h)) continue;
                        if (!OWNERS.contains(h.getOwner())) continue;
                        if (!METHOD_NAME.equals(h.getName())) continue;
                        if (!isLegacyProcessorSupplierDesc(h.getDesc())) continue;
                        out.add(violation(ctx, mn, insn));
                        break;
                    }
                }
            }
        }
        return out;
    }

    private static boolean isLegacyProcessorSupplierDesc(String desc) {
        return desc != null
                && desc.contains(LEGACY_SUPPLIER_DESC)
                && !desc.contains(NEW_SUPPLIER_DESC);
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.TOPOLOGY_ADD_GLOBAL_STORE_LEGACY_SUPPLIER, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Topology.addGlobalStore overload taking the legacy "
                        + "`org.apache.kafka.streams.processor.ProcessorSupplier` "
                        + "is reached here — either as a direct INVOKEVIRTUAL call "
                        + "or as an INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`topology::addGlobalStore` bound to a topology-factory "
                        + "SAM whose supplier-erased argument is the legacy "
                        + "ProcessorSupplier). The legacy overload is deprecated "
                        + "since Kafka Streams 3.0 (KIP-820, September 2021); the "
                        + "new overload takes "
                        + "`org.apache.kafka.streams.processor.api.ProcessorSupplier"
                        + "<KIn, VIn, Void, Void>` and yields a typed "
                        + "Processor<KIn, VIn, Void, Void> with strongly-typed "
                        + "Record<KIn, VIn> inputs and a Void-typed forward (any "
                        + "accidental forward fails at compile time). Five concrete "
                        + "failure modes follow: (1) replay path on every instance "
                        + "— global stores are populated by replaying the source "
                        + "topic through this processor on every Streams app "
                        + "instance, not partitioned, every instance reads the "
                        + "whole topic; a legacy untyped Processor running here "
                        + "means every Streams app instance is using the deprecated "
                        + "API on the replay path, so a type-related "
                        + "ClassCastException in the global processor is "
                        + "N-times-multiplied across every replica and surfaces in "
                        + "every container during the global-store warmup phase, "
                        + "not just on one partition's replay; (2) global stores "
                        + "are recovery-blocking — the global state replay is a "
                        + "startup-blocking step that runs before any partitioned "
                        + "task can be assigned; a ClassCastException from a "
                        + "type-erased context.forward(k, v) in the global "
                        + "processor blocks recovery indefinitely, the Streams "
                        + "instance stays in REBALANCING and never reaches RUNNING; "
                        + "(3) out-of-order keys reach the global processor by "
                        + "design — global stores replay the entire source topic "
                        + "in arbitrary partition order, keys are not "
                        + "co-partitioned; a legacy Processor#process(K, V) that "
                        + "implicitly relied on key-ordered arrival (because the "
                        + "application that wrote the global topic ordered by key) "
                        + "sees out-of-order replay; the new Record<KIn, VIn> "
                        + "carries the producer's record timestamp on the Record "
                        + "itself, making event-time decisions explicit at the "
                        + "call site rather than depending on the thread-local "
                        + "context; (4) heterogeneous forward inside a global "
                        + "processor is almost always a bug — a legacy global "
                        + "processor calling context.forward(k, v) to a downstream "
                        + "child node silently propagates state to wherever a "
                        + "stray sibling node lands; the new "
                        + "Processor<KIn, VIn, Void, Void> types KOut/VOut as Void "
                        + "at compile time, making any accidental forward fail at "
                        + "compile time; (5) INVOKEDYNAMIC "
                        + "`topology::addGlobalStore` captures silently bind to "
                        + "the deprecated overload — a topology-factory "
                        + "abstraction consuming a SAM whose supplier-erased "
                        + "argument is the legacy ProcessorSupplier resolves the "
                        + "method-ref by arity and erased argument types; the "
                        + "user-class bytecode contains zero direct INVOKEVIRTUAL "
                        + "on the legacy method, only an indy site whose bsm-args "
                        + "contain a REF_invokeVirtual handle whose descriptor "
                        + "contains the legacy supplier type. Migration: change "
                        + "the import from "
                        + "`org.apache.kafka.streams.processor.ProcessorSupplier` "
                        + "to "
                        + "`org.apache.kafka.streams.processor.api.ProcessorSupplier` "
                        + "with type parameters <KIn, VIn, Void, Void> for the "
                        + "global-store supplier and refactor the Processor's "
                        + "`process(K key, V value)` into `process(Record<KIn, "
                        + "VIn> record)`. The Void out-types on the new supplier "
                        + "fail at compile time on any accidental "
                        + "context.forward(...) call site.");
    }
}
