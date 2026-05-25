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
 * {@link org.apache.kafka.streams.Topology#addProcessor} overload
 * taking the legacy
 * {@code org.apache.kafka.streams.processor.ProcessorSupplier}.
 *
 * <p>The rule catches both direct {@code INVOKEVIRTUAL} calls and
 * indirect {@code INVOKEDYNAMIC} method-reference captures (e.g.
 * {@code topology::addProcessor} bound to a topology-factory SAM)
 * via a dual walk over each method's instructions.
 *
 * <h2>Why this overload is deprecated (KIP-820 summary)</h2>
 *
 * <p>KIP-820 (Kafka Streams 3.0, September 2021) added new overloads
 * to {@link org.apache.kafka.streams.Topology#addProcessor} taking
 * {@code org.apache.kafka.streams.processor.api.ProcessorSupplier<KIn,
 * VIn, KOut, VOut>} alongside the legacy untyped
 * {@code org.apache.kafka.streams.processor.ProcessorSupplier}. The
 * legacy interface uses
 * {@code Processor#process(K key, V value)} — two raw arguments,
 * type-erased forward, and a thread-local {@code ProcessorContext}
 * that exposes headers, timestamp, and partition as separate calls.
 * The new interface uses
 * {@code Processor<KIn, VIn, KOut, VOut>#process(Record<KIn, VIn>
 * record)} — strongly-typed in/out key and value, headers and
 * timestamp carried on the Record, and a typed
 * {@code ProcessorContext<KOut, VOut>} for forwarding.
 *
 * <p>Five concrete incident classes the legacy shape made silent or
 * unrecoverable on the PAPI side specifically:
 *
 * <ul>
 *   <li><b>Type-erased forward on the lowest-level building block.</b>
 *       The PAPI {@code Topology} is the user-visible base layer Kafka
 *       Streams stacks the DSL ({@code KStream}, {@code KTable}) on
 *       top of. Custom shops that build their own DSL on top of the
 *       Topology (e.g. a config-driven topology builder reading a YAML
 *       file and emitting Topology calls) and stay on the legacy
 *       supplier inherit the type-erased
 *       {@code context.forward(k, v)} call site at every processor
 *       node. A refactor that changes a downstream child node's input
 *       types — even a type-narrowing refactor such as
 *       {@code <String, Object>} to {@code <String, Long>} — does not
 *       trigger a compile-time error at the forward site; the bug
 *       ships and surfaces as a {@code ClassCastException} deep inside
 *       a sibling processor node, often on the first non-test record
 *       after deploy.</li>
 *   <li><b>Headers and timestamp lookup via thread-local
 *       ProcessorContext.</b> The legacy
 *       {@code Processor#process(K, V)} forces the implementor to call
 *       {@code context.headers()} and {@code context.timestamp()} —
 *       two thread-local lookups whose semantics depend on whether the
 *       processor is being driven by a record handler or a punctuator
 *       callback. A punctuator that reads
 *       {@code context.timestamp()} returns wall-clock time, not the
 *       just-processed record's event time, which leaks into
 *       aggregations as silent off-by-event-time bugs. The new
 *       {@code Record<KIn, VIn>} carries headers, timestamp, key, and
 *       value as fields on one object decoupled from the thread-local
 *       context.</li>
 *   <li><b>Heterogeneous downstream forward — child node receives
 *       Object/Object instead of the typed types it declared.</b> A
 *       Topology has one source node feeding multiple child processors
 *       (a fan-out). The legacy {@code context.forward(k, v)} signature
 *       erases everything to Object, so a {@code .forward(k, v)} site
 *       that should fan out only to a string-typed child silently
 *       reaches a long-typed sibling child at runtime; the sibling's
 *       {@code process(K, V)} site casts on entry and throws
 *       {@code ClassCastException}. The new
 *       {@code context.forward(Record)} typed against
 *       {@code <KOut, VOut>} fails at compile time when the Record's
 *       types don't match the supplier's declared out-types.</li>
 *   <li><b>Topology.describe() output drift across the deprecation
 *       boundary.</b> A platform team maintaining a topology-diff
 *       tool that compares the textual output of
 *       {@code Topology.describe()} across releases sees the same
 *       processor-node name on both sides of an
 *       {@code api.ProcessorSupplier} migration, but the underlying
 *       interface change is invisible in the describe text. A
 *       node-name-only diff misses the deprecation; a centralized
 *       lint catches the legacy interface at the call site
 *       directly.</li>
 *   <li><b>INVOKEDYNAMIC {@code topology::addProcessor} captures
 *       silently bind to the deprecated overload.</b> A topology-
 *       factory abstraction (e.g. a custom-DSL builder consuming a
 *       {@code BiFunction<String, ProcessorSupplier, Topology>} or a
 *       {@code TriFunction<String, ProcessorSupplier, String[],
 *       Topology>} captured via a SAM whose supplier-typed argument
 *       erases to the legacy
 *       {@code org.apache.kafka.streams.processor.ProcessorSupplier})
 *       resolves the method-ref by arity and erased argument types.
 *       The method-ref binds to the deprecated overload at link time;
 *       the user-class bytecode contains zero direct
 *       {@code INVOKEVIRTUAL} on the legacy method — only the
 *       {@code INVOKEDYNAMIC} + {@code LambdaMetafactory} bridge whose
 *       bsm-args contain a {@code REF_invokeVirtual} handle pointing
 *       at the legacy method, whose descriptor contains the legacy
 *       supplier type.</li>
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
 * <p>{@code topology::addProcessor} bound to a SAM with the matching
 * arity / erasures compiles to {@code INVOKEDYNAMIC} whose bsm-args
 * contain a {@code REF_invokeVirtual} handle (Topology is a class,
 * not an interface). The rule walks the bsm-args at every indy site,
 * checks each Handle's {@code (owner, name)} against
 * {@code Topology × addProcessor}, and then applies the same
 * descriptor predicate as for direct calls.
 */
public final class TopologyAddProcessorLegacySupplierRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.TOPOLOGY);
    private static final String METHOD_NAME = "addProcessor";
    private static final String LEGACY_SUPPLIER_DESC = "Lorg/apache/kafka/streams/processor/ProcessorSupplier;";
    private static final String NEW_SUPPLIER_DESC = "Lorg/apache/kafka/streams/processor/api/ProcessorSupplier;";

    private final Severity severity;

    public TopologyAddProcessorLegacySupplierRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.TOPOLOGY_ADD_PROCESSOR_LEGACY_SUPPLIER;
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
                RuleId.TOPOLOGY_ADD_PROCESSOR_LEGACY_SUPPLIER, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Topology.addProcessor overload taking the legacy "
                        + "`org.apache.kafka.streams.processor.ProcessorSupplier` is "
                        + "reached here — either as a direct INVOKEVIRTUAL call or as "
                        + "an INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`topology::addProcessor` bound to a topology-factory SAM "
                        + "whose supplier-erased argument is the legacy "
                        + "ProcessorSupplier). The legacy overload is deprecated since "
                        + "Kafka Streams 3.0 (KIP-820, September 2021); the new "
                        + "overload takes "
                        + "`org.apache.kafka.streams.processor.api.ProcessorSupplier"
                        + "<KIn, VIn, KOut, VOut>` and yields a typed "
                        + "Processor<KIn, VIn, KOut, VOut> with strongly-typed "
                        + "Record<KIn, VIn> inputs and a typed "
                        + "ProcessorContext<KOut, VOut> for forwarding. Five concrete "
                        + "failure modes follow: (1) type-erased forward on the "
                        + "lowest-level building block — the PAPI Topology is the "
                        + "user-visible base layer Kafka Streams stacks the DSL on "
                        + "top of; custom shops that build their own DSL on top of "
                        + "Topology (e.g. a YAML-driven topology builder) and stay on "
                        + "the legacy supplier inherit `context.forward(k, v)` at "
                        + "every processor node; a refactor that changes a downstream "
                        + "child's input types does not trigger a compile-time error "
                        + "at the forward site and ships as a runtime "
                        + "ClassCastException deep inside a sibling processor; (2) "
                        + "headers and timestamp via thread-local ProcessorContext — "
                        + "a punctuator that reads `context.timestamp()` returns "
                        + "wall-clock time, not the just-processed record's event "
                        + "time, which leaks into aggregations as silent "
                        + "off-by-event-time bugs; the new Record carries headers, "
                        + "timestamp, key, and value on one object decoupled from the "
                        + "thread-local context; (3) heterogeneous downstream forward "
                        + "— a Topology fan-out source node feeding multiple typed "
                        + "child processors loses type safety at the legacy "
                        + "`forward(k, v)` site; a string-typed forward silently "
                        + "reaches a long-typed sibling at runtime and the sibling's "
                        + "process(K, V) casts on entry and throws "
                        + "ClassCastException; the new `context.forward(Record)` "
                        + "fails at compile time when the Record types don't match "
                        + "the supplier's declared out-types; (4) Topology.describe() "
                        + "drift across the deprecation boundary — a platform team "
                        + "maintaining a topology-diff tool that compares the textual "
                        + "output of Topology.describe() sees the same processor-node "
                        + "name on both sides of an api.ProcessorSupplier migration; "
                        + "the underlying interface change is invisible in the "
                        + "describe text and a name-only diff misses the deprecation; "
                        + "(5) INVOKEDYNAMIC `topology::addProcessor` captures "
                        + "silently bind to the deprecated overload — a "
                        + "topology-factory abstraction consuming a "
                        + "TriFunction<String, ProcessorSupplier, String[], Topology> "
                        + "resolves the method-ref by arity and erased argument types; "
                        + "the user-class bytecode contains zero direct "
                        + "INVOKEVIRTUAL on the legacy method, only an indy site "
                        + "whose bsm-args contain a REF_invokeVirtual handle whose "
                        + "descriptor contains the legacy supplier type. "
                        + "Migration: change the import from "
                        + "`org.apache.kafka.streams.processor.ProcessorSupplier` to "
                        + "`org.apache.kafka.streams.processor.api.ProcessorSupplier` "
                        + "and refactor the Processor's `process(K key, V value)` "
                        + "into `process(Record<KIn, VIn> record)`. The new "
                        + "ProcessorContext is generic in <KOut, VOut>, so "
                        + "`context.forward(Record)` is now typed and downstream "
                        + "child-node type-mismatches surface at compile time.");
    }
}
