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
 * Fires for every reach of the deprecated legacy Processor-API overloads
 * on {@link org.apache.kafka.streams.Topology} and
 * {@link org.apache.kafka.streams.StreamsBuilder}:
 *
 * <ul>
 *   <li>{@code Topology.addProcessor(String, ProcessorSupplier, String...)}
 *       — overload taking the legacy
 *       {@code org.apache.kafka.streams.processor.ProcessorSupplier}.</li>
 *   <li>{@code Topology.addGlobalStore(...)} — every overload whose
 *       supplier argument is the legacy
 *       {@code org.apache.kafka.streams.processor.ProcessorSupplier}
 *       (multiple shapes exist with varying timestamp-extractor /
 *       reset-offset arguments — the descriptor filter catches them
 *       all).</li>
 *   <li>{@code StreamsBuilder.addGlobalStore(...)} — every overload
 *       whose supplier argument is the legacy
 *       {@code org.apache.kafka.streams.processor.ProcessorSupplier}.</li>
 * </ul>
 *
 * <p>The rule catches both direct {@code INVOKEVIRTUAL} calls and
 * indirect {@code INVOKEDYNAMIC} method-reference captures
 * (e.g. {@code topology::addProcessor} bound to a topology-builder SAM)
 * via a dual walk over each method's instructions.
 *
 * <h2>Why these overloads are deprecated</h2>
 *
 * <p>KIP-820 (Kafka Streams 3.3, October 2022) introduced a typed
 * {@code org.apache.kafka.streams.processor.api.ProcessorSupplier} that
 * yields {@code Processor<KIn, VIn, KOut, VOut>} with strongly-typed
 * {@code Record<KIn, VIn>} inputs and a typed
 * {@code ProcessorContext<KOut, VOut>} for forwarding. The legacy
 * {@code org.apache.kafka.streams.processor.ProcessorSupplier} returns
 * {@code Processor} (no type parameters) whose
 * {@code process(K key, V value)} is erased to {@code Object/Object}
 * at the bytecode level. Five concrete incident classes the legacy
 * shape made silent or unrecoverable:
 *
 * <ul>
 *   <li><b>Type-erased forward — ClassCastException at runtime, not
 *       compile time.</b> A legacy processor's
 *       {@code context.forward(k, v)} accepts any
 *       {@code Object, Object} pair. A refactor that changes the
 *       downstream child node's input types (for example, from
 *       {@code <String, Long>} to {@code <String, Double>}) does not
 *       trigger a compile-time error at the {@code forward} site —
 *       the bug ships and surfaces as a {@code ClassCastException}
 *       deep inside the downstream node, often on the first non-test
 *       record after deploy. The new API's
 *       {@code context.forward(Record<KOut, VOut>)} types
 *       {@code KOut/VOut} on the supplier itself and forwards a
 *       typed {@code Record}, so an incompatible type triggers a
 *       compile-time failure at the {@code forward} call site.</li>
 *   <li><b>Headers, timestamp, and partition exposed via separate
 *       calls rather than a typed Record.</b> The legacy
 *       {@code process(K, V)} forces the implementor to retrieve
 *       headers via {@code context.headers()} and timestamp via
 *       {@code context.timestamp()} — two thread-local lookups whose
 *       semantics depend on whether the processor is being called
 *       from a punctuator or a record handler. A subtle bug class:
 *       a punctuator that mutates context state for a
 *       record-handler-style read of {@code context.timestamp()}
 *       returns the punctuator's wall-clock time, not the
 *       just-processed record's event time. The new
 *       {@code Record<KIn, VIn>} carries headers, timestamp, key,
 *       and value as fields on one object, decoupled from the
 *       thread-local context.</li>
 *   <li><b>Named-child fan-out is positional in the legacy API.</b>
 *       Legacy {@code context.forward(k, v, To.child(0))} addresses
 *       the downstream child by its position in the parent's child
 *       list — the same array-index coupling that doomed the legacy
 *       {@code KStream.branch(Predicate[])} API. A refactor that
 *       reorders the downstream attachments silently reroutes
 *       records. The new API forwards via
 *       {@code context.forward(record, childName)} keyed by string
 *       — reorder-safe.</li>
 *   <li><b>No FixedKeyProcessor variant.</b> The legacy API has no
 *       way to express "this processor preserves the input key but
 *       transforms the value" — the implementation is free to mutate
 *       the key inside {@code process(K, V)}, and downstream
 *       co-partitioned joins that assumed key preservation break
 *       silently with cross-partition writes. The new API ships a
 *       compile-time-distinct {@code FixedKeyProcessor<KIn, VIn,
 *       VOut>} with a {@code FixedKeyRecord} that physically lacks a
 *       key-mutation API. Subscribing to this contract is a
 *       single-import change with structural safety.</li>
 *   <li><b>INVOKEDYNAMIC {@code Topology::addProcessor} captures
 *       silently bind to the deprecated overload.</b> A
 *       topology-factory abstraction (e.g. a generic
 *       {@code TriFunction<Topology, String, ProcessorSupplier,
 *       Topology>}) resolves the method-ref by arity and erased
 *       argument types. When the SAM's supplier-typed argument
 *       erases to the legacy
 *       {@code org.apache.kafka.streams.processor.ProcessorSupplier},
 *       the method-ref binds to the deprecated overload at link
 *       time. The user-class bytecode contains zero direct
 *       {@code INVOKEVIRTUAL} on the legacy method — only the
 *       {@code INVOKEDYNAMIC} + {@code LambdaMetafactory} bridge
 *       whose bsm-args contain a {@code REF_invokeVirtual} handle
 *       pointing at the legacy method.</li>
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
 * <p>The new package's internal name has the legacy package's name as
 * a prefix substring. A naive {@code .contains("processor/ProcessorSupplier;")}
 * check matches both, so the predicate is the conjunction:
 *
 * <ul>
 *   <li>{@code desc} contains the legacy descriptor substring AND</li>
 *   <li>{@code desc} does NOT contain the new
 *       (api-package) descriptor substring.</li>
 * </ul>
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code Topology::addProcessor} bound to a SAM with the matching
 * arity / erasures compiles to {@code INVOKEDYNAMIC} whose bsm-args
 * contain a {@code REF_invokeVirtual} handle. The rule walks the
 * bsm-args at every indy site, checks each Handle's
 * {@code (owner, name)} against the (Topology|StreamsBuilder) ×
 * (addProcessor|addGlobalStore) cartesian, and then applies the same
 * descriptor predicate as for direct calls.
 */
public final class StreamsLegacyProcessorApiDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.TOPOLOGY, KafkaTypes.STREAMS_BUILDER);
    private static final Set<String> METHOD_NAMES = Set.of("addProcessor", "addGlobalStore");
    private static final String LEGACY_SUPPLIER_DESC = "Lorg/apache/kafka/streams/processor/ProcessorSupplier;";
    private static final String NEW_SUPPLIER_DESC = "Lorg/apache/kafka/streams/processor/api/ProcessorSupplier;";

    private final Severity severity;

    public StreamsLegacyProcessorApiDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_LEGACY_PROCESSOR_API_DEPRECATED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAMES.contains(mi.name)
                        && isLegacyProcessorSupplierDesc(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy && indy.bsmArgs != null) {
                    for (Object arg : indy.bsmArgs) {
                        if (!(arg instanceof Handle h)) continue;
                        if (!OWNERS.contains(h.getOwner())) continue;
                        if (!METHOD_NAMES.contains(h.getName())) continue;
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
                RuleId.STREAMS_LEGACY_PROCESSOR_API_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Topology.addProcessor / Topology.addGlobalStore / "
                        + "StreamsBuilder.addGlobalStore overload taking the legacy "
                        + "`org.apache.kafka.streams.processor.ProcessorSupplier` is "
                        + "reached here — either as a direct call or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`topology::addProcessor` bound to a topology-factory SAM "
                        + "whose supplier-erased argument is the legacy "
                        + "ProcessorSupplier). The legacy overloads are deprecated "
                        + "since Kafka Streams 3.3 (KIP-820, October 2022) in favour "
                        + "of `org.apache.kafka.streams.processor.api.ProcessorSupplier`, "
                        + "which yields a typed Processor<KIn, VIn, KOut, VOut> with "
                        + "strongly-typed Record<KIn, VIn> inputs and a typed "
                        + "ProcessorContext<KOut, VOut> for forwarding. Five concrete "
                        + "failure modes follow: (1) type-erased forward — a legacy "
                        + "`context.forward(k, v)` accepts any Object/Object pair, so "
                        + "a refactor that changes the downstream child's input types "
                        + "does not break the build and ships as a runtime "
                        + "ClassCastException deep inside the downstream node, often "
                        + "on the first non-test record after deploy; the new "
                        + "`context.forward(Record<KOut, VOut>)` catches the mismatch "
                        + "at compile time; (2) headers, timestamp, and partition "
                        + "exposed via thread-local context lookups rather than a "
                        + "typed Record — a punctuator that reads "
                        + "`context.timestamp()` gets wall-clock time, not the "
                        + "just-processed record's event time, which leaks into "
                        + "aggregations as silent off-by-event-time bugs; the new "
                        + "Record carries headers, timestamp, key, and value on one "
                        + "object decoupled from the thread-local context; (3) "
                        + "named-child fan-out is positional in the legacy API — "
                        + "`context.forward(k, v, To.child(0))` addresses the "
                        + "downstream child by its position in the parent's child "
                        + "list, the same array-index coupling that doomed legacy "
                        + "`KStream.branch(Predicate[])`; reorders silently reroute "
                        + "records; the new `context.forward(record, childName)` is "
                        + "string-keyed and reorder-safe; (4) no FixedKeyProcessor — "
                        + "the legacy API cannot express 'preserves key, transforms "
                        + "value' so downstream co-partitioned joins that assumed "
                        + "key preservation break silently with cross-partition "
                        + "writes when an implementor mutates the key inside "
                        + "process(K, V); the new API ships a "
                        + "compile-time-distinct FixedKeyProcessor<KIn, VIn, VOut> "
                        + "whose FixedKeyRecord physically lacks a key-mutation API; "
                        + "(5) INVOKEDYNAMIC `Topology::addProcessor` captures "
                        + "silently bind to the deprecated overload — the user-class "
                        + "bytecode contains zero direct INVOKEVIRTUAL on the legacy "
                        + "method and a name-only walk misses it. Migration: change "
                        + "the import from `org.apache.kafka.streams.processor."
                        + "ProcessorSupplier` to "
                        + "`org.apache.kafka.streams.processor.api.ProcessorSupplier` "
                        + "and refactor the Processor's `process(K key, V value)` "
                        + "into `process(Record<K, V> record)` (or "
                        + "`FixedKeyProcessor.process(FixedKeyRecord<K, V> record)` "
                        + "for the key-preserving variant). The new "
                        + "ProcessorContext is generic in <KOut, VOut> and the "
                        + "forward call site catches downstream-type mismatches at "
                        + "compile time.");
    }
}
