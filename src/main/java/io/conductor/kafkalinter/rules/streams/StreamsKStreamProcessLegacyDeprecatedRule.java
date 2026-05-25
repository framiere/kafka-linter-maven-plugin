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
 * {@link org.apache.kafka.streams.kstream.KStream#process} overloads
 * taking the legacy
 * {@code org.apache.kafka.streams.processor.ProcessorSupplier}.
 *
 * <p>The rule catches both direct {@code INVOKEINTERFACE} /
 * {@code INVOKEVIRTUAL} calls and indirect {@code INVOKEDYNAMIC}
 * method-reference captures (e.g. {@code stream::process} bound to a
 * pipeline-builder SAM) via a dual walk over each method's
 * instructions.
 *
 * <h2>Why these overloads are deprecated (KIP-820 summary)</h2>
 *
 * <p>KIP-820 (Kafka Streams 3.3, October 2022) replaced the untyped
 * {@code KStream.process(legacy ProcessorSupplier, String...)} family
 * with new overloads taking
 * {@code org.apache.kafka.streams.processor.api.ProcessorSupplier<KIn,
 * VIn, KOut, VOut>}. The legacy form returns {@code void} (terminates
 * the DSL pipeline at the {@code process} call site), while the new
 * form returns {@code KStream<KOut, VOut>} so subsequent operators
 * ({@code .filter()}, {@code .map()}, {@code .to(...)}) chain
 * naturally. Five concrete incident classes the legacy shape made
 * silent or unrecoverable:
 *
 * <ul>
 *   <li><b>Pipeline-termination at a non-terminal node.</b> The legacy
 *       {@code KStream.process(...)} returns {@code void}, so a fluent
 *       chain that wants to apply downstream DSL operators after the
 *       processor — for example, {@code .filter(...).process(supplier).to("out")}
 *       — does not compile. The team-pressure workaround in legacy
 *       code bases was to attach the downstream {@code to()} sink
 *       inside the processor via {@code context.forward(k, v)} and a
 *       sibling {@code addSink} on the Topology, which mixes DSL
 *       (KStream) and PAPI (Topology) wiring in one builder. Mixed
 *       wiring fragments lineage, hides the sink from
 *       {@code Topology.describe()} DSL summaries, and breaks
 *       repartition planning. The new {@code process} returns
 *       {@code KStream<KOut, VOut>}: {@code .process(supplier).to("out")}
 *       is the natural one-line idiom.</li>
 *   <li><b>Type-erased forward — ClassCastException at runtime, not
 *       compile time.</b> A legacy processor's
 *       {@code context.forward(k, v)} accepts any {@code Object,
 *       Object} pair. A refactor that changes the downstream child
 *       node's input types (e.g. from {@code <String, Long>} to
 *       {@code <String, Double>}) does not trigger a compile-time
 *       error at the {@code forward} site — the bug ships and surfaces
 *       as a {@code ClassCastException} deep inside the downstream
 *       node, often on the first non-test record after deploy. The new
 *       API's {@code context.forward(Record<KOut, VOut>)} types
 *       {@code KOut/VOut} on the supplier itself and forwards a typed
 *       {@code Record}, so an incompatible type triggers a
 *       compile-time failure at the {@code forward} call site.</li>
 *   <li><b>Headers, timestamp, and partition exposed via separate
 *       thread-local context calls rather than a typed Record.</b> The
 *       legacy {@code process(K, V)} forces the implementor to
 *       retrieve headers via {@code context.headers()} and timestamp
 *       via {@code context.timestamp()} — two thread-local lookups
 *       whose semantics depend on whether the processor is being
 *       called from a punctuator or a record handler. A subtle bug
 *       class: a punctuator that reads {@code context.timestamp()}
 *       returns wall-clock time, not the just-processed record's event
 *       time, which leaks into aggregations as silent
 *       off-by-event-time bugs. The new {@code Record<KIn, VIn>}
 *       carries headers, timestamp, key, and value as fields on one
 *       object, decoupled from the thread-local context.</li>
 *   <li><b>No {@code processValues} variant — no compile-time
 *       guarantee of key preservation.</b> The legacy
 *       {@code KStream.process} returns a separate {@code KStream}
 *       whose key type is whatever the implementor chose to forward
 *       (the API is structurally key-agnostic). Downstream
 *       co-partitioned joins that assumed key preservation break
 *       silently with cross-partition writes when an implementor
 *       mutates the key inside {@code process(K, V)}. The new API
 *       ships a separate {@code KStream.processValues(FixedKeyProcessorSupplier,
 *       String...)} entry point whose {@code FixedKeyRecord}
 *       physically lacks a key-mutation API; the contract is
 *       enforced at the type system, not by convention.</li>
 *   <li><b>INVOKEDYNAMIC {@code KStream::process} captures silently
 *       bind to the deprecated overload.</b> A pipeline-factory
 *       abstraction (e.g. a generic {@code BiFunction<KStream,
 *       ProcessorSupplier, KStream>} or
 *       {@code Function<ProcessorSupplier, Void>}) resolves the
 *       method-ref by arity and erased argument types. When the SAM's
 *       supplier-typed argument erases to the legacy
 *       {@code org.apache.kafka.streams.processor.ProcessorSupplier},
 *       the method-ref binds to the deprecated overload at link time.
 *       The user-class bytecode contains zero direct
 *       {@code INVOKEINTERFACE} on the legacy method — only the
 *       {@code INVOKEDYNAMIC} + {@code LambdaMetafactory} bridge whose
 *       bsm-args contain a {@code REF_invokeInterface} handle pointing
 *       at the legacy method.</li>
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
 *   <li>{@code desc} does NOT contain the new (api-package)
 *       descriptor substring.</li>
 * </ul>
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code KStream::process} bound to a SAM with the matching arity /
 * erasures compiles to {@code INVOKEDYNAMIC} whose bsm-args contain a
 * {@code REF_invokeInterface} handle. The rule walks the bsm-args at
 * every indy site, checks each Handle's {@code (owner, name)} against
 * {@code KStream × process}, and then applies the same descriptor
 * predicate as for direct calls.
 */
public final class StreamsKStreamProcessLegacyDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final String METHOD_NAME = "process";
    private static final String LEGACY_SUPPLIER_DESC = "Lorg/apache/kafka/streams/processor/ProcessorSupplier;";
    private static final String NEW_SUPPLIER_DESC = "Lorg/apache/kafka/streams/processor/api/ProcessorSupplier;";

    private final Severity severity;

    public StreamsKStreamProcessLegacyDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_KSTREAM_PROCESS_LEGACY_DEPRECATED;
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
                RuleId.STREAMS_KSTREAM_PROCESS_LEGACY_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.process overload taking the legacy "
                        + "`org.apache.kafka.streams.processor.ProcessorSupplier` is "
                        + "reached here — either as a direct call or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`stream::process` bound to a pipeline-factory SAM whose "
                        + "supplier-erased argument is the legacy "
                        + "ProcessorSupplier). The legacy overloads are deprecated "
                        + "since Kafka Streams 3.3 (KIP-820, October 2022) in favour "
                        + "of `org.apache.kafka.streams.processor.api.ProcessorSupplier`, "
                        + "which yields a typed Processor<KIn, VIn, KOut, VOut> with "
                        + "strongly-typed Record<KIn, VIn> inputs and a typed "
                        + "ProcessorContext<KOut, VOut> for forwarding. Five concrete "
                        + "failure modes follow: (1) pipeline-termination at a "
                        + "non-terminal node — the legacy KStream.process returns "
                        + "void so a fluent chain that wants to apply downstream DSL "
                        + "operators after the processor (e.g. "
                        + "`.process(supplier).to(\"out\")`) does not compile; the "
                        + "team-pressure workaround was to attach the downstream "
                        + "sink inside the processor via `context.forward(k, v)` and "
                        + "a sibling `addSink` on the Topology, which mixes DSL "
                        + "(KStream) and PAPI (Topology) wiring in one builder, "
                        + "fragments lineage in `Topology.describe()`, and breaks "
                        + "repartition planning; the new process returns "
                        + "KStream<KOut, VOut> and `.process(supplier).to(\"out\")` "
                        + "is the natural one-line idiom; (2) type-erased forward — "
                        + "a legacy `context.forward(k, v)` accepts any Object/Object "
                        + "pair, so a refactor that changes the downstream child's "
                        + "input types does not break the build and ships as a "
                        + "runtime ClassCastException deep inside the downstream "
                        + "node, often on the first non-test record after deploy; "
                        + "the new `context.forward(Record<KOut, VOut>)` catches the "
                        + "mismatch at compile time; (3) headers, timestamp, and "
                        + "partition exposed via thread-local context lookups rather "
                        + "than a typed Record — a punctuator that reads "
                        + "`context.timestamp()` gets wall-clock time, not the "
                        + "just-processed record's event time, which leaks into "
                        + "aggregations as silent off-by-event-time bugs; the new "
                        + "Record carries headers, timestamp, key, and value on one "
                        + "object decoupled from the thread-local context; (4) no "
                        + "processValues variant on the legacy API — downstream "
                        + "co-partitioned joins that assumed key preservation break "
                        + "silently with cross-partition writes when an implementor "
                        + "mutates the key inside process(K, V); the new API ships a "
                        + "compile-time-distinct "
                        + "`KStream.processValues(FixedKeyProcessorSupplier, "
                        + "String...)` whose FixedKeyRecord physically lacks a "
                        + "key-mutation API; (5) INVOKEDYNAMIC `KStream::process` "
                        + "captures silently bind to the deprecated overload — the "
                        + "user-class bytecode contains zero direct INVOKEINTERFACE "
                        + "on the legacy method and a name-only walk misses it. "
                        + "Migration: change the import from "
                        + "`org.apache.kafka.streams.processor.ProcessorSupplier` to "
                        + "`org.apache.kafka.streams.processor.api.ProcessorSupplier` "
                        + "and refactor the Processor's `process(K key, V value)` "
                        + "into `process(Record<K, V> record)`. The new "
                        + "ProcessorContext is generic in <KOut, VOut>, the "
                        + ".process(...) call site now returns "
                        + "KStream<KOut, VOut> so DSL chaining is natural, and if "
                        + "the processor preserves the key switch to "
                        + ".processValues(FixedKeyProcessorSupplier, String...) for "
                        + "compile-time-enforced key preservation.");
    }
}
