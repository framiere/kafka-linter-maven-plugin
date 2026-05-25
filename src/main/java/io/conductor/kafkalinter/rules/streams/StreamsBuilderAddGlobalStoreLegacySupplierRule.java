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
 * {@link org.apache.kafka.streams.StreamsBuilder#addGlobalStore}
 * overload taking the legacy
 * {@code org.apache.kafka.streams.processor.ProcessorSupplier}.
 *
 * <p>The rule catches both direct {@code INVOKEVIRTUAL} calls and
 * indirect {@code INVOKEDYNAMIC} method-reference captures (e.g.
 * {@code builder::addGlobalStore} bound to a topology-factory SAM)
 * via a dual walk over each method's instructions.
 *
 * <h2>Why this overload is deprecated (KIP-820 — DSL-level entry
 * point)</h2>
 *
 * <p>KIP-820 (Kafka Streams 3.0, September 2021) added new overloads
 * to {@link org.apache.kafka.streams.StreamsBuilder#addGlobalStore}
 * accepting
 * {@code org.apache.kafka.streams.processor.api.ProcessorSupplier<KIn,
 * VIn, Void, Void>} alongside the legacy untyped supplier.
 * {@code StreamsBuilder.addGlobalStore} is the DSL-level entry point
 * for global stores, distinct from the PAPI-level
 * {@code Topology.addGlobalStore}. The deprecation timer is the same
 * on both, but the DSL form has subtly different incident classes:
 *
 * <ul>
 *   <li><b>DSL/PAPI confusion in mixed pipelines.</b> A typical
 *       Streams app stays in the DSL ({@code StreamsBuilder.stream},
 *       {@code .filter}, {@code .to}); the only PAPI hook many
 *       teams reach for is {@code StreamsBuilder.addGlobalStore}.
 *       When the global store registration uses the legacy untyped
 *       supplier, the resulting Processor lives in the otherwise-
 *       typed DSL pipeline as an island of {@code Object, Object}
 *       — a refactor that tightens downstream types upstream of
 *       the global store does not surface mismatches at this
 *       boundary at compile time.</li>
 *   <li><b>Consumed&lt;K, V&gt; mismatch with the legacy supplier
 *       is undetectable.</b> The DSL form takes a
 *       {@code Consumed<K, V>} parameter that types the
 *       deserializers; with the legacy raw supplier, javac cannot
 *       enforce that the supplier's {@code Processor<K, V>}
 *       type-parameters match the {@code Consumed<K, V>} types — a
 *       Consumed declaring {@code <String, Long>} keys/values can
 *       feed a Processor that {@code process(String, Object)} and
 *       the bug only surfaces as a {@code ClassCastException} at
 *       runtime when the global topic produces its first non-empty
 *       record after deploy.</li>
 *   <li><b>Recovery-blocking, every-instance replay — same as
 *       PAPI-level addGlobalStore.</b> Global stores are populated
 *       by replaying the source topic on every Streams instance
 *       before any partitioned task can be assigned. A
 *       ClassCastException from the legacy untyped processor blocks
 *       recovery indefinitely and is N-times-multiplied across
 *       every replica.</li>
 *   <li><b>Sibling DSL operators reference the global store name —
 *       a rename to a typed processor is not transparent.</b> A
 *       sibling {@code stream.join(globalKTable, ...)} that
 *       references the same store name (e.g. via
 *       {@code Materialized.as("global-foo")}) needs no edit when
 *       the supplier is migrated to the new typed API, but the
 *       internal node name in {@code Topology.describe()} stays
 *       identical too — a topology-diff tool that compares
 *       describe() text across releases sees a no-op rename even
 *       though the underlying processor interface has changed; a
 *       centralized lint at the call site is required.</li>
 *   <li><b>INVOKEDYNAMIC {@code builder::addGlobalStore} captures
 *       silently bind to the deprecated overload.</b> A topology-
 *       factory abstraction (e.g. a DSL-level global-store
 *       registration helper consuming a SAM whose supplier-erased
 *       argument is the legacy ProcessorSupplier) resolves the
 *       method-ref at link time. The user-class bytecode contains
 *       zero direct {@code INVOKEVIRTUAL} on the legacy method —
 *       only the {@code INVOKEDYNAMIC} + {@code LambdaMetafactory}
 *       bridge whose bsm-args contain a {@code REF_invokeVirtual}
 *       handle whose descriptor contains the legacy supplier
 *       type.</li>
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
 * <p>{@code builder::addGlobalStore} bound to a SAM with the
 * matching arity / erasures compiles to {@code INVOKEDYNAMIC} whose
 * bsm-args contain a {@code REF_invokeVirtual} handle (StreamsBuilder
 * is a class, not an interface). The rule walks the bsm-args at
 * every indy site, checks each Handle's {@code (owner, name)}
 * against {@code StreamsBuilder × addGlobalStore}, and then applies
 * the same descriptor predicate as for direct calls.
 */
public final class StreamsBuilderAddGlobalStoreLegacySupplierRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.STREAMS_BUILDER);
    private static final String METHOD_NAME = "addGlobalStore";
    private static final String LEGACY_SUPPLIER_DESC = "Lorg/apache/kafka/streams/processor/ProcessorSupplier;";
    private static final String NEW_SUPPLIER_DESC = "Lorg/apache/kafka/streams/processor/api/ProcessorSupplier;";

    private final Severity severity;

    public StreamsBuilderAddGlobalStoreLegacySupplierRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMSBUILDER_ADDGLOBALSTORE_LEGACY_SUPPLIER;
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
                RuleId.STREAMSBUILDER_ADDGLOBALSTORE_LEGACY_SUPPLIER, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "StreamsBuilder.addGlobalStore overload taking the legacy "
                        + "`org.apache.kafka.streams.processor.ProcessorSupplier` "
                        + "is reached here — either as a direct INVOKEVIRTUAL call "
                        + "or as an INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`builder::addGlobalStore` bound to a topology-factory "
                        + "SAM whose supplier-erased argument is the legacy "
                        + "ProcessorSupplier). The legacy overload is deprecated "
                        + "since Kafka Streams 3.0 (KIP-820, September 2021); the "
                        + "new overload takes "
                        + "`org.apache.kafka.streams.processor.api.ProcessorSupplier"
                        + "<KIn, VIn, Void, Void>` and yields a typed "
                        + "Processor<KIn, VIn, Void, Void> with strongly-typed "
                        + "Record<KIn, VIn> inputs and a Void-typed forward. "
                        + "StreamsBuilder.addGlobalStore is the DSL-level entry "
                        + "point for global stores, distinct from the PAPI-level "
                        + "Topology.addGlobalStore; the deprecation timer is the "
                        + "same on both, but the DSL form has subtly different "
                        + "incident classes. Five concrete failure modes follow: "
                        + "(1) DSL/PAPI confusion in mixed pipelines — a typical "
                        + "Streams app stays in the DSL "
                        + "(StreamsBuilder.stream, .filter, .to); the only PAPI "
                        + "hook many teams reach for is "
                        + "StreamsBuilder.addGlobalStore; when the global store "
                        + "registration uses the legacy untyped supplier, the "
                        + "resulting Processor lives in the otherwise-typed DSL "
                        + "pipeline as an island of Object/Object, and a refactor "
                        + "that tightens downstream types upstream of the global "
                        + "store does not surface mismatches at this boundary at "
                        + "compile time; (2) Consumed<K, V> mismatch with the "
                        + "legacy supplier is undetectable — the DSL form takes a "
                        + "Consumed<K, V> parameter that types the deserializers; "
                        + "with the legacy raw supplier, javac cannot enforce that "
                        + "the supplier's Processor<K, V> type-parameters match "
                        + "the Consumed<K, V> types; a Consumed declaring "
                        + "<String, Long> can feed a Processor that "
                        + "process(String, Object) and the bug only surfaces as a "
                        + "ClassCastException at runtime when the global topic "
                        + "produces its first non-empty record after deploy; (3) "
                        + "recovery-blocking, every-instance replay — global "
                        + "stores are populated by replaying the source topic on "
                        + "every Streams instance before any partitioned task can "
                        + "be assigned; a ClassCastException from the legacy "
                        + "untyped processor blocks recovery indefinitely and is "
                        + "N-times-multiplied across every replica; (4) sibling "
                        + "DSL operators reference the global store name — a "
                        + "sibling stream.join(globalKTable, ...) that references "
                        + "the same store name needs no edit when the supplier "
                        + "is migrated to the new typed API, but the internal "
                        + "node name in Topology.describe() stays identical too; "
                        + "a topology-diff tool that compares describe() text "
                        + "across releases sees a no-op rename even though the "
                        + "underlying processor interface has changed; a "
                        + "centralized lint at the call site is required; (5) "
                        + "INVOKEDYNAMIC `builder::addGlobalStore` captures "
                        + "silently bind to the deprecated overload — a "
                        + "topology-factory abstraction consuming a SAM whose "
                        + "supplier-erased argument is the legacy "
                        + "ProcessorSupplier resolves the method-ref by arity "
                        + "and erased argument types; the user-class bytecode "
                        + "contains zero direct INVOKEVIRTUAL on the legacy "
                        + "method, only an indy site whose bsm-args contain a "
                        + "REF_invokeVirtual handle whose descriptor contains "
                        + "the legacy supplier type. Migration: change the "
                        + "import from "
                        + "`org.apache.kafka.streams.processor.ProcessorSupplier` "
                        + "to "
                        + "`org.apache.kafka.streams.processor.api.ProcessorSupplier` "
                        + "with type parameters <KIn, VIn, Void, Void> matching "
                        + "the Consumed<KIn, VIn> on the same call site and "
                        + "refactor the Processor's `process(K key, V value)` "
                        + "into `process(Record<KIn, VIn> record)`. The Void "
                        + "out-types fail at compile time on any accidental "
                        + "context.forward(...) call site.");
    }
}
