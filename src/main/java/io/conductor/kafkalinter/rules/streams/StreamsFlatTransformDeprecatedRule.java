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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Fires for every reach of the deprecated fan-out value-rewriting
 * pair {@code KStream.flatTransform(...)} (2 overloads) and
 * {@code KStream.flatTransformValues(...)} (4 overloads) — whether
 * the call lands directly via {@code INVOKEINTERFACE} or indirectly
 * through an {@code INVOKEDYNAMIC} method-reference capture (e.g.
 * {@code stream::flatTransform} bound to a topology-builder SAM that
 * takes a {@code TransformerSupplier} returning {@code Iterable})
 * or {@code stream::flatTransformValues} bound to a builder that
 * takes a value-supplier returning {@code Iterable}.
 *
 * <h2>Why these methods are deprecated, not just a name change</h2>
 *
 * <p>{@code flatTransform} and {@code flatTransformValues} are the
 * fan-out siblings of {@code transform} / {@code transformValues}:
 * the supplied transformer returns
 * {@code Iterable<KeyValue<K1, V1>>} (or {@code Iterable<VR>} for
 * the value-only variant), and the framework iterates the result to
 * emit zero, one, or many forwards per input record. They exist
 * because the no-fan-out parents return only one record at a time
 * and {@code null} to drop — fan-out had to be a separate API. The
 * very fact that the no-fan-out / fan-out split exists at the API
 * level is the design failure KIP-820 fixed.
 *
 * <p>KIP-820 (Kafka Streams 3.3, August 2022) replaced the entire
 * legacy {@code transform} / {@code transformValues} /
 * {@code flatTransform} / {@code flatTransformValues} quartet with
 * the single modern {@code process(ProcessorSupplier)} and
 * {@code processValues(FixedKeyProcessorSupplier)} pair from
 * {@code org.apache.kafka.streams.processor.api}. The new
 * {@code Processor.process(Record)} (and
 * {@code FixedKeyProcessor.process(FixedKeyRecord)}) contract has
 * no return — the processor calls {@code context.forward(...)} zero,
 * one, or many times. Fan-out is no longer a separate method; it's
 * just calling {@code forward} more than once in the loop.
 *
 * <p>Five concrete reasons KIP-820 forced the migration:
 *
 * <ul>
 *   <li><b>The legacy API forced an allocation-per-output design.</b>
 *       The supplied transformer must materialise its full output
 *       as an {@code Iterable} (typically backed by an
 *       {@code ArrayList}) before returning, even when the
 *       downstream pipeline only consumes a few records. The new
 *       {@code Processor.process(Record)} contract calls
 *       {@code context.forward(Record)} as records become
 *       available — there is no intermediate collection allocation
 *       and downstream backpressure does not require buffering the
 *       full fan-out in user memory.</li>
 *   <li><b>The {@code flatTransformValues} key-invariant is only
 *       convention-enforced.</b> The
 *       {@code ValueTransformerWithKey}-backed
 *       {@code flatTransformValues} can call
 *       {@code context.forward(otherKey, value)} on the
 *       {@code ProcessorContext} and the framework has no way to
 *       detect the violation at the type level. The new
 *       {@code FixedKeyRecord} has no setter for the key — the
 *       type system statically prevents the violation, eliminating
 *       silent partition-skew bugs from leaked-key forwards on the
 *       value-only path.</li>
 *   <li><b>The {@code flatTransform(TransformerSupplier)} pair
 *       inherits all five problems documented on
 *       {@code STREAMS_TRANSFORM_DEPRECATED}:</b> mutable
 *       {@code ProcessorContext} record-state coupling, no
 *       per-record timestamp override, deprecated legacy
 *       {@code Processor} interface that
 *       {@code TransformerSupplier} implements internally.</li>
 *   <li><b>{@code INVOKEDYNAMIC stream::flatTransform} captures
 *       silently bind to the deprecated method.</b> A
 *       topology-builder abstraction (e.g. a {@code FlatBuilder<K, V,
 *       K1, V1>} SAM that takes a {@code TransformerSupplier}
 *       returning {@code Iterable<KeyValue<K1, V1>>} plus the
 *       state-store names) resolves the method-ref by arity and
 *       argument-erasure to one of the six legacy overloads. The
 *       user-class bytecode contains zero direct
 *       {@code INVOKEINTERFACE} on the legacy method — only the
 *       {@code INVOKEDYNAMIC} + {@code LambdaMetafactory} bridge. A
 *       name-only {@code MethodInsnNode} walk misses this case
 *       entirely.</li>
 *   <li><b>The legacy {@code Transformer} and
 *       {@code ValueTransformer} / {@code ValueTransformerWithKey}
 *       interfaces are themselves deprecated.</b> Even if a project
 *       could continue using {@code flatTransform()} /
 *       {@code flatTransformValues()} for binary-compatibility
 *       reasons, the supplied transformer type is on a deprecation
 *       timer of its own.</li>
 * </ul>
 *
 * <h2>The replacement API</h2>
 *
 * <p>{@code KStream.process(ProcessorSupplier, String...)} replaces
 * {@code flatTransform(...)} for the key-rewriting case;
 * {@code KStream.processValues(FixedKeyProcessorSupplier,
 * String...)} replaces {@code flatTransformValues(...)} for the
 * value-only case. Migration sketch:
 *
 * <pre>{@code
 *   // BEFORE — legacy KIP-820 deprecated flatTransformValues
 *   stream.flatTransformValues(() -> new ValueTransformer<V, Iterable<VR>>() {
 *       public Iterable<VR> transform(V v) {
 *           List<VR> out = new ArrayList<>();
 *           for (...) out.add(...);
 *           return out;  // or empty list to drop
 *       }
 *   }, "store-name");
 *
 *   // AFTER — KIP-820 modern processValues
 *   stream.processValues(() -> new FixedKeyProcessor<K, V, VR>() {
 *       private FixedKeyProcessorContext<K, VR> ctx;
 *       public void init(FixedKeyProcessorContext<K, VR> c) { this.ctx = c; }
 *       public void process(FixedKeyRecord<K, V> r) {
 *           for (...) ctx.forward(r.withValue(newV));
 *           // emit nothing to drop
 *       }
 *   }, "store-name");
 * }</pre>
 *
 * <p>No intermediate {@code ArrayList} allocation per call; fan-out
 * is just calling {@code forward} more than once.
 *
 * <h2>Descriptor discrimination — six legacy overloads</h2>
 *
 * <ul>
 *   <li>{@code flatTransform(TransformerSupplier, String...)}</li>
 *   <li>{@code flatTransform(TransformerSupplier, Named, String...)}</li>
 *   <li>{@code flatTransformValues(ValueTransformerSupplier, String...)}</li>
 *   <li>{@code flatTransformValues(ValueTransformerSupplier, Named, String...)}</li>
 *   <li>{@code flatTransformValues(ValueTransformerWithKeySupplier, String...)}</li>
 *   <li>{@code flatTransformValues(ValueTransformerWithKeySupplier, Named, String...)}</li>
 * </ul>
 *
 * <p>The two method names share a single rule because they share a
 * single fan-out semantic and a single migration target (the
 * no-return processor API). Discrimination is by exact name +
 * descriptor — the owner is always {@code KStream}, the name is
 * exactly one of the two listed, and the descriptor is exactly one
 * of the six pinned strings.
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code stream::flatTransform} or
 * {@code stream::flatTransformValues} bound to a SAM that takes the
 * appropriate supplier-plus-storeNames shape compiles to
 * {@code INVOKEDYNAMIC} whose bsm-args contain a
 * {@code REF_invokeInterface} handle pointing at the resolved
 * legacy method. The rule's bsm-arg walk catches this case by
 * checking the handle's {@code (owner, name, desc)} triple against
 * the same filter used for direct calls, iterated over both names
 * and all their descriptors.
 */
public final class StreamsFlatTransformDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);

    private static final Map<String, Set<String>> NAMES_TO_LEGACY_DESCS = Map.of(
            "flatTransform", Set.of(
                    "(Lorg/apache/kafka/streams/kstream/TransformerSupplier;[Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KStream;",
                    "(Lorg/apache/kafka/streams/kstream/TransformerSupplier;Lorg/apache/kafka/streams/kstream/Named;[Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KStream;"),
            "flatTransformValues", Set.of(
                    "(Lorg/apache/kafka/streams/kstream/ValueTransformerSupplier;[Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KStream;",
                    "(Lorg/apache/kafka/streams/kstream/ValueTransformerSupplier;Lorg/apache/kafka/streams/kstream/Named;[Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KStream;",
                    "(Lorg/apache/kafka/streams/kstream/ValueTransformerWithKeySupplier;[Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KStream;",
                    "(Lorg/apache/kafka/streams/kstream/ValueTransformerWithKeySupplier;Lorg/apache/kafka/streams/kstream/Named;[Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KStream;"));

    private final Severity severity;

    public StreamsFlatTransformDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_FLAT_TRANSFORM_DEPRECATED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)) {
                    Set<String> descs = NAMES_TO_LEGACY_DESCS.get(mi.name);
                    if (descs != null && descs.contains(mi.desc)) {
                        out.add(violation(ctx, mn, insn));
                        continue;
                    }
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    boolean matched = false;
                    for (Entry<String, Set<String>> e : NAMES_TO_LEGACY_DESCS.entrySet()) {
                        for (String desc : e.getValue()) {
                            Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, e.getKey(), desc);
                            if (h != null) {
                                matched = true;
                                break;
                            }
                        }
                        if (matched) break;
                    }
                    if (matched) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_FLAT_TRANSFORM_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.flatTransform(...) or KStream.flatTransformValues(...) is "
                        + "reached here — either as a direct call or as an INVOKEDYNAMIC "
                        + "method-reference capture (e.g. `stream::flatTransform` or "
                        + "`stream::flatTransformValues` bound to a topology-builder "
                        + "SAM that takes a TransformerSupplier returning Iterable plus "
                        + "the state-store names). These methods are the fan-out "
                        + "siblings of transform()/transformValues() — they exist only "
                        + "because the no-fan-out parents return a single record and "
                        + "null-to-drop, so fan-out had to be a separate API. KIP-820 "
                        + "(Kafka Streams 3.3, August 2022) deprecated the entire "
                        + "transform/transformValues/flatTransform/flatTransformValues "
                        + "quartet and replaced it with the single "
                        + "process(ProcessorSupplier) / processValues(FixedKey"
                        + "ProcessorSupplier) pair from "
                        + "org.apache.kafka.streams.processor.api whose "
                        + "Processor.process(Record) (and "
                        + "FixedKeyProcessor.process(FixedKeyRecord)) contracts have "
                        + "no return: the processor calls context.forward(...) zero, "
                        + "one, or many times. Five reasons for continued use of "
                        + "flatTransform*: (1) the legacy API forces an "
                        + "allocation-per-output design — the transformer must "
                        + "materialise its full output as an Iterable (typically "
                        + "backed by an ArrayList) before returning, even when the "
                        + "downstream pipeline only consumes a few records; the new "
                        + "processor calls forward() as records become available with "
                        + "no intermediate collection allocation; (2) the "
                        + "flatTransformValues key-invariant is only "
                        + "convention-enforced — a ValueTransformerWithKey-backed "
                        + "flatTransformValues can call "
                        + "context.forward(otherKey, value) on the legacy "
                        + "ProcessorContext and the framework has no way to detect the "
                        + "violation at the type level (silent partition skew when a "
                        + "downstream groupByKey/join is co-partitioned on the leaked "
                        + "key); the new FixedKeyRecord has no setter for the key — "
                        + "the type system statically prevents the violation; (3) the "
                        + "flatTransform(TransformerSupplier) family inherits all five "
                        + "STREAMS_TRANSFORM_DEPRECATED problems (mutable "
                        + "ProcessorContext record-state coupling, no per-record "
                        + "timestamp override, deprecated legacy Processor interface "
                        + "that TransformerSupplier implements internally); "
                        + "(4) INVOKEDYNAMIC `stream::flatTransform*` captures "
                        + "silently bind to the legacy method whenever the SAM has "
                        + "matching arity and argument erasure — the user-class "
                        + "bytecode contains zero direct INVOKEINTERFACE on the legacy "
                        + "method and a name-only walk misses it; (5) the legacy "
                        + "Transformer / ValueTransformer / ValueTransformerWithKey "
                        + "interfaces are themselves deprecated — the supplied "
                        + "transformer type is on a deprecation timer of its own. "
                        + "Migrate to `stream.process(ProcessorSupplier, storeNames)` "
                        + "for the key-rewriting case or "
                        + "`stream.processValues(FixedKeyProcessorSupplier, "
                        + "storeNames)` for the value-only case; rewrite the "
                        + "`return collection` Iterable body as a loop calling "
                        + "context.forward(...) once per output and rewrite "
                        + "`return Collections.emptyList()` as simply not "
                        + "forwarding. The store-name argument list is identical, "
                        + "so the topology wiring is unchanged. Make this migration "
                        + "in lockstep with the parallel `transform → process` and "
                        + "`transformValues → processValues` migrations.");
    }
}
