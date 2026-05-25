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
 * {@code KStream.transformValues(ValueTransformerSupplier, String...)}
 * and its three sibling overloads
 * ({@code ValueTransformerSupplier} variant with {@code Named},
 * {@code ValueTransformerWithKeySupplier} variant without
 * {@code Named}, and {@code ValueTransformerWithKeySupplier} variant
 * with {@code Named}) — whether the call lands directly via
 * {@code INVOKEINTERFACE} or indirectly through an
 * {@code INVOKEDYNAMIC} method-reference capture (e.g.
 * {@code stream::transformValues} bound to a topology-builder SAM
 * that takes a {@code ValueTransformerSupplier} /
 * {@code ValueTransformerWithKeySupplier} plus a store-name varargs
 * array).
 *
 * <h2>Why this method is deprecated, not just a name change</h2>
 *
 * <p>{@code KStream.transformValues(ValueTransformerSupplier,
 * String...)} is the legacy value-only stateful map: it takes a
 * {@code ValueTransformer.transform(V) -> VR} (or its
 * key-aware sibling
 * {@code ValueTransformerWithKey.transform(K, V) -> VR}) and is
 * documented as the right choice when "you want a {@code transform()}
 * that does not change the key, so the partition assignment
 * downstream stays stable for {@code groupByKey} / join". The
 * problem is that the legacy API only enforced this key-invariant by
 * convention — {@code ValueTransformerWithKey} could call
 * {@code context.forward(otherKey, value)} on the
 * {@code ProcessorContext} and the framework had no way to detect
 * the violation at the type level. KIP-820 (Kafka Streams 3.3,
 * August 2022) replaced this with
 * {@code KStream.processValues(FixedKeyProcessorSupplier)} which
 * uses {@code FixedKeyProcessor.process(FixedKeyRecord)} where
 * {@code FixedKeyRecord} exposes {@code withValue(newV)} but has no
 * setter or copy-constructor for the key — the type system
 * statically prevents the violation.
 *
 * <p>Five consequences for continued use:
 *
 * <ul>
 *   <li><b>Silent partition skew when a forward leaks a new key.</b>
 *       The legacy {@code ValueTransformerWithKey} body can call
 *       {@code context.forward(otherKey, value)} and the framework
 *       accepts it. A downstream {@code groupByKey()} then
 *       redistributes records by the new key, but the source-topic
 *       partition assignment was based on the original key — the
 *       resulting state-store keys are not co-partitioned with the
 *       inputs that produced them. The symptom is partition lag
 *       distribution skew that is only diagnosable by inspecting the
 *       consumer-group offset profile of the downstream topology.
 *       The new {@code FixedKeyRecord} type forbids this by
 *       construction — the user code physically cannot construct a
 *       new key.</li>
 *   <li><b>The legacy {@code ValueTransformer} contract uses the
 *       return value as the forward.</b> Returning a value emits;
 *       returning {@code null} drops. Fan-out requires the separate
 *       deprecated {@code flatTransformValues} method. The new
 *       {@code FixedKeyProcessor.process(FixedKeyRecord)} has no
 *       return — the processor calls
 *       {@code context.forward(record.withValue(newV))} zero, one,
 *       or many times. Fan-out and drop are first-class.</li>
 *   <li><b>No timestamp override at forward time.</b> The legacy
 *       returned value inherits the input record's timestamp. The
 *       new {@code FixedKeyRecord} exposes
 *       {@code withTimestamp(long)} for late-event projection in
 *       windowed pipelines.</li>
 *   <li><b>The legacy
 *       {@code org.apache.kafka.streams.kstream.ValueTransformer}
 *       and {@code ValueTransformerWithKey} interfaces are
 *       themselves deprecated.</b> Even if a project could continue
 *       using {@code transformValues()} for binary-compatibility
 *       reasons, the supplied transformer type is on a deprecation
 *       timer of its own.</li>
 *   <li><b>{@code INVOKEDYNAMIC stream::transformValues} captures
 *       silently bind to the deprecated method.</b> A
 *       topology-builder abstraction (e.g. a {@code ValueBuilder<K,
 *       V, VR>} SAM that takes a {@code ValueTransformerSupplier}
 *       plus the state-store names) resolves the method-ref by
 *       arity and argument-erasure to one of the four legacy
 *       overloads. The user-class bytecode contains zero direct
 *       {@code INVOKEINTERFACE} on the legacy method — only the
 *       {@code INVOKEDYNAMIC} + {@code LambdaMetafactory} bridge. A
 *       name-only {@code MethodInsnNode} walk misses this case
 *       entirely.</li>
 * </ul>
 *
 * <h2>The replacement API</h2>
 *
 * <p>{@code KStream.processValues(FixedKeyProcessorSupplier<K, V,
 * VOut>, String...)} from
 * {@code org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier}
 * is the modern replacement. Migration is mechanical and the
 * store-name argument list is identical:
 *
 * <pre>{@code
 *   // BEFORE — legacy KIP-820 deprecated transformValues
 *   stream.transformValues(() -> new ValueTransformerWithKey<K, V, VR>() {
 *       public void init(ProcessorContext ctx) { ... }
 *       public VR transform(K k, V v) {
 *           return computeNewValue(v);  // or null to drop
 *       }
 *       public void close() { ... }
 *   }, "store-name");
 *
 *   // AFTER — KIP-820 modern processValues
 *   stream.processValues(() -> new FixedKeyProcessor<K, V, VR>() {
 *       private FixedKeyProcessorContext<K, VR> ctx;
 *       public void init(FixedKeyProcessorContext<K, VR> c) { this.ctx = c; }
 *       public void process(FixedKeyRecord<K, V> r) {
 *           // forward to emit; do nothing to drop
 *           ctx.forward(r.withValue(computeNewValue(r.value())));
 *       }
 *       public void close() { ... }
 *   }, "store-name");
 * }</pre>
 *
 * <h2>Descriptor discrimination — four legacy overloads</h2>
 *
 * <p>There are four distinct deprecated descriptors the rule must
 * catch, because each compiles to a distinct
 * {@code INVOKEINTERFACE} / {@code REF_invokeInterface} bytecode
 * shape:
 *
 * <ul>
 *   <li>{@code transformValues(ValueTransformerSupplier, String...)}</li>
 *   <li>{@code transformValues(ValueTransformerSupplier, Named, String...)}</li>
 *   <li>{@code transformValues(ValueTransformerWithKeySupplier, String...)}</li>
 *   <li>{@code transformValues(ValueTransformerWithKeySupplier, Named, String...)}</li>
 * </ul>
 *
 * <p>All four are deprecated. The owner pin to {@code KStream}
 * discriminates from any unrelated {@code transformValues} on other
 * Streams types (none exists in 3.x).
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code stream::transformValues} bound to a SAM that takes
 * {@code (ValueTransformer*Supplier, String[])} compiles to
 * {@code INVOKEDYNAMIC} whose bsm-args contain a
 * {@code REF_invokeInterface} handle pointing at the resolved legacy
 * method. The rule's bsm-arg walk catches this case by checking the
 * handle's {@code (owner, name, desc)} triple against the same filter
 * used for direct calls, iterated over all four legacy descriptors.
 */
public final class StreamsTransformValuesDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final String METHOD_NAME = "transformValues";
    private static final Set<String> LEGACY_DESCS = Set.of(
            "(Lorg/apache/kafka/streams/kstream/ValueTransformerSupplier;[Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KStream;",
            "(Lorg/apache/kafka/streams/kstream/ValueTransformerSupplier;Lorg/apache/kafka/streams/kstream/Named;[Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KStream;",
            "(Lorg/apache/kafka/streams/kstream/ValueTransformerWithKeySupplier;[Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KStream;",
            "(Lorg/apache/kafka/streams/kstream/ValueTransformerWithKeySupplier;Lorg/apache/kafka/streams/kstream/Named;[Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KStream;");

    private final Severity severity;

    public StreamsTransformValuesDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_TRANSFORM_VALUES_DEPRECATED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && LEGACY_DESCS.contains(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    for (String desc : LEGACY_DESCS) {
                        Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, desc);
                        if (h != null) {
                            out.add(violation(ctx, mn, insn));
                            break;
                        }
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_TRANSFORM_VALUES_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.transformValues(...) (one of four deprecated overloads — "
                        + "ValueTransformerSupplier or ValueTransformerWithKeySupplier, "
                        + "each with and without a leading Named argument) is reached "
                        + "here — either as a direct call or as an INVOKEDYNAMIC "
                        + "method-reference capture (e.g. `stream::transformValues` "
                        + "bound to a topology-builder SAM that takes a value-transformer "
                        + "supplier plus state-store names). This method is deprecated "
                        + "since Kafka Streams 3.3 (KIP-820, August 2022) because: "
                        + "(1) the legacy ValueTransformerWithKey only enforced the "
                        + "key-invariant by convention — the body could call "
                        + "context.forward(otherKey, value) and the framework had no "
                        + "way to detect the violation at the type level, producing "
                        + "silent partition skew where a downstream groupByKey/join "
                        + "operation redistributes records by the new key but the "
                        + "source-topic partition assignment was based on the original "
                        + "key (state-store keys not co-partitioned with the inputs); "
                        + "the new FixedKeyRecord type physically cannot construct a "
                        + "new key — the type system statically prevents the violation; "
                        + "(2) the legacy ValueTransformer contract uses the return "
                        + "value as the forward (return value emits, return null drops, "
                        + "fan-out requires the separate deprecated flatTransformValues "
                        + "method); the new FixedKeyProcessor.process(FixedKeyRecord) "
                        + "has no return — the processor calls "
                        + "context.forward(record.withValue(newV)) zero, one, or many "
                        + "times, making fan-out and drop first-class on the API; "
                        + "(3) the legacy API has no timestamp override at forward "
                        + "time — the returned value inherits the input record's "
                        + "timestamp; the new FixedKeyRecord exposes "
                        + "withTimestamp(long) for late-event projection in windowed "
                        + "pipelines; (4) the legacy ValueTransformer / "
                        + "ValueTransformerWithKey interfaces are themselves "
                        + "deprecated — the supplied transformer type is on a "
                        + "deprecation timer of its own; (5) INVOKEDYNAMIC "
                        + "`stream::transformValues` captures silently bind to the "
                        + "deprecated method whenever the SAM has matching arity and "
                        + "argument erasure — the user-class bytecode contains zero "
                        + "direct INVOKEINTERFACE on the legacy method and a name-only "
                        + "walk misses it. Migrate to "
                        + "`stream.processValues(FixedKeyProcessorSupplier, "
                        + "storeNames)` using "
                        + "org.apache.kafka.streams.processor.api.FixedKeyProcessor / "
                        + "FixedKeyProcessorContext / FixedKeyRecord; rewrite "
                        + "`return newValue` as "
                        + "`ctx.forward(record.withValue(newValue))` and `return null` "
                        + "as simply not forwarding. The store-name argument list is "
                        + "identical, so the topology wiring is unchanged — only the "
                        + "lambda body changes. Make this migration in lockstep with "
                        + "the parallel `transform → process` migration "
                        + "(STREAMS_TRANSFORM_DEPRECATED).");
    }
}
