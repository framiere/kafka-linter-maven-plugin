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
 * {@link org.apache.kafka.streams.kstream.KStream#transform(org.apache.kafka.streams.kstream.TransformerSupplier, String...)}
 * (and its named-overload sibling
 * {@code KStream.transform(TransformerSupplier, Named, String...)}) —
 * whether the call lands directly via {@code INVOKEINTERFACE} or
 * indirectly through an {@code INVOKEDYNAMIC} method-reference capture
 * (e.g. {@code stream::transform} bound to a builder SAM that takes a
 * {@code TransformerSupplier} plus a store-name varargs array).
 *
 * <h2>Why this method is deprecated, not just a name change</h2>
 *
 * <p>{@code KStream.transform(TransformerSupplier, String...)} is the
 * legacy "stateful map" hook on {@code KStream}: it takes a
 * {@code TransformerSupplier<K, V, KeyValue<K1, V1>>}, attaches state
 * stores by name, and forwards each transformed
 * {@code KeyValue<K1, V1>} returned from {@code Transformer.transform(K, V)}.
 * KIP-820 (Kafka Streams 3.3, August 2022) deprecated this API and
 * replaced it with {@code KStream.process(ProcessorSupplier<K, V, KOut, VOut>)}
 * from {@code org.apache.kafka.streams.processor.api} — the modern
 * top-level processor API that has been the public extension surface
 * since Kafka 2.7 (KIP-478).
 *
 * <p>Five well-documented reasons KIP-820 forced this migration:
 *
 * <ul>
 *   <li><b>The legacy {@code Transformer} contract leaks the return
 *       value as the forward.</b> {@code Transformer.transform(K, V)}
 *       returns a {@code KeyValue} that the framework forwards;
 *       returning {@code null} means "drop this record". The new
 *       {@code Processor.process(Record)} contract has no return — the
 *       processor calls {@code context.forward(Record)} zero, one, or
 *       many times. Fan-out, drop, and re-keying are first-class on
 *       the new API and require no semantic mapping; on the legacy
 *       API, fan-out had to go through {@code flatTransform} (a
 *       separate deprecated method) and "drop" had to go through
 *       returning {@code null}.</li>
 *   <li><b>The legacy API forces wall-clock-time forwarding.</b>
 *       Returning a {@code KeyValue} forwards a record with the
 *       record's existing timestamp. The new {@code Record<K, V>}
 *       value type carries an explicit {@code long timestamp} field
 *       that {@code Processor.process} can override before forwarding —
 *       essential for stream-time projection in late-arriving data
 *       pipelines.</li>
 *   <li><b>The legacy {@code ProcessorContext} mixes the
 *       record-bound APIs (key, value, timestamp, headers, topic,
 *       partition, offset) and the topology-bound APIs (forward,
 *       commit, schedule, getStateStore) on a single mutable object
 *       whose accessors depend on which record is being processed.</b>
 *       The new {@code Record<K, V>} value type owns the record-bound
 *       fields immutably, and {@code Processor.process(Record)}
 *       receives both the immutable record and the topology-bound
 *       {@code ProcessorContext<KOut, VOut>}. The accessor-state
 *       coupling that produced "context.timestamp() returns -1 between
 *       process() calls" production bugs is gone by construction.</li>
 *   <li><b>The legacy
 *       {@code org.apache.kafka.streams.processor.Processor} interface
 *       is itself deprecated.</b> {@code TransformerSupplier} and its
 *       siblings have always implemented this legacy interface
 *       internally; the modern
 *       {@code org.apache.kafka.streams.processor.api.Processor}
 *       (note the package difference: {@code .api}) is the only
 *       interface a new processor should implement. Even if a project
 *       could continue using the legacy {@code transform()} method for
 *       binary-compatibility reasons, the type the supplier returns is
 *       on a deprecation timer of its own.</li>
 *   <li><b>{@code INVOKEDYNAMIC stream::transform} captures silently
 *       bind to the deprecated method.</b> A topology-builder
 *       abstraction (e.g. a {@code Builder<K, V>} SAM that takes a
 *       {@code TransformerSupplier} plus the state-store names)
 *       resolves the method-ref by arity and argument-erasure to one
 *       of the two legacy {@code transform} overloads. The user-class
 *       bytecode contains zero direct {@code INVOKEINTERFACE} on the
 *       legacy method — only the {@code INVOKEDYNAMIC} +
 *       {@code LambdaMetafactory} bridge. A name-only
 *       {@code MethodInsnNode} walk misses this case entirely.</li>
 * </ul>
 *
 * <h2>The replacement API</h2>
 *
 * <p>{@code KStream.process(ProcessorSupplier<K, V, KOut, VOut>,
 * String...)} from
 * {@code org.apache.kafka.streams.processor.api.ProcessorSupplier}
 * is the modern replacement. The migration is mechanical:
 *
 * <pre>{@code
 *   // BEFORE — legacy KIP-820 deprecated transform
 *   stream.transform(() -> new Transformer<K, V, KeyValue<K1, V1>>() {
 *       public void init(ProcessorContext ctx) { ... }
 *       public KeyValue<K1, V1> transform(K k, V v) {
 *           return KeyValue.pair(newK, newV);  // or null to drop
 *       }
 *       public void close() { ... }
 *   }, "store-name");
 *
 *   // AFTER — KIP-820 modern process
 *   stream.process(() -> new Processor<K, V, K1, V1>() {
 *       private ProcessorContext<K1, V1> ctx;
 *       public void init(ProcessorContext<K1, V1> c) { this.ctx = c; }
 *       public void process(Record<K, V> r) {
 *           // forward to emit; do nothing to drop
 *           ctx.forward(new Record<>(newK, newV, r.timestamp()));
 *       }
 *       public void close() { ... }
 *   }, "store-name");
 * }</pre>
 *
 * <p>The store-name argument list is identical between the two APIs —
 * the migration touches only the lambda body, never the topology
 * wiring around it.
 *
 * <h2>Descriptor discrimination — two legacy overloads</h2>
 *
 * <p>There are two distinct deprecated descriptors the rule must
 * catch, because both compile to distinct {@code INVOKEINTERFACE} /
 * {@code REF_invokeInterface} bytecode shapes:
 *
 * <ul>
 *   <li>{@code transform(TransformerSupplier, String...)} →
 *       {@code (Lorg/apache/kafka/streams/kstream/TransformerSupplier;[Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KStream;}</li>
 *   <li>{@code transform(TransformerSupplier, Named, String...)} →
 *       {@code (Lorg/apache/kafka/streams/kstream/TransformerSupplier;Lorg/apache/kafka/streams/kstream/Named;[Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KStream;}</li>
 * </ul>
 *
 * <p>The owner pin to {@code KStream} discriminates from the
 * non-deprecated {@code transform} methods elsewhere in the Streams
 * surface (e.g. {@code KGroupedStream} has no {@code transform}; the
 * new {@code process(ProcessorSupplier)} has a different method name
 * — there is no name collision risk on {@code KStream} itself).
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code stream::transform} bound to a SAM that takes
 * {@code (TransformerSupplier, String[])} compiles to
 * {@code INVOKEDYNAMIC} whose bsm-args contain a
 * {@code REF_invokeInterface} handle pointing at the resolved legacy
 * method. The rule's bsm-arg walk catches this case by checking the
 * handle's {@code (owner, name, desc)} triple against the same filter
 * used for direct calls, iterated over both legacy descriptors.
 */
public final class StreamsTransformDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final String METHOD_NAME = "transform";
    private static final Set<String> LEGACY_DESCS = Set.of(
            "(Lorg/apache/kafka/streams/kstream/TransformerSupplier;[Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KStream;",
            "(Lorg/apache/kafka/streams/kstream/TransformerSupplier;Lorg/apache/kafka/streams/kstream/Named;[Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KStream;");

    private final Severity severity;

    public StreamsTransformDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_TRANSFORM_DEPRECATED;
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
                RuleId.STREAMS_TRANSFORM_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.transform(TransformerSupplier, String...) (or its "
                        + "named-overload sibling KStream.transform(TransformerSupplier, "
                        + "Named, String...)) is reached here — either as a direct call "
                        + "or as an INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`stream::transform` bound to a topology-builder SAM that "
                        + "takes a TransformerSupplier plus the state-store names). "
                        + "This method is deprecated since Kafka Streams 3.3 (KIP-820, "
                        + "August 2022) because (1) the legacy Transformer contract "
                        + "uses the return value as the forward — returning KeyValue "
                        + "emits, returning null drops, and fan-out requires the "
                        + "separate deprecated flatTransform method; the new "
                        + "Processor.process(Record) has no return and emits via "
                        + "context.forward zero, one, or many times, making fan-out, "
                        + "drop, and re-keying first-class on the API; (2) the legacy "
                        + "API has no way to override the record timestamp at forward "
                        + "time — the returned KeyValue inherits the input record's "
                        + "timestamp, while the new Record value type carries an "
                        + "explicit `long timestamp` field that Processor.process can "
                        + "override before forwarding (essential for stream-time "
                        + "projection in late-arriving data pipelines); (3) the "
                        + "legacy org.apache.kafka.streams.processor.ProcessorContext "
                        + "mixes record-bound fields (key/value/timestamp/headers/topic/"
                        + "partition/offset) and topology-bound APIs "
                        + "(forward/commit/schedule/getStateStore) on a single mutable "
                        + "object whose accessors depend on which record is being "
                        + "processed, producing 'context.timestamp() returns -1 "
                        + "between process() calls' production bugs; the new Record "
                        + "type owns the record-bound fields immutably and "
                        + "Processor.process receives them as a value alongside the "
                        + "topology-bound ProcessorContext; (4) the legacy "
                        + "org.apache.kafka.streams.processor.Processor interface that "
                        + "TransformerSupplier implements internally is itself "
                        + "deprecated — the only forward-compatible interface is "
                        + "org.apache.kafka.streams.processor.api.Processor (note the "
                        + "package: `.api`); (5) INVOKEDYNAMIC `stream::transform` "
                        + "captures silently bind to the legacy method whenever the "
                        + "SAM has matching arity and argument erasure — the "
                        + "user-class bytecode contains zero direct INVOKEINTERFACE on "
                        + "the legacy method and a name-only walk misses it. Migrate "
                        + "to `stream.process(ProcessorSupplier, storeNames)` using "
                        + "org.apache.kafka.streams.processor.api.ProcessorSupplier; "
                        + "rewrite `return KeyValue.pair(k, v)` as "
                        + "`ctx.forward(new Record<>(k, v, record.timestamp()))` and "
                        + "`return null` as simply not forwarding. The store-name "
                        + "argument list is identical, so the topology wiring is "
                        + "unchanged — only the lambda body changes.");
    }
}
