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
 * {@link org.apache.kafka.streams.kstream.KStream#branch(org.apache.kafka.streams.kstream.Predicate[])}
 * (and its named-overload sibling
 * {@code KStream.branch(Named, Predicate...)}) — whether the call lands
 * directly via {@code INVOKEINTERFACE} or indirectly through an
 * {@code INVOKEDYNAMIC} method-reference capture
 * (e.g. {@code stream::branch} bound to a routing-factory SAM that
 * accepts an array of {@code Predicate<K, V>} or a (Named,
 * Predicate[]) pair).
 *
 * <h2>Why this method is deprecated, not just a name change</h2>
 *
 * <p>{@code KStream.branch(Predicate...)} returns
 * {@code KStream<K,V>[]} — a raw Java array indexed by predicate
 * argument position. The API surface has four well-documented
 * correctness traps that have been the source of many production
 * incidents over the years:
 *
 * <ul>
 *   <li><b>Silent drop on no-match.</b> If a record matches none of
 *       the supplied predicates, the legacy implementation drops it
 *       on the floor — no error, no metric, no log line. The only
 *       observable symptom is downstream consumer lag on a topic that
 *       should have records arriving but doesn't. Root-causing such a
 *       drop typically requires re-running the topology against a
 *       known input and bisecting the predicates; the dropped-record
 *       path is invisible to {@code KafkaStreams} metrics, to
 *       {@code TopologyTestDriver}, and to JMX exporters because
 *       there is no processor node downstream of the unmatched
 *       record to attach a counter to.</li>
 *   <li><b>Array-index coupling.</b> Every downstream consumer of a
 *       branch result reads from {@code branches[i]} for some integer
 *       {@code i}. A refactor that reorders the predicates — for
 *       example, alphabetising them, extracting one to a constant, or
 *       inserting a new predicate at the front — silently reroutes
 *       records to different branches without any compile-time or
 *       test-time failure. The bug surfaces in production as
 *       cross-branch data contamination.</li>
 *   <li><b>No named branches in metrics.</b> Each branch is materialised
 *       as an anonymous processor node with a generated name like
 *       {@code KSTREAM-BRANCHCHILD-0000000007}. JMX rocksdb metrics,
 *       stream-thread state-store metrics, and per-topology
 *       throughput counters carry these opaque IDs, making per-branch
 *       performance analysis impossible without a name-to-index
 *       lookup that lives only in the source tree.</li>
 *   <li><b>{@code INVOKEDYNAMIC stream::branch} captures silently bind
 *       to the deprecated method.</b> A routing-factory abstraction
 *       (e.g. a generic {@code Router<K, V>} SAM that takes
 *       {@code Predicate<K, V>...}) resolves the method-ref by arity
 *       and erased argument types to one of the two legacy branch
 *       overloads. The user-class bytecode contains zero direct
 *       {@code INVOKEINTERFACE} on the legacy method — only the
 *       {@code INVOKEDYNAMIC} + {@code LambdaMetafactory} bridge. A
 *       name-only MethodInsnNode walk misses this case entirely.</li>
 * </ul>
 *
 * <h2>The replacement API</h2>
 *
 * <p>KIP-418 (Kafka Streams 2.8, April 2021) introduced
 * {@code KStream.split([Named])} returning {@code BranchedKStream<K, V>},
 * which exposes a fluent builder where each branch is attached by
 * name:
 *
 * <pre>{@code
 *   Map<String, KStream<K, V>> branches =
 *       stream
 *           .split(Named.as("router"))
 *           .branch(p1, Branched.as("hot"))
 *           .branch(p2, Branched.as("warm"))
 *           .defaultBranch(Branched.as("cold"));
 * }</pre>
 *
 * <p>Three concrete correctness wins over the legacy API:
 *
 * <ul>
 *   <li>{@code defaultBranch(...)} guarantees that records matching
 *       none of the predicates are not silently dropped — they go to
 *       the explicit default sink, where the operator can attach a
 *       counter, a warn-level log, or a dead-letter topic write.</li>
 *   <li>Branches are addressed by {@code Branched.as(name)} string
 *       key. Reordering the {@code .branch(...)} calls preserves the
 *       routing because each downstream consumer reads
 *       {@code branches.get("hot")}, not {@code branches[0]}.</li>
 *   <li>Branches show up in JMX with their declared names, making
 *       per-branch latency / throughput / restore-time profiling
 *       trivial.</li>
 * </ul>
 *
 * <h2>Descriptor discrimination — two legacy overloads</h2>
 *
 * <p>There are two distinct deprecated descriptors the rule must
 * catch, because both compile to distinct {@code INVOKEINTERFACE} /
 * {@code REF_invokeInterface} bytecode shapes:
 *
 * <ul>
 *   <li>{@code branch(Predicate...)} →
 *       {@code ([Lorg/apache/kafka/streams/kstream/Predicate;)[Lorg/apache/kafka/streams/kstream/KStream;}</li>
 *   <li>{@code branch(Named, Predicate...)} →
 *       {@code (Lorg/apache/kafka/streams/kstream/Named;[Lorg/apache/kafka/streams/kstream/Predicate;)[Lorg/apache/kafka/streams/kstream/KStream;}</li>
 * </ul>
 *
 * <p>Both are deprecated. The rule iterates over both descriptors at
 * each candidate instruction. The name {@code "branch"} is also used
 * by the new {@code BranchedKStream.branch(...)} fluent API, but the
 * owner pin to {@code KStream} (not {@code BranchedKStream})
 * discriminates: the new fluent calls have owner
 * {@code BranchedKStream} and a different descriptor shape (they take
 * a {@code Branched} argument, not a {@code Predicate[]}).
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code stream::branch} bound to a SAM that takes
 * {@code Predicate<K, V>...} compiles to {@code INVOKEDYNAMIC} whose
 * bsm-args contain a {@code REF_invokeInterface} handle pointing at
 * the resolved legacy method. The rule's bsm-arg walk catches this
 * case by checking the handle's {@code (owner, name, desc)} triple
 * against the same filter used for direct calls, iterated over both
 * legacy descriptors.
 */
public final class StreamsBranchDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final String METHOD_NAME = "branch";
    private static final Set<String> LEGACY_DESCS = Set.of(
            "([Lorg/apache/kafka/streams/kstream/Predicate;)[Lorg/apache/kafka/streams/kstream/KStream;",
            "(Lorg/apache/kafka/streams/kstream/Named;[Lorg/apache/kafka/streams/kstream/Predicate;)[Lorg/apache/kafka/streams/kstream/KStream;");

    private final Severity severity;

    public StreamsBranchDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_BRANCH_DEPRECATED;
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
                RuleId.STREAMS_BRANCH_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.branch(Predicate...) (or its named-overload sibling "
                        + "KStream.branch(Named, Predicate...)) is reached here — either "
                        + "as a direct call or as an INVOKEDYNAMIC method-reference "
                        + "capture (e.g. `stream::branch` bound to a routing-factory SAM "
                        + "that takes Predicate<K, V>...). This method is deprecated "
                        + "since Kafka Streams 2.8 (KIP-418, April 2021) because the "
                        + "returned KStream<K,V>[] is indexed by predicate-argument "
                        + "position and has four well-known correctness traps: "
                        + "(1) silent drop on no-match — records matching none of the "
                        + "predicates are dropped with no error, no metric, no log "
                        + "line; the only observable symptom is downstream consumer lag "
                        + "on a topic that should have records but doesn't, and the "
                        + "drop path is invisible to KafkaStreams metrics, "
                        + "TopologyTestDriver, and JMX exporters because there is no "
                        + "processor node downstream of the unmatched record to attach "
                        + "a counter to; (2) array-index coupling — every downstream "
                        + "consumer reads from `branches[i]` for some integer i, so a "
                        + "refactor that reorders the predicates silently reroutes "
                        + "records to different branches with no compile-time or "
                        + "test-time failure, surfacing in production as cross-branch "
                        + "data contamination; (3) no named branches in metrics — each "
                        + "branch becomes an anonymous processor with a generated name "
                        + "like KSTREAM-BRANCHCHILD-0000000007, making per-branch JMX "
                        + "performance analysis impossible without a name-to-index "
                        + "lookup that lives only in the source tree; (4) INVOKEDYNAMIC "
                        + "`stream::branch` captures silently bind to the deprecated "
                        + "method whenever the SAM has matching arity and argument "
                        + "erasure — the user-class bytecode contains zero direct "
                        + "INVOKEINTERFACE on the legacy method and a name-only walk "
                        + "misses it. Migrate to `stream.split(Named.as(\"router\"))"
                        + ".branch(p1, Branched.as(\"hot\")).branch(p2, "
                        + "Branched.as(\"warm\")).defaultBranch(Branched.as(\"cold\"))`. "
                        + "The result is a Map<String, KStream<K,V>> addressable by "
                        + "name (reorder-safe), the defaultBranch sinks all otherwise-"
                        + "unmatched records (drop-safe), and the JMX node names carry "
                        + "the declared branch names (debuggable).");
    }
}
