package io.conductor.kafkalinter.rules.streams;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Fires for every reach of the {@link
 * org.apache.kafka.streams.kstream.KStream#groupByKey()
 * KStream.groupByKey()} overload — the only overload that does
 * NOT take a {@link org.apache.kafka.streams.kstream.Grouped
 * Grouped} argument and therefore leaves both the resulting
 * KGroupedStream and (when the upstream stream is repartition-
 * required) the synthesised repartition topic auto-named from
 * the topology graph index.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (KStream
 * is an interface) and {@code INVOKEDYNAMIC} method-reference
 * captures (e.g. {@code stream::groupByKey} bound to a {@link
 * java.util.function.Supplier
 * Supplier&lt;KGroupedStream&gt;}).
 *
 * <h2>Why no-Grouped groupByKey is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.streams.kstream.KStream#groupByKey
 * KStream.groupByKey} groups records by their existing key in
 * preparation for an aggregation. Internally, two things happen:
 *
 * <ul>
 *   <li><b>If the upstream stream is repartition-required</b>
 *       (any prior {@code selectKey}, {@code map}, {@code
 *       flatMap}, {@code transform} that may have changed the
 *       key), Streams INSERTS a repartition topic upstream. The
 *       repartition topic is auto-named from the graph index —
 *       e.g. {@code my-app-KSTREAM-AGGREGATE-STATE-STORE-
 *       0000000005-repartition}. The repartition topic carries
 *       every record between the source app instance and the
 *       partitioner for the new key distribution.</li>
 *   <li><b>The resulting KGroupedStream</b> is also tagged with
 *       a graph-index-derived name that downstream aggregations
 *       (e.g. {@code aggregate}, {@code count}, {@code reduce})
 *       inherit as the basis for their backing state store and
 *       changelog topic names.</li>
 * </ul>
 *
 * <p>Without an explicit {@link
 * org.apache.kafka.streams.kstream.Grouped#as(String)}, both
 * auto-derive from the same graph index. Any upstream topology
 * edit shifts the sequence numbers and renames the artifacts.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Downstream aggregations restart from offset 0 after a
 *       topology edit.</b> A team has {@code stream.selectKey(
 *       (k, v) -> v.customerId()).groupByKey().count()} producing
 *       a per-customer count. Selecting a new key marks the
 *       stream as repartition-required; Streams synthesises a
 *       repartition topic auto-named {@code my-app-KSTREAM-
 *       AGGREGATE-STATE-STORE-0000000005-repartition}. Team adds
 *       one upstream filter; the graph index shifts by one; the
 *       new repartition topic is now {@code 0000000007-
 *       repartition}; the old topic still exists on the brokers
 *       (Streams never deletes auto-created topics) but the new
 *       instance writes to the new topic only; the consumer
 *       group for the downstream count() reads from the NEW
 *       repartition topic at offset 0; the count starts from
 *       zero, completely losing all historical aggregation
 *       state.</li>
 *   <li><b>Repartition-topic storage accumulates indefinitely.</b>
 *       Streams never auto-deletes orphaned repartition topics.
 *       Every topology edit that shifts the graph index produces
 *       a new repartition topic and orphans the previous one.
 *       For a team with weekly deploys that touches the upstream
 *       of a groupByKey call site, that is 52 orphan repartition
 *       topics per year, each holding the full upstream record
 *       volume between the deploy and the cleanup policy. Across
 *       N call sites and M Streams apps sharing a cluster, the
 *       bytes-on-disk grows unboundedly until SRE audit.</li>
 *   <li><b>Partitioner mismatch silently splits aggregation
 *       state.</b> The repartition topic carries the new key
 *       distribution. When the auto-name changes, the OLD
 *       repartition topic still has the partition assignments
 *       Streams produced before the deploy; the NEW repartition
 *       topic gets fresh assignments. If, during the deploy
 *       window, some instances are running the old topology and
 *       some the new, the SAME logical key may be written to
 *       different partitions of two different topics — and the
 *       downstream aggregation reads only the new topic.
 *       Aggregation values for the affected keys are silently
 *       split: partial state in the old changelog, partial state
 *       in the new state store, neither correct.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures bypass
 *       naïve {@code MethodInsnNode}-only lint.</b> A factory
 *       built as {@code Supplier&lt;KGroupedStream&lt;K, V&gt;&gt;
 *       group = stream::groupByKey} compiles to {@code
 *       INVOKEDYNAMIC} whose bsm-args contain a {@code
 *       REF_invokeInterface} Handle pointing at {@code
 *       KStream.groupByKey()KGroupedStream}. The user-class
 *       bytecode contains zero direct {@code INVOKEINTERFACE} on
 *       the no-Grouped overload, only the indy site.</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the single unsafe overload
 * descriptor {@code ()Lorg/apache/kafka/streams/kstream/
 * KGroupedStream;}. Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Grouped} naming the
 * KGroupedStream — e.g. {@code stream.groupByKey(Grouped.as(
 * "orders-by-customer"))}. The chosen name is stable across
 * topology edits because the user wrote it down, and any
 * synthesised repartition topic inherits it.
 */
public final class StreamsGroupByKeyNoGroupedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final String METHOD_NAME = "groupByKey";
    private static final String UNSAFE_DESC =
            "()Lorg/apache/kafka/streams/kstream/KGroupedStream;";

    private final Severity severity;

    public StreamsGroupByKeyNoGroupedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_GROUP_BY_KEY_NO_GROUPED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && UNSAFE_DESC.equals(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy
                        && AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, UNSAFE_DESC) != null) {
                    out.add(violation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_GROUP_BY_KEY_NO_GROUPED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.groupByKey() — the no-Grouped overload is "
                        + "reached here — either as a direct "
                        + "INVOKEINTERFACE on the method or as an "
                        + "INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `stream::groupByKey` bound to "
                        + "Supplier<KGroupedStream>). KStream.groupByKey "
                        + "groups records by their existing key in "
                        + "preparation for an aggregation; if the "
                        + "upstream stream is repartition-required "
                        + "(any prior selectKey/map/flatMap/transform "
                        + "that may have changed the key), Streams "
                        + "INSERTS a repartition topic upstream auto-"
                        + "named e.g. `my-app-KSTREAM-AGGREGATE-STATE-"
                        + "STORE-0000000005-repartition` from the "
                        + "topology graph index; the resulting "
                        + "KGroupedStream is also tagged with a graph-"
                        + "index-derived name that downstream "
                        + "aggregations inherit as the basis for their "
                        + "backing state store and changelog topic "
                        + "names. Concrete failure modes: (1) "
                        + "downstream aggregations restart from offset "
                        + "0 after a topology edit — team has "
                        + "`stream.selectKey((k, v) -> v.customerId())."
                        + "groupByKey().count()` producing a per-"
                        + "customer count; selecting a new key marks "
                        + "the stream as repartition-required; Streams "
                        + "synthesises a repartition topic auto-named "
                        + "`my-app-KSTREAM-AGGREGATE-STATE-STORE-"
                        + "0000000005-repartition`; team adds one "
                        + "upstream filter; the graph index shifts; "
                        + "the new repartition topic is now "
                        + "`0000000007-repartition`; the old topic "
                        + "still exists on the brokers (Streams never "
                        + "deletes auto-created topics) but the new "
                        + "instance writes to the new topic only; the "
                        + "consumer group for the downstream count() "
                        + "reads from the NEW repartition topic at "
                        + "offset 0; the count starts from zero, "
                        + "completely losing all historical aggregation "
                        + "state; (2) repartition-topic storage "
                        + "accumulates indefinitely — Streams never "
                        + "auto-deletes orphaned repartition topics; "
                        + "every topology edit that shifts the graph "
                        + "index produces a new repartition topic and "
                        + "orphans the previous one; weekly deploys "
                        + "that touch upstream of a groupByKey call "
                        + "site = 52 orphan repartition topics per "
                        + "year, each holding the full upstream record "
                        + "volume between the deploy and the cleanup "
                        + "policy; across N call sites and M Streams "
                        + "apps sharing a cluster, bytes-on-disk grows "
                        + "unboundedly until SRE audit; (3) partitioner "
                        + "mismatch silently splits aggregation state "
                        + "— the repartition topic carries the new key "
                        + "distribution; when the auto-name changes, "
                        + "the OLD repartition topic still has the "
                        + "partition assignments Streams produced "
                        + "before the deploy; the NEW gets fresh "
                        + "assignments; if during the deploy window "
                        + "some instances are running the old topology "
                        + "and some the new, the SAME logical key may "
                        + "be written to different partitions of two "
                        + "different topics — and the downstream "
                        + "aggregation reads only the new topic; "
                        + "aggregation values for affected keys are "
                        + "silently split between old changelog and "
                        + "new state store, neither correct; (4) "
                        + "INVOKEDYNAMIC method-reference captures "
                        + "bypass naive MethodInsnNode-only lint — "
                        + "`Supplier<KGroupedStream<K, V>> group = "
                        + "stream::groupByKey` compiles to "
                        + "INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at "
                        + "KStream.groupByKey()KGroupedStream; the "
                        + "user-class bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Grouped overload, "
                        + "only the indy site. Migration: pass an "
                        + "explicit Grouped naming the KGroupedStream "
                        + "— `stream.groupByKey(Grouped.as(\""
                        + "orders-by-customer\"))`. The chosen name is "
                        + "stable across topology edits because the "
                        + "user wrote it down, and any synthesised "
                        + "repartition topic inherits it. The "
                        + "groupByKey(Grouped) overload is never "
                        + "flagged by this rule.");
    }
}
