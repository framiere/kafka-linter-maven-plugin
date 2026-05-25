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
 * Fires for every reach of the no-Named overload of {@link
 * org.apache.kafka.streams.kstream.KStream#selectKey(
 * org.apache.kafka.streams.kstream.KeyValueMapper)
 * KStream.selectKey(KeyValueMapper)} — the variant that does
 * NOT take a {@link
 * org.apache.kafka.streams.kstream.Named Named} argument and
 * therefore leaves the SelectKey processor node — and any
 * downstream repartition topic that the new key triggers —
 * auto-named from the topology graph index.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (KStream
 * is an interface) and {@code INVOKEDYNAMIC} method-reference
 * captures (e.g. {@code stream::selectKey} bound to a
 * {@link java.util.function.Function Function&lt;KStream,
 * KStream&gt;} or to a custom SAM whose erased implMethod
 * descriptor matches the one unsafe overload descriptor exactly).
 *
 * <h2>Why no-Named selectKey is a topology-stability hazard</h2>
 *
 * <p>{@link org.apache.kafka.streams.kstream.KStream#selectKey
 * KStream.selectKey} rebuilds the record key. Two consequences
 * follow from a key change:
 *
 * <ul>
 *   <li><b>The processor node itself is auto-named</b> — e.g.
 *       {@code KSTREAM-KEY-SELECT-0000000003}. The auto-name is
 *       derived from the topology graph index — a monotonically
 *       increasing counter assigned at builder time. Any upstream
 *       topology edit (a new {@code filter}, a renamed source
 *       node) shifts the counter and renames this node.</li>
 *   <li><b>Any downstream operation that needs the new key</b>
 *       — typically a stateful operation such as
 *       {@code groupByKey().aggregate(...)},
 *       {@code groupByKey().reduce(...)}, or a {@code join} —
 *       forces Streams to insert a repartition topic between
 *       selectKey and the stateful operator, because the new
 *       key necessarily lives in different partitions than the
 *       old one. The repartition topic name is also auto-named
 *       e.g. {@code my-app-KSTREAM-KEY-SELECT-0000000003-
 *       repartition} — derived directly from the selectKey
 *       node name above.</li>
 * </ul>
 *
 * <p>Without an explicit {@link
 * org.apache.kafka.streams.kstream.Named#as(String)}, ANY
 * topology change above selectKey renames the SelectKey
 * processor node and (transitively) renames the downstream
 * repartition topic. The downstream stateful operator restarts
 * consumption of the new repartition topic from offset 0, but
 * the new topic is EMPTY — the aggregation produces no output
 * until enough upstream records have flowed through, the
 * downstream aggregation state store rebuilds against a
 * brand-new topic, and the old repartition topic accumulates
 * unread bytes forever (Streams never deletes auto-created
 * repartition topics).
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Aggregation restart on every topology edit.</b> A
 *       team has {@code stream.selectKey((k, v) -> v.userId)
 *       .groupByKey().count()} producing per-user counts. They
 *       ship a hotfix that adds a {@code filter} above
 *       selectKey to drop heartbeat events. On deploy the
 *       SelectKey node's graph index shifts; the downstream
 *       repartition topic is renamed; the aggregation state
 *       store is rebuilt against the empty new repartition
 *       topic; per-user counts drop to zero and slowly recover
 *       as upstream traffic flows through; downstream
 *       dashboards show a counts spike of "missing" data for
 *       hours, monitoring alerts page, and the old repartition
 *       topic continues to consume cluster disk forever because
 *       Streams will not garbage-collect it.</li>
 *   <li><b>Per-node metrics tagged by node ID break dashboards.</b>
 *       Streams emits per-processor-node JMX metrics tagged by
 *       node ID (e.g. {@code processor-node-id=KSTREAM-KEY-
 *       SELECT-0000000003}). Grafana panels that scope queries
 *       by this tag silently return empty after a topology edit
 *       because the node ID changed; engineers see "no data" on
 *       dashboards that previously worked, hunt for routing /
 *       collection bugs, and lose hours before noticing the
 *       node-ID mismatch.</li>
 *   <li><b>Repartition-topic ACL grants drift.</b> Ops grants
 *       per-topic produce / consume ACLs against the original
 *       auto-generated repartition topic name. After a topology
 *       edit the new repartition topic name doesn't match any
 *       granted ACL; the Streams instance gets
 *       {@code TopicAuthorizationException} on the first record
 *       and enters a crash loop; the fix requires ops to grant
 *       ACLs on the new topic name — 30-minute outage minimum
 *       in business hours, longer if ops is in another
 *       timezone.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       reroute factory built as
 *       {@code Function<KStream<String, OrderEvent>,
 *       KStream<String, OrderEvent>> rekey = stream ->
 *       stream.selectKey((k, v) -> v.userId)} via method
 *       reference does NOT match — but {@code Function<KStream,
 *       KStream> rekey = stream -> stream.selectKey(mapper)}
 *       where mapper is captured does. Likewise
 *       {@code stream::selectKey} bound to a SAM that takes the
 *       KeyValueMapper and returns the rekeyed KStream compiles
 *       to INVOKEDYNAMIC whose bsm-args contain a
 *       REF_invokeInterface Handle pointing at
 *       {@code KStream.selectKey(KeyValueMapper)KStream} — the
 *       unsafe descriptor exactly.</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the one unsafe overload
 * descriptor. Migration: pass a stable Named argument —
 * {@code stream.selectKey((k, v) -> v.userId,
 * Named.as("rekey-by-user"))}. The chosen node name must be
 * stable across topology edits because it transitively names the
 * downstream repartition topic, which carries per-node metric
 * tags and grants.
 */
public final class StreamsSelectKeyNoNamedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final String METHOD_NAME = "selectKey";
    private static final String UNSAFE_DESC =
            "(Lorg/apache/kafka/streams/kstream/KeyValueMapper;)"
                    + "Lorg/apache/kafka/streams/kstream/KStream;";

    private final Severity severity;

    public StreamsSelectKeyNoNamedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_SELECT_KEY_NO_NAMED;
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
                RuleId.STREAMS_SELECT_KEY_NO_NAMED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.selectKey(KeyValueMapper) — the "
                        + "no-Named overload is reached here — either as "
                        + "a direct INVOKEINTERFACE on the method or as "
                        + "an INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `stream::selectKey` bound to a SAM "
                        + "whose erased implMethod descriptor matches "
                        + "the unsafe overload). Without an explicit "
                        + "Named argument, the SelectKey processor node "
                        + "is auto-named e.g. `KSTREAM-KEY-SELECT-"
                        + "0000000003` from the topology graph index, "
                        + "and any downstream stateful operator that "
                        + "needs the new key (groupByKey, aggregate, "
                        + "reduce, join) forces Streams to insert a "
                        + "repartition topic also auto-named from the "
                        + "same index — e.g. `my-app-KSTREAM-KEY-SELECT-"
                        + "0000000003-repartition`. Any upstream topology "
                        + "edit shifts the graph index and renames both "
                        + "the processor node and the downstream "
                        + "repartition topic. Concrete failure modes: "
                        + "(1) aggregation restart on every topology "
                        + "edit — team has `stream.selectKey((k, v) -> "
                        + "v.userId).groupByKey().count()` producing "
                        + "per-user counts; team ships a hotfix that "
                        + "adds one upstream filter above selectKey; "
                        + "graph index shifts; downstream repartition "
                        + "topic is renamed; aggregation state store "
                        + "rebuilds against the empty new repartition "
                        + "topic; per-user counts drop to zero and "
                        + "slowly recover as upstream traffic flows "
                        + "through; downstream dashboards show a counts "
                        + "spike of `missing` data for hours, monitoring "
                        + "alerts page off-hours, and the old "
                        + "repartition topic continues to consume "
                        + "cluster disk forever because Streams never "
                        + "garbage-collects auto-created repartition "
                        + "topics; (2) per-node metrics tagged by node "
                        + "ID break dashboards — Streams emits "
                        + "per-processor-node JMX metrics tagged by node "
                        + "ID (e.g. `processor-node-id=KSTREAM-KEY-"
                        + "SELECT-0000000003`); Grafana panels that "
                        + "scope queries by this tag silently return "
                        + "empty after a topology edit because the node "
                        + "ID changed; engineers see `no data` on "
                        + "dashboards that previously worked, hunt for "
                        + "routing/collection bugs, lose hours before "
                        + "noticing the node-ID mismatch; (3) "
                        + "repartition-topic ACL grants drift — ops "
                        + "grants per-topic produce/consume ACLs against "
                        + "the original auto-generated repartition topic "
                        + "name; after a topology edit the new "
                        + "repartition topic name doesn't match any "
                        + "granted ACL; Streams instance gets "
                        + "TopicAuthorizationException on the first "
                        + "record and enters a crash loop; fix requires "
                        + "ops to grant ACLs on the new topic name — "
                        + "30-minute outage minimum, longer if ops is "
                        + "in another timezone; (4) INVOKEDYNAMIC "
                        + "method-reference captures bypass naive "
                        + "MethodInsnNode-only lint — `Function<KStream<"
                        + "String, OrderEvent>, KStream<String, "
                        + "OrderEvent>> rekey = stream::selectKey` "
                        + "compiles to INVOKEDYNAMIC whose bsm-args "
                        + "contain a REF_invokeInterface Handle pointing "
                        + "at KStream.selectKey(KeyValueMapper)KStream; "
                        + "the user-class bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Named overload, "
                        + "only the indy site. Migration: pass a stable "
                        + "Named argument — `stream.selectKey((k, v) -> "
                        + "v.userId, Named.as(\"rekey-by-user\"))`. The "
                        + "chosen node name must be stable across "
                        + "topology edits because it transitively names "
                        + "the downstream repartition topic, which "
                        + "carries per-node metric tags and grants. The "
                        + "safe overload (with Named) is never flagged "
                        + "by this rule.");
    }
}
