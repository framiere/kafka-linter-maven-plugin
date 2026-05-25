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
 * Fires for every reach of a {@link
 * org.apache.kafka.streams.kstream.KTable#join(
 * org.apache.kafka.streams.kstream.KTable,
 * java.util.function.Function,
 * org.apache.kafka.streams.kstream.ValueJoiner) KTable.join} or
 * {@link org.apache.kafka.streams.kstream.KTable#leftJoin(
 * org.apache.kafka.streams.kstream.KTable,
 * java.util.function.Function,
 * org.apache.kafka.streams.kstream.ValueJoiner) KTable.leftJoin}
 * <b>foreign-key</b> overload — the variants whose descriptor
 * contains a {@code java.util.function.Function} parameter (the
 * foreign-key extractor) AND does NOT contain a
 * {@link org.apache.kafka.streams.kstream.Materialized
 * Materialized} parameter.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (KTable is
 * an interface — the join/leftJoin call site emits
 * {@code INVOKEINTERFACE}, not {@code INVOKEVIRTUAL}) and
 * {@code INVOKEDYNAMIC} method-reference captures (e.g.
 * {@code orders::join}, {@code customers::leftJoin}) whose
 * bsm-args contain a {@code REF_invokeInterface} Handle pointing
 * at KTable.join/leftJoin with a Function-bearing, no-Materialized
 * descriptor.
 *
 * <h2>Discriminating FK joins from inner-key joins</h2>
 *
 * <p>{@link org.apache.kafka.streams.kstream.KTable KTable}
 * exposes three families of {@code join} / {@code leftJoin}
 * overloads, and only one is a foreign-key (FK) join:
 *
 * <ul>
 *   <li><b>Inner-key join</b> — {@code (KTable, ValueJoiner, ...)}:
 *       joins on the SAME key. No {@code Function} parameter.
 *       Different state-store hazard, handled by other rules.</li>
 *   <li><b>Inner-key join with ValueJoinerWithKey</b> — also
 *       {@code (KTable, ValueJoinerWithKey, ...)}. Still no FK
 *       extractor.</li>
 *   <li><b>Foreign-key join</b> — {@code (KTable, Function,
 *       ValueJoiner, ...)}: the Function&lt;V, KO&gt; extracts the
 *       foreign key from the left side's value. This is what this
 *       rule predicate matches: descriptor contains
 *       {@code Ljava/util/function/Function;}.</li>
 * </ul>
 *
 * <p>Among FK-join overloads, only the variants without a
 * {@link org.apache.kafka.streams.kstream.Materialized}
 * argument are the hazard. Concretely, the unsafe descriptors
 * (in Kafka Streams 3.7) are:
 *
 * <ul>
 *   <li>3-arg {@code (KTable, Function, ValueJoiner)}</li>
 *   <li>4-arg {@code (KTable, Function, ValueJoiner, Named)}</li>
 *   <li>4-arg {@code (KTable, Function, ValueJoiner, TableJoined)}</li>
 * </ul>
 *
 * <p>The safe overloads add a trailing
 * {@code Materialized<K, VR, KeyValueStore<Bytes, byte[]>>} and
 * name the result store explicitly.
 *
 * <h2>Why FK joins are uniquely hazardous without Materialized</h2>
 *
 * <p>A FK join is implemented as a multi-stage internal topology:
 *
 * <ul>
 *   <li><b>Subscription store</b> — a state store on the right-side
 *       partitioning of the foreign key, holding which left-side
 *       primary keys are subscribed to each foreign key. Auto-named
 *       e.g. {@code KTABLE-FK-JOIN-SUBSCRIPTION-STATE-STORE-
 *       0000000007}.</li>
 *   <li><b>Subscription topic</b> — an internal repartition topic
 *       that ships left-side subscription updates from left
 *       partitioning to right partitioning. Auto-named e.g.
 *       {@code KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-
 *       0000000008-topic}.</li>
 *   <li><b>Response topic</b> — an internal repartition topic that
 *       ships right-side value updates back from right partitioning
 *       to left partitioning for emit. Auto-named e.g.
 *       {@code KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-0000000009-
 *       topic}.</li>
 *   <li><b>Response store</b> — a state store on the left-side
 *       partitioning that materializes the join result. Auto-named
 *       e.g. {@code KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-STATE-
 *       STORE-0000000010}.</li>
 * </ul>
 *
 * <p>Without an explicit {@link
 * org.apache.kafka.streams.kstream.Materialized#as(String)}, ALL
 * FOUR are auto-named from the topology graph index. Any
 * upstream edit (a new {@code mapValues}, a renamed source) shifts
 * the sequence numbers and renames all four artifacts.
 *
 * <h2>Concrete failure modes (carried verbatim into the violation
 * message)</h2>
 *
 * <ul>
 *   <li><b>Quadruple-rename on topology edit.</b> A team adds one
 *       upstream filter above an order-customer FK join. On
 *       deploy, all four FK-join artifacts get new names. Streams
 *       creates four new internal topics (subscription + response)
 *       and provisions two new state stores (subscription +
 *       response). The four old topics are abandoned but not
 *       deleted (Streams never deletes internal topics on rename),
 *       silently accumulating cluster storage. The two new state
 *       stores rebuild from offset 0 of the new subscription and
 *       response topics — but those topics are EMPTY until upstream
 *       changes flow through; the join produces nothing for the
 *       entire warm-up window. Order-enrichment output drops to
 *       zero until the upstream KTable's full state has cycled
 *       through the FK extractor and seeded the new subscription
 *       store.</li>
 *   <li><b>Internal-topic ACL drift / quota mismatch.</b> Ops grants
 *       per-topic READ/WRITE ACLs and per-topic produce quotas
 *       against the original auto-generated subscription/response
 *       topic names. After a topology edit, the four new
 *       auto-generated names don't match any granted ACL. The
 *       Streams instance gets {@code TopicAuthorizationException}
 *       on the first FK update and the application enters a crash
 *       loop. The fix requires ops to grant ACLs on the new names
 *       — a 30-minute outage minimum, longer if the ops team is
 *       in another timezone.</li>
 *   <li><b>Internal-topic replication-factor drift.</b> Subscription
 *       and response topics are created by Streams at startup with
 *       {@code topic.replication.factor} from the Streams config
 *       (default 1 on dev, often 3 on prod). On a topology edit
 *       in an environment where the config diverged from the
 *       broker's default at original creation time, the new topics
 *       may have different replication. Loss of one broker in the
 *       window between rename and rebuild loses join state.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures bypass
 *       naïve {@code MethodInsnNode}-only lint.</b> A FK-join
 *       factory built as
 *       {@code QuadFunction<KTable, KTable, Function, ValueJoiner,
 *       KTable> joiner = orders::join} compiles to
 *       {@code INVOKEDYNAMIC} whose bsm-args contain a
 *       {@code REF_invokeInterface} Handle pointing at
 *       {@code KTable.join(KTable, Function, ValueJoiner)KTable}.
 *       The user-class bytecode contains zero direct
 *       {@code INVOKEINTERFACE} on the no-Materialized overload —
 *       only the indy site.</li>
 * </ul>
 *
 * <p>Predicate is: descriptor contains a {@code Function} parameter
 * (FK-join family) AND descriptor does NOT contain a
 * {@code Materialized} parameter (unsafe variant of that family).
 * Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Materialized} naming the result
 * store — e.g.
 * {@code orders.join(customers, Order::customerId, EnrichedOrder::new,
 * Materialized.<String, EnrichedOrder, KeyValueStore<Bytes, byte[]>>
 * as("orders-by-customer-enriched"))}. The chosen result-store name
 * must be stable across topology edits because all four internal
 * artifacts derive from it and the subscription warm-up window is
 * silent (zero output).
 */
public final class StreamsForeignKeyJoinNoMaterializedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KTABLE);
    private static final Set<String> METHOD_NAMES = Set.of("join", "leftJoin");
    private static final String FUNCTION = "Ljava/util/function/Function;";
    private static final String MATERIALIZED =
            "Lorg/apache/kafka/streams/kstream/Materialized;";

    private final Severity severity;

    public StreamsForeignKeyJoinNoMaterializedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_FOREIGN_KEY_JOIN_NO_MATERIALIZED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAMES.contains(mi.name)
                        && isUnsafeFkJoinDesc(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = findFkJoinHandle(indy);
                    if (h != null) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private static boolean isUnsafeFkJoinDesc(String desc) {
        return desc != null && desc.contains(FUNCTION) && !desc.contains(MATERIALIZED);
    }

    private static Handle findFkJoinHandle(InvokeDynamicInsnNode indy) {
        for (String name : METHOD_NAMES) {
            Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, name, null);
            if (h != null && isUnsafeFkJoinDesc(h.getDesc())) {
                return h;
            }
        }
        return null;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_FOREIGN_KEY_JOIN_NO_MATERIALIZED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KTable.join(KTable, Function, ValueJoiner, ...) / "
                        + "KTable.leftJoin(KTable, Function, ValueJoiner, "
                        + "...) — a foreign-key join overload with NO "
                        + "Materialized argument is reached here — either "
                        + "as a direct INVOKEINTERFACE on the method or "
                        + "as an INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `orders::join` bound to a SAM whose "
                        + "erased implMethod descriptor contains Function "
                        + "but not Materialized). A FK join is implemented "
                        + "as a multi-stage internal topology: (a) "
                        + "subscription store on the right-side "
                        + "partitioning of the foreign key — auto-named "
                        + "e.g. `KTABLE-FK-JOIN-SUBSCRIPTION-STATE-"
                        + "STORE-0000000007`; (b) subscription topic — "
                        + "internal repartition topic shipping left-side "
                        + "subscription updates from left to right "
                        + "partitioning — auto-named e.g. `KTABLE-FK-"
                        + "JOIN-SUBSCRIPTION-REGISTRATION-0000000008-"
                        + "topic`; (c) response topic — internal "
                        + "repartition topic shipping right-side value "
                        + "updates from right back to left for emit — "
                        + "auto-named e.g. `KTABLE-FK-JOIN-SUBSCRIPTION-"
                        + "RESPONSE-0000000009-topic`; (d) response "
                        + "store on the left-side partitioning that "
                        + "materializes the join result — auto-named "
                        + "e.g. `KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-"
                        + "STATE-STORE-0000000010`. Without explicit "
                        + "Materialized.as(\"name\"), ALL FOUR names are "
                        + "derived from the topology graph index; any "
                        + "upstream edit shifts the sequence numbers and "
                        + "renames all four. Concrete failure modes: (1) "
                        + "quadruple-rename on topology edit — team adds "
                        + "one upstream filter above an order-customer FK "
                        + "join; on deploy all four FK-join artifacts get "
                        + "new names; Streams creates two new internal "
                        + "topics and provisions two new state stores; "
                        + "the four old topics are abandoned but not "
                        + "deleted (Streams never deletes internal "
                        + "topics on rename), silently accumulating "
                        + "cluster storage; the two new state stores "
                        + "rebuild from offset 0 of the new "
                        + "subscription/response topics but those topics "
                        + "are EMPTY until upstream changes flow through; "
                        + "the join produces nothing for the entire "
                        + "warm-up window; order-enrichment output drops "
                        + "to zero until the upstream KTable's full "
                        + "state has cycled through the FK extractor and "
                        + "seeded the new subscription store; (2) "
                        + "internal-topic ACL drift / quota mismatch — "
                        + "ops grants per-topic READ/WRITE ACLs and "
                        + "produce quotas against the original "
                        + "auto-generated subscription/response topic "
                        + "names; after a topology edit the four new "
                        + "auto-generated names don't match any granted "
                        + "ACL; Streams instance gets TopicAuthorization"
                        + "Exception on the first FK update and the "
                        + "application enters a crash loop; fix requires "
                        + "ops to grant ACLs on the new names — 30-minute "
                        + "outage minimum, longer if ops is in another "
                        + "timezone; (3) internal-topic replication-"
                        + "factor drift — subscription/response topics "
                        + "are created at startup with topic.replication."
                        + "factor from the Streams config (default 1 on "
                        + "dev, often 3 on prod); on a topology edit "
                        + "where the config diverged from the broker's "
                        + "default at original creation time, the new "
                        + "topics may have different replication; loss "
                        + "of one broker in the window between rename "
                        + "and rebuild loses join state; (4) "
                        + "INVOKEDYNAMIC method-reference captures "
                        + "bypass naive MethodInsnNode-only lint — `Quad"
                        + "Function<KTable, KTable, Function, ValueJoiner,"
                        + " KTable> factory = orders::join` compiles to "
                        + "INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at KTable."
                        + "join(KTable, Function, ValueJoiner)KTable; the "
                        + "user-class bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Materialized "
                        + "overload, only the indy site. Migration: pass "
                        + "an explicit Materialized naming the result "
                        + "store — `orders.join(customers, Order::"
                        + "customerId, EnrichedOrder::new, Materialized."
                        + "<String, EnrichedOrder, KeyValueStore<Bytes, "
                        + "byte[]>>as(\"orders-by-customer-enriched\"))`. "
                        + "The chosen result-store name must be stable "
                        + "across topology edits because all four "
                        + "internal artifacts derive from it and the "
                        + "subscription warm-up window is silent (zero "
                        + "output). The safe overloads (with "
                        + "Materialized) are never flagged by this rule, "
                        + "and same-key (non-FK) join/leftJoin overloads "
                        + "without a Function parameter are also never "
                        + "flagged.");
    }
}
