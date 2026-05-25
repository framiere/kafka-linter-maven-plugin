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
 * Fires for every reach of a {@link
 * org.apache.kafka.streams.kstream.KStream#toTable()
 * KStream.toTable()} or {@link
 * org.apache.kafka.streams.kstream.KStream#toTable(
 * org.apache.kafka.streams.kstream.Named)
 * KStream.toTable(Named)} overload — the two overloads that do
 * NOT take a {@link org.apache.kafka.streams.kstream.Materialized
 * Materialized} argument and therefore leave the resulting
 * KTable's internal state store auto-named from the topology
 * graph index.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls (KStream
 * is an interface) and {@code INVOKEDYNAMIC} method-reference
 * captures (e.g. {@code stream::toTable} bound to a {@link
 * java.util.function.Supplier Supplier&lt;KTable&gt;} or to a
 * {@link java.util.function.Function Function&lt;Named,
 * KTable&gt;} or to a custom SAM whose erased implMethod
 * descriptor matches one of the two unsafe overload
 * descriptors exactly).
 *
 * <h2>Why no-Materialized toTable is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.streams.kstream.KStream#toTable
 * KStream.toTable} promotes a stream of {@code (K, V)} records
 * into a KTable. Internally, Streams must materialize the latest
 * value per key into a state store. The promotion happens through
 * a synthetic source/processor node that:
 *
 * <ul>
 *   <li><b>Creates a backing state store</b> with a name derived
 *       from the topology graph index — e.g.
 *       {@code KSTREAM-TOTABLE-STATE-STORE-0000000005}. The store
 *       holds the materialized table contents.</li>
 *   <li><b>Creates an internal changelog topic</b> — e.g.
 *       {@code my-app-KSTREAM-TOTABLE-STATE-STORE-0000000005-
 *       changelog} — to back the state store across restarts and
 *       rebalances.</li>
 *   <li><b>Creates a repartition topic upstream</b> if the source
 *       stream is repartition-required (any prior selectKey, map,
 *       flatMap). The repartition topic is also auto-named.</li>
 * </ul>
 *
 * <p>Without an explicit {@link
 * org.apache.kafka.streams.kstream.Materialized#as(String)}, ALL
 * three (or two, depending on whether repartition is required)
 * auto-derive from the same graph index. Any upstream topology
 * edit shifts the sequence numbers and renames the artifacts.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>Silently-empty table after a topology edit.</b> A team
 *       has a stream-side projection {@code events.toTable()}
 *       producing a KTable used in a downstream join. The
 *       backing changelog is auto-named e.g.
 *       {@code my-app-KSTREAM-TOTABLE-STATE-STORE-0000000005-
 *       changelog}. Team ships a hotfix that adds one upstream
 *       filter (drops a deprecated record type); graph index
 *       shifts; the new instance creates a NEW changelog topic
 *       and a NEW state store with new auto-names; the OLD
 *       changelog still exists on the brokers, full of every
 *       compacted-latest record the table previously held, but
 *       the new state store knows nothing about it; the new
 *       state store starts empty and only populates as upstream
 *       re-emits each key; downstream join produces null for
 *       every key that has not yet been re-emitted; if upstream
 *       is bursty or compacted, some keys may never re-emit;
 *       the table stays silently incomplete forever.</li>
 *   <li><b>Old changelog accumulates cluster storage.</b>
 *       Streams never deletes auto-created changelog topics. The
 *       old {@code KSTREAM-TOTABLE-STATE-STORE-0000000005-
 *       changelog} continues to consume cluster disk indefinitely,
 *       and every subsequent topology edit adds another orphaned
 *       changelog. A team with weekly deploys accumulates 52
 *       orphan changelogs per year per toTable call site;
 *       multiplied by N call sites and M Streams applications
 *       sharing a cluster, the bytes-on-disk grows unboundedly
 *       until an SRE manually audits and deletes.</li>
 *   <li><b>Interactive queries fail.</b> A monitoring dashboard
 *       or HTTP endpoint does
 *       {@code streams.store(StoreQueryParameters.fromNameAndType(
 *       "events-table", QueryableStoreTypes.<String, Event>
 *       keyValueStore()))} expecting a stable store name; without
 *       {@code Materialized.as("events-table")} the store is
 *       named by the topology builder; the call throws
 *       {@code InvalidStateStoreException} or returns a store
 *       object pointing at the wrong data.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures bypass
 *       naïve {@code MethodInsnNode}-only lint.</b> A promotion
 *       factory built as
 *       {@code Supplier<KTable<String, Event>> promote =
 *       events::toTable} compiles to {@code INVOKEDYNAMIC} whose
 *       bsm-args contain a {@code REF_invokeInterface} Handle
 *       pointing at {@code KStream.toTable()KTable} — the
 *       0-arg unsafe descriptor exactly. The user-class
 *       bytecode contains zero direct {@code INVOKEINTERFACE}
 *       on the no-Materialized overloads, only the indy site.</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the two unsafe overload
 * descriptors. Migration: pass an explicit {@link
 * org.apache.kafka.streams.kstream.Materialized} naming the
 * store — e.g. {@code events.toTable(Named.as("events-promotion"),
 * Materialized.<String, Event, KeyValueStore<Bytes, byte[]>>
 * as("events-table"))}. The chosen store name must be stable
 * across topology edits because the changelog topic name is
 * derived from it.
 */
public final class StreamsToTableNoMaterializedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final String METHOD_NAME = "toTable";
    private static final String UNSAFE_DESC_NULLARY =
            "()Lorg/apache/kafka/streams/kstream/KTable;";
    private static final String UNSAFE_DESC_NAMED =
            "(Lorg/apache/kafka/streams/kstream/Named;)"
                    + "Lorg/apache/kafka/streams/kstream/KTable;";

    private final Severity severity;

    public StreamsToTableNoMaterializedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_TO_TABLE_NO_MATERIALIZED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && (UNSAFE_DESC_NULLARY.equals(mi.desc)
                                || UNSAFE_DESC_NAMED.equals(mi.desc))) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy
                        && (AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, UNSAFE_DESC_NULLARY) != null
                                || AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, UNSAFE_DESC_NAMED) != null)) {
                    out.add(violation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_TO_TABLE_NO_MATERIALIZED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.toTable() / toTable(Named) — a "
                        + "no-Materialized overload is reached here — "
                        + "either as a direct INVOKEINTERFACE on the "
                        + "method or as an INVOKEDYNAMIC method-"
                        + "reference capture (e.g. `events::toTable` "
                        + "bound to Supplier<KTable> / Function<Named, "
                        + "KTable> / custom SAM whose erased implMethod "
                        + "descriptor matches). KStream.toTable promotes "
                        + "a stream of (K, V) records into a KTable; "
                        + "internally it creates a backing state store "
                        + "auto-named e.g. `KSTREAM-TOTABLE-STATE-STORE-"
                        + "0000000005` from the topology graph index, an "
                        + "internal changelog topic auto-named e.g. "
                        + "`my-app-KSTREAM-TOTABLE-STATE-STORE-"
                        + "0000000005-changelog`, and (if the upstream "
                        + "stream is repartition-required) an upstream "
                        + "repartition topic also auto-named. Concrete "
                        + "failure modes: (1) silently-empty table after "
                        + "a topology edit — team has `events.toTable()` "
                        + "producing a KTable used in a downstream join; "
                        + "backing changelog auto-named `my-app-KSTREAM-"
                        + "TOTABLE-STATE-STORE-0000000005-changelog`; "
                        + "team adds one upstream filter; graph index "
                        + "shifts; the new instance creates a NEW "
                        + "changelog topic and a NEW state store with "
                        + "new auto-names; the OLD changelog still "
                        + "exists on the brokers, full of every "
                        + "compacted-latest record the table previously "
                        + "held, but the new state store knows nothing "
                        + "about it; new state store starts empty and "
                        + "only populates as upstream re-emits each "
                        + "key; downstream join produces null for every "
                        + "key not yet re-emitted; if upstream is bursty "
                        + "or compacted, some keys may never re-emit; "
                        + "the table stays silently incomplete forever; "
                        + "(2) old changelog accumulates cluster storage "
                        + "— Streams never deletes auto-created "
                        + "changelog topics; old `KSTREAM-TOTABLE-STATE-"
                        + "STORE-0000000005-changelog` continues to "
                        + "consume cluster disk indefinitely; every "
                        + "subsequent topology edit adds another orphan; "
                        + "a team with weekly deploys accumulates 52 "
                        + "orphan changelogs per year per toTable call "
                        + "site; multiplied by N call sites and M "
                        + "Streams apps sharing a cluster, bytes-on-disk "
                        + "grows unboundedly until an SRE manually "
                        + "audits; (3) interactive queries fail — "
                        + "monitoring dashboard does `streams.store("
                        + "StoreQueryParameters.fromNameAndType(\""
                        + "events-table\", QueryableStoreTypes.<String, "
                        + "Event>keyValueStore()))` expecting a stable "
                        + "store name; without Materialized.as(\""
                        + "events-table\") the store is named by the "
                        + "topology builder; the call throws "
                        + "InvalidStateStoreException or returns a "
                        + "store object pointing at the wrong data; "
                        + "(4) INVOKEDYNAMIC method-reference captures "
                        + "bypass naive MethodInsnNode-only lint — "
                        + "`Supplier<KTable<String, Event>> promote = "
                        + "events::toTable` compiles to INVOKEDYNAMIC "
                        + "whose bsm-args contain a REF_invokeInterface "
                        + "Handle pointing at KStream.toTable()KTable; "
                        + "the user-class bytecode contains zero direct "
                        + "INVOKEINTERFACE on the no-Materialized "
                        + "overloads, only the indy site. Migration: "
                        + "pass an explicit Materialized naming the "
                        + "store — `events.toTable(Named.as(\""
                        + "events-promotion\"), Materialized.<String, "
                        + "Event, KeyValueStore<Bytes, byte[]>>as(\""
                        + "events-table\"))`. The chosen store name "
                        + "must be stable across topology edits because "
                        + "the changelog topic name is derived from it. "
                        + "The two safe overloads (with Materialized) "
                        + "are never flagged by this rule.");
    }
}
