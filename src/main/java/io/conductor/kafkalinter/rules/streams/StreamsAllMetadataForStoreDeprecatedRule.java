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
import java.util.Set;

/**
 * Fires for every reach of the deprecated Interactive-Query
 * metadata-listing methods on {@link org.apache.kafka.streams.KafkaStreams}:
 * {@code allMetadata()} and {@code allMetadataForStore(String)}.
 * Whether the call lands directly via {@code INVOKEVIRTUAL} or
 * indirectly through an {@code INVOKEDYNAMIC} method-reference capture
 * (e.g. {@code streams::allMetadataForStore} bound to a SAM whose
 * erased signature is {@code (KafkaStreams, String) -> Collection}),
 * the rule fires.
 *
 * <h2>Why these methods are deprecated</h2>
 *
 * <p>KIP-744 (Kafka Streams 3.0, September 2021) deprecated both
 * {@code allMetadata()} and {@code allMetadataForStore(String)} along
 * with the {@code org.apache.kafka.streams.state.StreamsMetadata} type
 * they return. The old type predates Kafka Streams standby-replica
 * support being first-class in Interactive Queries: it exposes only
 * the active host's {@code hostInfo()}, {@code stateStoreNames()},
 * and {@code topicPartitions()} — no standby-host fields. The new
 * {@code org.apache.kafka.streams.StreamsMetadata} adds
 * {@code standbyStateStoreNames()} and {@code standbyTopicPartitions()},
 * and the new methods {@code metadataForAllStreamsClients()} and
 * {@code streamsMetadataForStore(String)} return collections of the
 * new type. The deprecation tracks four concrete IQ-availability
 * incident classes:
 *
 * <ul>
 *   <li><b>IQ endpoint returns 503 for the entire active-restore
 *       window.</b> When a Kafka Streams active host loses its state
 *       store (pod restart, disk corruption, node replacement), the
 *       store must be restored from the changelog topic before
 *       {@code KafkaStreams.store(...)} can serve queries. For a
 *       multi-GB store, restoration can take minutes to hours. With
 *       the legacy {@code allMetadataForStore} return type, the IQ
 *       router has no way to know that another host is running as
 *       standby for the same state-store — the standby fields don't
 *       exist on the legacy {@code StreamsMetadata}. The router's
 *       only option is to wait for the active to finish restore and
 *       return 503 to every IQ request in the meantime, or worse,
 *       proxy the request to the active where {@code store(...)}
 *       throws {@code InvalidStateStoreException}. The new
 *       {@code streamsMetadataForStore} exposes
 *       {@code standbyStateStoreNames()}, letting the router route
 *       IQ requests to a standby host that already has a warm replica
 *       of the store. The standby's read is bounded by changelog lag
 *       (typically seconds), not by the full restore window
 *       (typically minutes-to-hours). Recovery-time-objective on the
 *       IQ surface drops from "minutes" to "seconds" without changing
 *       the topology.</li>
 *   <li><b>Standby-aware least-loaded routing impossible with the
 *       legacy type.</b> An IQ router that wants to balance load
 *       across multiple hosts that hold a copy of the same store
 *       (one active plus N-1 standbys) needs to know which hosts
 *       have which standby stores. The legacy
 *       {@code StreamsMetadata.stateStoreNames()} only includes the
 *       active store names; the router cannot tell whether a given
 *       host is a candidate for a given store unless that host is
 *       the active. Practical effect: even when standbys exist and
 *       are warm, the legacy IQ router can only route to the active,
 *       so traffic concentrates on one host while the standbys sit
 *       idle holding warm replicas. The new
 *       {@code standbyStateStoreNames()} unlocks round-robin or
 *       least-connected balancing across all hosts that hold the
 *       store.</li>
 *   <li><b>Standby-aware queries with consistency tradeoffs require
 *       the new type.</b> KIP-535 (Kafka Streams 2.5) and KIP-796
 *       (Kafka Streams 3.2) introduced {@code IQv2}'s
 *       {@code StateQueryRequest.enableExecutionInfo()} and
 *       {@code withPositionBound(...)} so the caller can choose
 *       between read-from-active (linearizable) and read-from-standby
 *       (bounded staleness). The IQ router that drives
 *       {@code IQv2.query(...)} on the right host must know the
 *       standby topology to make that choice — which the legacy
 *       {@code StreamsMetadata} cannot express.</li>
 *   <li><b>INVOKEDYNAMIC {@code streams::allMetadataForStore}
 *       captures silently bind to the deprecated overload.</b> A
 *       routing-factory abstraction (e.g. an IQ middleware that
 *       takes {@code Function<String, Collection<StreamsMetadata>>}
 *       to look up hosts for a store) resolves the method-ref by
 *       arity and erased argument types. The user-class bytecode at
 *       the capture site contains zero direct {@code INVOKEVIRTUAL}
 *       on the legacy method — only the {@code INVOKEDYNAMIC} +
 *       {@code LambdaMetafactory} bridge. A name-only MethodInsnNode
 *       walk misses this entirely.</li>
 * </ul>
 *
 * <h2>The replacement API</h2>
 *
 * <p>The new overloads return collections of the new
 * {@code org.apache.kafka.streams.StreamsMetadata} type:
 *
 * <pre>{@code
 *   // Old (fires):
 *   Collection<org.apache.kafka.streams.state.StreamsMetadata> all = streams.allMetadata();
 *   Collection<org.apache.kafka.streams.state.StreamsMetadata> forStore = streams.allMetadataForStore("my-store");
 *
 *   // New (does not fire):
 *   Collection<org.apache.kafka.streams.StreamsMetadata> all = streams.metadataForAllStreamsClients();
 *   Collection<org.apache.kafka.streams.StreamsMetadata> forStore = streams.streamsMetadataForStore("my-store");
 *
 *   for (StreamsMetadata md : forStore) {
 *       boolean isActive = md.stateStoreNames().contains("my-store");
 *       boolean isStandby = md.standbyStateStoreNames().contains("my-store");
 *       // Route the IQ request to active OR standby.
 *   }
 * }</pre>
 *
 * <h2>Multi-name dispatch — two deprecated method names share one
 * semantic</h2>
 *
 * <p>Both {@code allMetadata} and {@code allMetadataForStore} return
 * {@code Collection<org.apache.kafka.streams.state.StreamsMetadata>}
 * (the old type). They differ only in scope:
 * {@code allMetadataForStore} filters to hosts that host a given
 * store. Both names share the same migration story (rename to the
 * new method that returns the new type). The rule's name set is
 * {@code {"allMetadata", "allMetadataForStore"}}; each name has a
 * distinct descriptor (zero-arg vs single-String-arg), so a multi-
 * descriptor set is also required.
 *
 * <h2>Descriptor discrimination</h2>
 *
 * <p>The legacy methods return {@code Collection<state.StreamsMetadata>}
 * which erases at the bytecode level to {@code Collection} — the same
 * raw return as the new methods. The signature discrimination must
 * therefore be on the {@code (owner, name)} pair, not on the return
 * descriptor. The owner is pinned to
 * {@code org/apache/kafka/streams/KafkaStreams} and the name is
 * pinned to the two-element set above. Descriptor discriminates
 * arity:
 *
 * <ul>
 *   <li>{@code allMetadata} →
 *       {@code ()Ljava/util/Collection;}</li>
 *   <li>{@code allMetadataForStore} →
 *       {@code (Ljava/lang/String;)Ljava/util/Collection;}</li>
 * </ul>
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code streams::allMetadataForStore} bound to a SAM whose
 * erased argument is {@code String} compiles to {@code INVOKEDYNAMIC}
 * whose bsm-args contain a {@code REF_invokeVirtual} handle pointing
 * at the legacy method. The rule's bsm-arg walk catches this case by
 * checking the handle's {@code (owner, name, desc)} triple against
 * the same filter used for direct calls, iterated over both legacy
 * descriptors.
 */
public final class StreamsAllMetadataForStoreDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KAFKA_STREAMS);

    private static final Map<String, String> NAME_TO_LEGACY_DESC = Map.of(
            "allMetadata", "()Ljava/util/Collection;",
            "allMetadataForStore", "(Ljava/lang/String;)Ljava/util/Collection;");

    private final Severity severity;

    public StreamsAllMetadataForStoreDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_ALL_METADATA_FOR_STORE_DEPRECATED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi && OWNERS.contains(mi.owner)) {
                    String expectedDesc = NAME_TO_LEGACY_DESC.get(mi.name);
                    if (expectedDesc != null && expectedDesc.equals(mi.desc)) {
                        out.add(violation(ctx, mn, insn));
                        continue;
                    }
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    for (Map.Entry<String, String> e : NAME_TO_LEGACY_DESC.entrySet()) {
                        Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, e.getKey(), e.getValue());
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
                RuleId.STREAMS_ALL_METADATA_FOR_STORE_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KafkaStreams.allMetadata() or KafkaStreams.allMetadataForStore(String) "
                        + "is reached here — either as a direct call or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`streams::allMetadataForStore` bound to an IQ routing-factory "
                        + "SAM). Both methods are deprecated since Kafka Streams 3.0 "
                        + "(KIP-744, September 2021) because they return the legacy "
                        + "`org.apache.kafka.streams.state.StreamsMetadata`, which "
                        + "predates standby-replica support being first-class in "
                        + "Interactive Queries: it exposes only the active host's "
                        + "stateStoreNames() and topicPartitions() with no standby "
                        + "fields. The new `org.apache.kafka.streams.StreamsMetadata` "
                        + "(returned by metadataForAllStreamsClients() and "
                        + "streamsMetadataForStore(String)) adds standbyStateStoreNames() "
                        + "and standbyTopicPartitions(), which unlocks four IQ-"
                        + "availability incident classes the legacy type silently makes "
                        + "worse: (1) IQ endpoint returns 503 for the entire active-"
                        + "restore window — when an active host loses its store (pod "
                        + "restart, disk, node replacement), restoration from the "
                        + "changelog can take minutes to hours; the legacy "
                        + "StreamsMetadata has no way to expose that another host "
                        + "already has a warm standby of the same store, so the IQ "
                        + "router must either wait or return 503 to every request; the "
                        + "new type lets the router fail over to the standby and serve "
                        + "the query within seconds (bounded by changelog lag) rather "
                        + "than minutes-to-hours; (2) standby-aware least-loaded "
                        + "routing impossible with the legacy type — an IQ router that "
                        + "wants to balance traffic across hosts holding a copy of the "
                        + "same store (one active + N-1 standbys) needs to know which "
                        + "hosts have which standby stores, but legacy "
                        + "stateStoreNames() only enumerates active stores, so traffic "
                        + "concentrates on the active and standbys sit idle holding "
                        + "warm replicas; (3) IQv2 standby-with-bounded-staleness "
                        + "queries (KIP-796) require the new type's standby fields to "
                        + "drive read-from-active vs read-from-standby routing per "
                        + "request; (4) INVOKEDYNAMIC `streams::allMetadataForStore` "
                        + "captures silently bind to the deprecated overload — the "
                        + "user-class bytecode contains zero direct INVOKEVIRTUAL on "
                        + "the legacy method and a name-only walk misses it. Migrate "
                        + "to `streams.metadataForAllStreamsClients()` and "
                        + "`streams.streamsMetadataForStore(\"my-store\")`. The IQ "
                        + "router can then inspect each returned StreamsMetadata's "
                        + "standbyStateStoreNames() to fail over to a standby while "
                        + "the active is restoring.");
    }
}
