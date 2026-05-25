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
 * {@link org.apache.kafka.streams.kstream.KStream#through(String)} and
 * {@link org.apache.kafka.streams.kstream.KStream#through(String,
 * org.apache.kafka.streams.kstream.Produced)} — whether the call lands
 * directly via {@code INVOKEINTERFACE} or indirectly through an
 * {@code INVOKEDYNAMIC} method-reference capture (e.g.
 * {@code stream::through} bound to a SAM that takes {@code String} or
 * {@code (String, Produced)}).
 *
 * <h2>Why this method is deprecated, not just a name change</h2>
 *
 * <p>{@code KStream.through(topic)} is the pre-KIP-221 way to force a
 * repartition in a Kafka Streams topology: it sinks the upstream
 * records to a Kafka topic the operator named and pre-provisioned,
 * then re-reads from that topic to produce a downstream
 * {@code KStream}. KIP-221 (Kafka Streams 2.6, August 2020) deprecated
 * both overloads in favour of {@code KStream.repartition([Repartitioned])},
 * which is semantically equivalent but moves topic ownership from the
 * operator to the framework. The motivation is a long list of
 * production-incident classes that the old API made easy:
 *
 * <ul>
 *   <li><b>Wrong partition count silently breaks key-based downstream
 *       operators.</b> {@code through(topic)} re-reads from {@code topic}
 *       with whatever partition count {@code topic} happens to have on
 *       the broker. If the operator provisioned the topic with a
 *       different partition count than the upstream source — or if the
 *       broker auto-created the topic with {@code num.partitions} (the
 *       broker default, often 1) because the operator forgot — the
 *       downstream {@code aggregate} / {@code reduce} / {@code join}
 *       silently produces wrong results: the partitioner assigns each
 *       key to a different partition than the co-partitioning math
 *       expects, so the same logical key lands on different
 *       state-store shards on different stream-threads. There is no
 *       runtime check; the only observable symptom is silently
 *       incorrect aggregates that don't reconcile against any other
 *       source of truth. {@code repartition()} introspects the upstream
 *       topology, creates the internal topic with the matching
 *       partition count, and registers it under the application
 *       ID so {@code kafka-streams-application-reset} can clean it
 *       up.</li>
 *   <li><b>Topic lifecycle is operator-owned, application-decoupled.</b>
 *       A topic referenced by {@code through("intermediate-topic")} is
 *       a regular Kafka topic — it persists across application
 *       redeploys, application-ID changes, and topology rewrites. When
 *       a topology is refactored to no longer use a given intermediate,
 *       the topic is orphaned: it consumes disk indefinitely, it shows
 *       up in cluster topic listings as an unowned producer-less
 *       artefact, and the operator must remember to delete it
 *       manually. {@code repartition()} names the internal topic
 *       {@code <app-id>-<KSTREAM-REPARTITION-N>-repartition} and
 *       {@code KafkaStreams.cleanUp()} / {@code application-reset}
 *       deletes it transactionally with the application state.</li>
 *   <li><b>No per-call serdes for the keyless overload.</b>
 *       {@code through(topic)} (single-arg) wires serdes via the
 *       application-wide {@code StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG}
 *       / {@code DEFAULT_VALUE_SERDE_CLASS_CONFIG} defaults. If the
 *       intermediate record schema differs from the application
 *       default — for example, the upstream operator changed the value
 *       type from {@code String} to a Protobuf message after a
 *       {@code mapValues} — the round-trip silently writes with the
 *       wrong default serializer and reads back with the wrong
 *       default deserializer. The {@code Produced<K, V>} overload
 *       solves this but is itself deprecated under the same
 *       deprecation marker on the type. {@code repartition(Repartitioned
 *       .with(keySerde, valueSerde))} takes per-call serdes that
 *       compose with the upstream node type without leaking
 *       configuration into application-wide defaults.</li>
 *   <li><b>No declared internal-topic-config control.</b> An operator
 *       who set {@code through("intermediate")} has to provision the
 *       topic with the right cleanup policy, retention, segment size,
 *       and min-ISR. If the operator forgot to set {@code cleanup.policy=delete}
 *       and {@code retention.ms} matching the application reset
 *       cadence, the intermediate topic accumulates indefinitely
 *       (compaction does not apply — records are keyed by the
 *       repartition key, not by a logical identity that compaction
 *       can reason about). {@code repartition()} provisions the internal
 *       topic with the right per-application config, computed from the
 *       upstream node type.</li>
 *   <li><b>INVOKEDYNAMIC {@code stream::through} captures silently
 *       bind to the deprecated method.</b> A topology-factory
 *       abstraction (e.g. a generic {@code ThroughFn<K, V>} SAM that
 *       takes {@code (KStream<K, V>, String) -> KStream<K, V>})
 *       resolves the method-ref by arity and erased argument types to
 *       one of the two legacy {@code through} overloads. The
 *       user-class bytecode contains zero direct {@code INVOKEINTERFACE}
 *       on the legacy method — only the {@code INVOKEDYNAMIC} +
 *       {@code LambdaMetafactory} bridge. A name-only MethodInsnNode
 *       walk misses this case entirely.</li>
 * </ul>
 *
 * <h2>The replacement API</h2>
 *
 * <p>KIP-221 introduced {@code KStream.repartition([Repartitioned])}
 * in Kafka Streams 2.6 (August 2020) as the framework-owned
 * replacement:
 *
 * <pre>{@code
 *   // Old:
 *   KStream<K, V> rekeyed = stream.through("my-intermediate");
 *
 *   // New, default config:
 *   KStream<K, V> rekeyed = stream.repartition();
 *
 *   // New, per-call serdes and partitioner:
 *   KStream<K, V> rekeyed = stream.repartition(
 *       Repartitioned.<K, V>as("by-customer")
 *           .withKeySerde(customerSerde)
 *           .withValueSerde(orderSerde)
 *           .withNumberOfPartitions(64));
 * }</pre>
 *
 * <p>{@code repartition()} provisions the internal topic with the
 * matching partition count, wires serdes from the upstream node type,
 * applies the named partitioner (or the default), and tags the topic
 * with the application ID so cleanup is transactional with the
 * application reset.
 *
 * <h2>Descriptor discrimination — two legacy overloads</h2>
 *
 * <p>There are two distinct deprecated descriptors the rule must
 * catch, because both compile to distinct {@code INVOKEINTERFACE} /
 * {@code REF_invokeInterface} bytecode shapes:
 *
 * <ul>
 *   <li>{@code through(String)} →
 *       {@code (Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KStream;}</li>
 *   <li>{@code through(String, Produced)} →
 *       {@code (Ljava/lang/String;Lorg/apache/kafka/streams/kstream/Produced;)Lorg/apache/kafka/streams/kstream/KStream;}</li>
 * </ul>
 *
 * <p>Both are deprecated. The rule iterates over both descriptors at
 * each candidate instruction. The name {@code "through"} is also used
 * by some unrelated Kafka Streams test fixtures and by user code, but
 * the owner pin to {@code KStream} discriminates: only call sites on
 * the {@code KStream} interface match.
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code stream::through} bound to a SAM that takes
 * {@code (KStream, String)} or {@code (KStream, String, Produced)}
 * compiles to {@code INVOKEDYNAMIC} whose bsm-args contain a
 * {@code REF_invokeInterface} handle pointing at the resolved legacy
 * method. The rule's bsm-arg walk catches this case by checking the
 * handle's {@code (owner, name, desc)} triple against the same filter
 * used for direct calls, iterated over both legacy descriptors.
 */
public final class StreamsThroughDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KSTREAM);
    private static final String METHOD_NAME = "through";
    private static final Set<String> LEGACY_DESCS = Set.of(
            "(Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KStream;",
            "(Ljava/lang/String;Lorg/apache/kafka/streams/kstream/Produced;)Lorg/apache/kafka/streams/kstream/KStream;");

    private final Severity severity;

    public StreamsThroughDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_THROUGH_DEPRECATED;
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
                RuleId.STREAMS_THROUGH_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KStream.through(String) (or its Produced-overload sibling "
                        + "KStream.through(String, Produced)) is reached here — either as "
                        + "a direct call or as an INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `stream::through` bound to a topology-factory SAM that "
                        + "takes String or (String, Produced)). This method is deprecated "
                        + "since Kafka Streams 2.6 (KIP-221, August 2020) because the "
                        + "intermediate topic it relies on is operator-owned rather than "
                        + "framework-owned, which leaks four classes of correctness and "
                        + "lifecycle bugs into production: (1) wrong partition count "
                        + "silently breaks key-based downstream operators — through() "
                        + "re-reads from the topic with whatever partition count the "
                        + "broker happens to have for it, so if the operator pre-"
                        + "provisioned a different count than upstream or the broker "
                        + "auto-created the topic with num.partitions (often 1), the "
                        + "downstream aggregate/reduce/join silently produces wrong "
                        + "results with no runtime check and no observable symptom other "
                        + "than aggregates that don't reconcile; (2) topic lifecycle is "
                        + "operator-owned and application-decoupled — when a topology is "
                        + "refactored to no longer use the intermediate, the topic is "
                        + "orphaned, consuming disk indefinitely, and the operator must "
                        + "remember to delete it manually; (3) no per-call serdes for "
                        + "the keyless overload — through(topic) wires serdes via the "
                        + "application-wide StreamsConfig defaults, so if the "
                        + "intermediate record schema differs from the default (e.g. "
                        + "the upstream operator changed the value type after a "
                        + "mapValues), the round-trip silently writes and reads with the "
                        + "wrong serdes; (4) INVOKEDYNAMIC `stream::through` captures "
                        + "silently bind to the deprecated method — the user-class "
                        + "bytecode contains zero direct INVOKEINTERFACE on the legacy "
                        + "method and a name-only walk misses it. Migrate to "
                        + "`stream.repartition()` (default config) or "
                        + "`stream.repartition(Repartitioned.<K, V>as(\"by-customer\")"
                        + ".withKeySerde(...).withValueSerde(...).withNumberOfPartitions"
                        + "(N))` (per-call serdes and partition count). repartition() "
                        + "provisions the internal topic with the matching partition "
                        + "count, wires serdes from the upstream node type, and tags it "
                        + "with the application ID so cleanup is transactional with "
                        + "kafka-streams-application-reset.");
    }
}
