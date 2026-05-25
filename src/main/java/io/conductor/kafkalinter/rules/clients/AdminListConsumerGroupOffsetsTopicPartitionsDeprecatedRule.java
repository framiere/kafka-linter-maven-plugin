package io.conductor.kafkalinter.rules.clients;

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
 * {@link org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions#topicPartitions(java.util.List)}
 * setter and its sibling
 * {@link org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions#topicPartitions()}
 * getter — whether the call lands directly via {@code INVOKEVIRTUAL}
 * or indirectly through an {@code INVOKEDYNAMIC} method-reference
 * capture (e.g. {@code options::topicPartitions} bound to a
 * config-builder SAM, or {@code ListConsumerGroupOffsetsOptions::topicPartitions}
 * bound to a Function-style API).
 *
 * <h2>Why these methods are deprecated</h2>
 *
 * <p>KIP-709 (Kafka 3.3, October 2022) split the
 * {@code Admin.listConsumerGroupOffsets} API into a batched form that
 * accepts a
 * {@code Map<String, ListConsumerGroupOffsetsSpec>} where each
 * {@code ListConsumerGroupOffsetsSpec} carries its own per-group
 * topic-partition filter. The new batched overload silently IGNORES
 * the per-{@code ListConsumerGroupOffsetsOptions} TP filter — the
 * filter set via {@code options.topicPartitions(tps)} on the Options
 * object is never read by the batched code path; only the Spec's
 * per-entry filter matters. Five concrete incident classes the
 * deprecation tracks:
 *
 * <ul>
 *   <li><b>Silent over-fetch.</b> A caller migrates from the legacy
 *       single-group form
 *       {@code admin.listConsumerGroupOffsets(groupId, options)} to the
 *       batched form
 *       {@code admin.listConsumerGroupOffsets(groupSpecs)} but keeps
 *       calling {@code options.topicPartitions(tps)} as a
 *       defensive-coding habit. The batched call returns offsets for
 *       ALL partitions assigned to the group — not just the filter set
 *       — because the Options TP filter is silently dropped. The
 *       caller's downstream code (typically a per-partition lag
 *       computation) then iterates over partitions it did not request
 *       and either crashes on a missing entry in a partition-to-lag
 *       map or, worse, double-counts lag for partitions outside the
 *       intended scope.</li>
 *   <li><b>N × broker-RTT sequential fan-out before KIP-709.</b> The
 *       legacy single-group API serialised one OFFSET_FETCH RPC per
 *       group to the group's coordinator. Listing offsets for N groups
 *       took N × RTT in the worst case. The batched form fans out the
 *       N RPCs to all coordinators in parallel — typically reducing
 *       wall-clock time from "N seconds" to "one round-trip" for a
 *       cluster monitoring 100+ groups (think Kafka Streams app
 *       fleet-wide rebalance-lag dashboards).</li>
 *   <li><b>Getter is part of the same deprecation.</b>
 *       {@code topicPartitions()} (the zero-arg getter) is also
 *       deprecated because reading the per-Options filter is
 *       meaningless once the call path is the batched form: the read
 *       value reflects whatever the caller previously stored, but the
 *       batched API ignores it on dispatch. Callers that introspect
 *       the filter for logging or validation are reading a value with
 *       no operational effect.</li>
 *   <li><b>INVOKEDYNAMIC capture path.</b> A code base that uses
 *       Options configuration via a fluent-builder DSL (e.g. a
 *       generic {@code Consumer<ListConsumerGroupOffsetsOptions>}
 *       parameter) can capture the setter as
 *       {@code options::topicPartitions} bound to that Consumer SAM.
 *       The user-class bytecode contains zero direct
 *       {@code INVOKEVIRTUAL} on the legacy method — only the
 *       {@code INVOKEDYNAMIC} + {@code LambdaMetafactory} bridge whose
 *       bsm-args contain a {@code REF_invokeVirtual} handle pointing
 *       at the legacy method. A name-only MethodInsnNode walk misses
 *       this case entirely.</li>
 *   <li><b>Trivial migration with parallel RPC payoff.</b>
 *       {@code admin.listConsumerGroupOffsets(Map.of(groupId, new ListConsumerGroupOffsetsSpec().topicPartitions(tps)))}
 *       replaces the legacy form one-for-one and immediately benefits
 *       from the parallel coordinator fan-out. The rule fires to
 *       surface the migration target.</li>
 * </ul>
 *
 * <h2>Descriptor discrimination — getter and setter share a name</h2>
 *
 * <p>The owner has two distinct methods named
 * {@code topicPartitions}:
 *
 * <ul>
 *   <li>Setter:
 *       {@code (Ljava/util/List;)Lorg/apache/kafka/clients/admin/ListConsumerGroupOffsetsOptions;}
 *       — accepts a {@code List<TopicPartition>} and returns the
 *       Options instance for fluent chaining.</li>
 *   <li>Getter:
 *       {@code ()Ljava/util/List;} — zero-arg, returns
 *       {@code List<TopicPartition>}.</li>
 * </ul>
 *
 * <p>Both are deprecated by KIP-709. The rule iterates over both
 * descriptors at each candidate call site.
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code options::topicPartitions} bound to a SAM that takes
 * {@code List<TopicPartition>} compiles to {@code INVOKEDYNAMIC}
 * whose bsm-args contain a {@code REF_invokeVirtual} handle pointing
 * at the resolved setter. {@code ListConsumerGroupOffsetsOptions::topicPartitions}
 * bound to a {@code Function<ListConsumerGroupOffsetsOptions, List>}
 * captures the getter. The rule's bsm-arg walk catches both cases by
 * checking the handle's {@code (owner, name, desc)} triple against
 * the same filter used for direct calls, iterated over both legacy
 * descriptors.
 */
public final class AdminListConsumerGroupOffsetsTopicPartitionsDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.ADMIN_LIST_CONSUMER_GROUP_OFFSETS_OPTIONS);
    private static final String METHOD_NAME = "topicPartitions";
    private static final Set<String> LEGACY_DESCS = Set.of(
            "(Ljava/util/List;)Lorg/apache/kafka/clients/admin/ListConsumerGroupOffsetsOptions;",
            "()Ljava/util/List;");

    private final Severity severity;

    public AdminListConsumerGroupOffsetsTopicPartitionsDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_LIST_CONSUMER_GROUP_OFFSETS_TOPIC_PARTITIONS_DEPRECATED;
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
                RuleId.ADMIN_LIST_CONSUMER_GROUP_OFFSETS_TOPIC_PARTITIONS_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "ListConsumerGroupOffsetsOptions.topicPartitions(List<TopicPartition>) "
                        + "or its zero-arg getter ListConsumerGroupOffsetsOptions.topicPartitions() "
                        + "is reached here — either as a direct call or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`options::topicPartitions` bound to a fluent-builder Consumer SAM "
                        + "or `ListConsumerGroupOffsetsOptions::topicPartitions` bound to a "
                        + "Function returning List<TopicPartition>). These methods are "
                        + "deprecated since Kafka 3.3 (KIP-709, October 2022) because the "
                        + "per-Options topic-partition filter is silently IGNORED by the new "
                        + "batched Admin.listConsumerGroupOffsets(Map<String, "
                        + "ListConsumerGroupOffsetsSpec>) overload — the batched dispatch "
                        + "reads only the per-Spec filter inside the Map, never the Options "
                        + "filter. Five concrete failure modes follow: (1) silent over-fetch "
                        + "— a caller migrating from the legacy single-group form to the "
                        + "batched form but keeping `options.topicPartitions(tps)` as a "
                        + "defensive-coding habit gets offsets for ALL partitions assigned to "
                        + "the group, not the requested subset, because the Options filter is "
                        + "dropped on dispatch; downstream per-partition-lag code then either "
                        + "crashes on a missing partition-to-lag map entry or double-counts "
                        + "lag for partitions outside the intended scope; (2) N × broker-RTT "
                        + "sequential fan-out before KIP-709 — the legacy single-group API "
                        + "serialised one OFFSET_FETCH RPC per group to the group's "
                        + "coordinator, so listing offsets for N groups took N × RTT in the "
                        + "worst case; the batched form fans out the N RPCs to all "
                        + "coordinators in parallel, typically collapsing wall-clock time "
                        + "from N seconds to one round-trip for cluster-monitoring callers "
                        + "(think Kafka Streams fleet rebalance-lag dashboards); (3) "
                        + "zero-arg getter is part of the same deprecation — reading the "
                        + "per-Options filter is meaningless once the call path is the "
                        + "batched form because the batched API ignores the value on "
                        + "dispatch; callers that introspect the filter for logging or "
                        + "validation read a value with no operational effect; (4) "
                        + "INVOKEDYNAMIC capture — a fluent-builder DSL "
                        + "(`Consumer<ListConsumerGroupOffsetsOptions>` parameter) capturing "
                        + "the setter as `options::topicPartitions` emits zero direct "
                        + "INVOKEVIRTUAL on the legacy method and a name-only walk misses it; "
                        + "the bsm-args of the indy site contain a REF_invokeVirtual handle "
                        + "pointing at the legacy method; (5) trivial migration with parallel "
                        + "RPC payoff. Migrate to "
                        + "`admin.listConsumerGroupOffsets(Map.of(groupId, "
                        + "new ListConsumerGroupOffsetsSpec().topicPartitions(tps)))`. The "
                        + "per-Spec filter is honoured on dispatch and the batched form fans "
                        + "out coordinator RPCs in parallel.");
    }
}
