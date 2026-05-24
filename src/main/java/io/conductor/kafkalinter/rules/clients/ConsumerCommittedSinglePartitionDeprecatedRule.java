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

/**
 * Fires for every reach of {@code Consumer.committed(TopicPartition)} or
 * {@code Consumer.committed(TopicPartition, Duration)} — the deprecated
 * single-partition overloads — whether as a direct call or as an
 * {@code INVOKEDYNAMIC} method-reference capture (e.g. {@code consumer::committed}
 * stored into a {@code Function<TopicPartition, OffsetAndMetadata>}).
 *
 * <h2>Why we discriminate by descriptor, not by name</h2>
 *
 * <p>{@code committed} on {@code Consumer} / {@code KafkaConsumer} has FOUR
 * overloads:
 * <ul>
 *   <li>{@code committed(TopicPartition)} — descriptor prefix
 *       {@code (Lorg/apache/kafka/common/TopicPartition;)}, deprecated since
 *       Kafka 2.4 (KIP-520).</li>
 *   <li>{@code committed(TopicPartition, Duration)} — descriptor prefix
 *       {@code (Lorg/apache/kafka/common/TopicPartition;Ljava/time/Duration;)},
 *       also deprecated (KIP-520).</li>
 *   <li>{@code committed(Set<TopicPartition>)} — descriptor prefix
 *       {@code (Ljava/util/Set;)}, the supported batched replacement.</li>
 *   <li>{@code committed(Set<TopicPartition>, Duration)} — descriptor prefix
 *       {@code (Ljava/util/Set;Ljava/time/Duration;)}, the supported
 *       batched-with-timeout replacement.</li>
 * </ul>
 * A name-only match would catch all four; we discriminate on the FIRST
 * descriptor argument being {@code Lorg/apache/kafka/common/TopicPartition;}
 * to fire only on the two deprecated single-partition shapes.
 *
 * <h2>Why the batched replacement matters operationally</h2>
 *
 * <p>Each {@code committed(TopicPartition)} call costs ONE
 * {@code OFFSET_FETCH} request to the group coordinator broker — including
 * the request framing, the coordinator look-up of the group's offset state,
 * and the response unmarshalling. A loop that iterates an N-partition
 * assignment and calls the single-partition overload per partition pays
 * N &times; broker-RTT of strictly sequential round trips, with no
 * pipelining. For an assignment of 32 partitions and a 5 ms broker RTT
 * inside a datacenter that's a 160 ms latency floor per "where am I
 * across my partitions?" question; for a cross-region setup at 80 ms RTT
 * it's 2.5 seconds. {@code committed(Set<TopicPartition>)} bundles all
 * partitions into ONE {@code OFFSET_FETCH} request and returns a single
 * {@code Map<TopicPartition, OffsetAndMetadata>}, paying broker-RTT once.
 *
 * <h2>Where the loop typically lives</h2>
 *
 * <p>The pattern shows up most often in (1) IQ / web endpoints exposing
 * per-partition lag (where the latency floor manifests as a slow HTTP
 * endpoint), (2) lag-monitoring sidecars that scrape committed offsets
 * across an assignment on a poll interval, and (3) custom rebalance
 * listeners that fetch committed offsets for each newly-assigned
 * partition in {@code onPartitionsAssigned} — exactly the path that
 * needs to be fast so the rebalance doesn't blow past
 * {@code max.poll.interval.ms} and trigger another rebalance.
 *
 * <h2>Method-reference capture</h2>
 *
 * <p>{@code consumer::committed} compiles to an {@code INVOKEDYNAMIC} whose
 * bootstrap-method args include a {@code REF_invokeInterface} (or
 * {@code REF_invokeVirtual} for {@code KafkaConsumer}) handle to
 * {@code committed}. Java's overload resolution picks the captured
 * overload from the functional-interface SAM signature: assigning
 * {@code consumer::committed} to {@code Function<TopicPartition, OffsetAndMetadata>}
 * captures the deprecated {@code (TopicPartition)} overload, while
 * {@code Function<Set<TopicPartition>, Map<TopicPartition, OffsetAndMetadata>>}
 * captures the supported {@code (Set)} overload. The rule's
 * {@code INVOKEDYNAMIC} walk inspects the resolved handle's descriptor
 * and only flags the {@code (TopicPartition)} captures.
 */
public final class ConsumerCommittedSinglePartitionDeprecatedRule implements Rule {

    private static final String COMMITTED = "committed";
    private static final String DESC_PREFIX = "(Lorg/apache/kafka/common/TopicPartition;";

    private final Severity severity;

    public ConsumerCommittedSinglePartitionDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_COMMITTED_SINGLE_PARTITION_DEPRECATED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)
                        && COMMITTED.equals(mi.name)
                        && mi.desc != null
                        && mi.desc.startsWith(DESC_PREFIX)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, KafkaTypes.CONSUMER_OWNERS, COMMITTED, null);
                    if (h != null && h.getDesc() != null && h.getDesc().startsWith(DESC_PREFIX)) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.CONSUMER_COMMITTED_SINGLE_PARTITION_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Consumer.committed(TopicPartition) / committed(TopicPartition, Duration) is reached "
                        + "here — either as a direct call or as a method-reference capture (e.g. "
                        + "`consumer::committed` assigned to a Function<TopicPartition, "
                        + "OffsetAndMetadata>) whose deferred invocation has the same deprecated "
                        + "semantics. These single-partition overloads are deprecated since Kafka 2.4 "
                        + "(KIP-520). Each call costs ONE OFFSET_FETCH request to the group "
                        + "coordinator; iterating an N-partition assignment and calling the "
                        + "single-partition overload per partition pays N × broker-RTT of strictly "
                        + "sequential round trips with no pipelining. For 32 partitions at 5 ms "
                        + "in-DC RTT that's a 160 ms floor per 'where am I?' query; across regions "
                        + "at 80 ms RTT it's 2.5 seconds. The pattern shows up most often in IQ / "
                        + "web endpoints exposing per-partition lag, lag-monitoring sidecars that "
                        + "scrape on a poll interval, and rebalance listeners that fetch committed "
                        + "offsets in onPartitionsAssigned — where slow fetches risk blowing past "
                        + "max.poll.interval.ms and triggering another rebalance. Use the batched "
                        + "overload consumer.committed(Set<TopicPartition>) — or "
                        + "consumer.committed(Set<TopicPartition>, Duration) — which bundles all "
                        + "partitions into ONE OFFSET_FETCH request and returns a "
                        + "Map<TopicPartition, OffsetAndMetadata> in a single round trip. Equivalent "
                        + "safe call: consumer.committed(Set.of(tp1, tp2, tp3)).");
    }
}
