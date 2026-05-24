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
 * Fires for every reach of
 * {@code Producer.sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata>, String groupId)}
 * — the deprecated String-{@code groupId} overload — whether as a direct call
 * or as an {@code INVOKEDYNAMIC} method-reference capture.
 *
 * <h2>Why we discriminate by descriptor, not by name</h2>
 *
 * <p>{@code sendOffsetsToTransaction} on {@code Producer} / {@code KafkaProducer}
 * has TWO overloads:
 * <ul>
 *   <li>{@code sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata>, String groupId)}
 *       — descriptor {@code (Ljava/util/Map;Ljava/lang/String;)V}, deprecated since
 *       Kafka 3.0 (KIP-447).</li>
 *   <li>{@code sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata>, ConsumerGroupMetadata)}
 *       — descriptor
 *       {@code (Ljava/util/Map;Lorg/apache/kafka/clients/consumer/ConsumerGroupMetadata;)V},
 *       the supported replacement.</li>
 * </ul>
 * A name-only match would catch both; we discriminate on the exact descriptor
 * being {@code (Ljava/util/Map;Ljava/lang/String;)V} to fire only on the
 * deprecated form.
 *
 * <h2>Why the replacement matters: zombie fencing across rebalances</h2>
 *
 * <p>The classic EOS (Exactly-Once Semantics) read-process-write loop uses a
 * transactional producer that carries the consumer's committed offset
 * alongside the produced records, atomically. {@code sendOffsetsToTransaction}
 * is the bridge: it appends an offset commit to the producer's in-flight
 * transaction so that &laquo;records produced&raquo; and
 * &laquo;input offset advanced&raquo; either both happen or neither does.
 *
 * <p>The String-{@code groupId} form takes only the consumer-group name and
 * trusts the producer to be the legitimate owner of that group's processing.
 * After KIP-447 (Kafka 2.5 broker, 3.0 client), brokers learned to fence
 * producer commits against the consumer's CURRENT generation and member ID:
 * if a consumer was rebalanced away from a partition while a stale producer
 * was still in the middle of a transaction, the broker refuses the offset
 * commit and the stale producer's transaction fails. This closes the
 * &laquo;zombie producer&raquo; data-corruption window where a network-partitioned
 * instance could overwrite the committed offsets of the consumer that now
 * owns those partitions, breaking EOS guarantees.
 *
 * <p>The {@code (Map, ConsumerGroupMetadata)} overload carries the full
 * generation/member fencing token retrieved from
 * {@code consumer.groupMetadata()}; the {@code (Map, String)} overload
 * supplies only the group name and is therefore unfenced. The deprecation
 * is not a stylistic preference — it is a correctness gap.
 *
 * <h2>Operational impact</h2>
 *
 * <p>EOS pipelines built on the deprecated overload look correct under
 * steady-state load and silently double-process input records during
 * rebalances: a partition fails over, the new owner starts processing
 * from the prior committed offset, the old owner — still in transaction —
 * commits a more-advanced offset on top, the new owner re-reads anyway
 * because by then the old commit was written too late, and downstream
 * sees the same input record materialised into two output records.
 * Downstream idempotence is then load-bearing for a correctness property
 * EOS was meant to provide.
 *
 * <h2>Method-reference capture</h2>
 *
 * <p>{@code producer::sendOffsetsToTransaction} compiles to an
 * {@code INVOKEDYNAMIC} whose bsm-args include a {@code REF_invokeInterface}
 * (or {@code REF_invokeVirtual} for {@code KafkaProducer}) handle whose
 * descriptor is fixed at capture time by Java's overload resolution against
 * the functional-interface SAM. Capturing into a
 * {@code BiConsumer<Map, String>} picks the deprecated overload; capturing
 * into a {@code BiConsumer<Map, ConsumerGroupMetadata>} picks the supported
 * one. The rule's {@code INVOKEDYNAMIC} walk inspects the resolved handle's
 * descriptor and only flags the {@code (Map, String)} captures.
 */
public final class ProducerSendOffsetsToTxnGroupIdDeprecatedRule implements Rule {

    private static final String SEND_OFFSETS = "sendOffsetsToTransaction";
    private static final String DEPRECATED_DESC = "(Ljava/util/Map;Ljava/lang/String;)V";

    private final Severity severity;

    public ProducerSendOffsetsToTxnGroupIdDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_SEND_OFFSETS_TO_TXN_GROUP_ID_DEPRECATED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && KafkaTypes.PRODUCER_OWNERS.contains(mi.owner)
                        && SEND_OFFSETS.equals(mi.name)
                        && DEPRECATED_DESC.equals(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, KafkaTypes.PRODUCER_OWNERS, SEND_OFFSETS, null);
                    if (h != null && DEPRECATED_DESC.equals(h.getDesc())) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.PRODUCER_SEND_OFFSETS_TO_TXN_GROUP_ID_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Producer.sendOffsetsToTransaction(Map, String groupId) is reached here — "
                        + "either as a direct call or as a method-reference capture (e.g. "
                        + "`producer::sendOffsetsToTransaction` assigned to a "
                        + "BiConsumer<Map, String>) whose deferred invocation has the same "
                        + "deprecated semantics. This overload is deprecated since Kafka 3.0 "
                        + "(KIP-447) because it bypasses consumer-group generation/member "
                        + "fencing: the broker accepts the offset commit on the basis of the "
                        + "group NAME alone, with no proof that this producer is the "
                        + "legitimate owner of those partitions in the current generation. A "
                        + "network-partitioned 'zombie' producer that still holds an open "
                        + "transaction can therefore overwrite the committed offsets of the "
                        + "consumer that now owns those partitions after a rebalance, breaking "
                        + "EOS (Exactly-Once Semantics) guarantees and silently double-"
                        + "processing input records. The replacement "
                        + "sendOffsetsToTransaction(Map, ConsumerGroupMetadata) — supplied "
                        + "from consumer.groupMetadata() — carries the full "
                        + "generation/member-ID fencing token so the broker can reject a "
                        + "stale producer's commit. The deprecation is not a stylistic "
                        + "preference; it is a correctness gap. Equivalent safe call: "
                        + "producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata()).");
    }
}
