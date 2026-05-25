package io.conductor.kafkalinter.rules.clients;

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
 * Fires for every reach of the no-listener
 * {@link org.apache.kafka.clients.consumer.Consumer#subscribe} overloads
 * — descriptors {@code (Ljava/util/Collection;)V} and
 * {@code (Ljava/util/regex/Pattern;)V} — neither of which carries a
 * {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener}.
 * Catches both direct {@code INVOKEVIRTUAL}/{@code INVOKEINTERFACE}
 * calls and indirect {@code INVOKEDYNAMIC} method-reference captures
 * (e.g. {@code consumer::subscribe} bound to a custom SAM or to
 * {@link java.util.function.Consumer Consumer&lt;Collection&lt;String&gt;&gt;}
 * — the indy bsm-args contain a {@code REF_invokeVirtual} or
 * {@code REF_invokeInterface} handle pointing at one of the two no-
 * listener descriptors).
 *
 * <h2>Why no-listener subscribe is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer#subscribe(java.util.Collection)}
 * and {@code subscribe(Pattern)} are the most common entry points to
 * the Kafka consumer group protocol — and the most commonly
 * misconfigured. The group protocol guarantees at-most-one consumer
 * per partition by triggering a rebalance whenever group membership
 * changes (new instance joins, instance leaves, partition added to a
 * topic, regex matches a new topic). During a rebalance, partitions
 * are revoked from one consumer instance and assigned to another. The
 * {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener}
 * callback is the ONLY hook the application has between the
 * "partitions about to be revoked" signal and the "partitions
 * actually revoked" event — meaning it is the only place to flush
 * in-memory state, commit final offsets, release per-partition
 * resources (database connections, file handles, downstream
 * batching), or hand off in-flight work to the next owner.
 *
 * <p>Without a listener:
 *
 * <ul>
 *   <li><b>In-flight records are silently dropped on revoke.</b>
 *       Pattern: the consumer accumulates records in an in-memory
 *       buffer (batching writes to a downstream DB, building HTTP
 *       payloads, deduplicating a window). On rebalance the partition
 *       is reassigned mid-batch; the buffer is discarded; the new
 *       owner starts from the last committed offset which is the
 *       offset BEFORE the dropped batch. Net effect: the records in
 *       the dropped batch are replayed and re-processed.</li>
 *   <li><b>Final commitSync before revoke is skipped.</b> Pattern:
 *       the consumer commits at the end of every batch. On rebalance,
 *       processing of the current batch is interrupted; the commit
 *       for the last fully-processed offset never happens; the new
 *       owner picks up from the previous commit and re-processes the
 *       interrupted batch. Idempotent downstream writes mask the
 *       duplication; non-idempotent writes (charges, emails, payouts)
 *       produce visible double-effects.</li>
 *   <li><b>Per-partition resources leak.</b> Pattern: a consumer
 *       opens a per-partition file handle, JDBC connection, or
 *       downstream batching context indexed by partition. On revoke
 *       the partition is reassigned to a different instance, but the
 *       previous owner's resource is never closed because there is no
 *       callback firing on the previous owner. Over a sequence of
 *       rebalances (rolling deploy, autoscaling), file handles or
 *       connections leak until {@code EMFILE} or pool exhaustion.</li>
 *   <li><b>Per-partition consumer state desync.</b> Pattern: a
 *       consumer maintains per-partition stateful state (a sliding
 *       window count, a Bloom filter, a session tracker). On revoke
 *       this state must be persisted (to a sidecar store, to disk, to
 *       a state topic) or it is lost; on assign the new partition's
 *       state must be loaded. Without onPartitionsRevoked /
 *       onPartitionsAssigned, the state simply vanishes on every
 *       rebalance; the consumer behaves correctly between rebalances
 *       and degrades silently around them.</li>
 *   <li><b>{@code subscribe(Pattern)} regex auto-discovery
 *       compounds the hazard.</b> The pattern variant resubscribes
 *       automatically as new topics matching the regex are created
 *       and triggers a rebalance every time. Without a listener,
 *       every such auto-discovery event silently drops in-flight
 *       work on every existing partition.</li>
 *   <li><b>INVOKEDYNAMIC {@code consumer::subscribe} captures bypass
 *       naïve MethodInsnNode-only lint.</b> A
 *       {@link java.util.function.Consumer Consumer&lt;Collection&lt;String&gt;&gt;}
 *       parameter bound by a {@code consumer::subscribe} method
 *       reference compiles to {@code INVOKEDYNAMIC} whose bsm-args
 *       contain a {@code REF_invokeVirtual} or
 *       {@code REF_invokeInterface} handle pointing at one of the
 *       no-listener descriptors. The user-class bytecode contains
 *       zero direct {@code INVOKEVIRTUAL} on those overloads — only
 *       the indy site. A rule that walks only {@code MethodInsnNode}
 *       misses every such site.</li>
 * </ul>
 *
 * <h2>Descriptor discrimination — six overloads, two are unsafe</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer} declares
 * three subscribe overload pairs (without and with listener):
 *
 * <ul>
 *   <li>Unsafe: {@code (Ljava/util/Collection;)V} —
 *       subscribe(Collection)</li>
 *   <li>Unsafe: {@code (Ljava/util/regex/Pattern;)V} —
 *       subscribe(Pattern)</li>
 *   <li>Safe:
 *       {@code (Ljava/util/Collection;Lorg/apache/kafka/clients/consumer/ConsumerRebalanceListener;)V}
 *       — subscribe(Collection, ConsumerRebalanceListener)</li>
 *   <li>Safe:
 *       {@code (Ljava/util/regex/Pattern;Lorg/apache/kafka/clients/consumer/ConsumerRebalanceListener;)V}
 *       — subscribe(Pattern, ConsumerRebalanceListener)</li>
 *   <li>Safe (subscribe to SubscriptionPattern, KIP-848):
 *       {@code (Lorg/apache/kafka/clients/consumer/SubscriptionPattern;Lorg/apache/kafka/clients/consumer/ConsumerRebalanceListener;)V}</li>
 *   <li>Safe (no-listener but new RE2J pattern, KIP-848):
 *       {@code (Lorg/apache/kafka/clients/consumer/SubscriptionPattern;)V}
 *       — same hazard but a separate evolution; not in scope here.</li>
 * </ul>
 *
 * <p>Predicate is exact-match against either unsafe descriptor; the
 * three callback-carrying overloads have strictly different signatures
 * and are never flagged.
 */
public final class ConsumerSubscribeWithoutRebalanceListenerRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.CONSUMER_OWNERS;
    private static final String METHOD_NAME = "subscribe";
    private static final String COLLECTION_DESC = "(Ljava/util/Collection;)V";
    private static final String PATTERN_DESC = "(Ljava/util/regex/Pattern;)V";

    private final Severity severity;

    public ConsumerSubscribeWithoutRebalanceListenerRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_SUBSCRIBE_WITHOUT_REBALANCE_LISTENER;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && (COLLECTION_DESC.equals(mi.desc) || PATTERN_DESC.equals(mi.desc))) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy
                        && (AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, COLLECTION_DESC) != null
                            || AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, PATTERN_DESC) != null)) {
                    out.add(violation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.CONSUMER_SUBSCRIBE_WITHOUT_REBALANCE_LISTENER, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Consumer.subscribe(Collection) or Consumer.subscribe("
                        + "Pattern) (no ConsumerRebalanceListener) is "
                        + "reached here — either as a direct "
                        + "INVOKEVIRTUAL/INVOKEINTERFACE call or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`consumer::subscribe` bound to a "
                        + "Consumer<Collection<String>> or a custom SAM). "
                        + "The ConsumerRebalanceListener callback is the "
                        + "ONLY hook the application has between the "
                        + "'partitions about to be revoked' signal and the "
                        + "'partitions actually revoked' event — meaning "
                        + "it is the only place to flush in-memory state, "
                        + "commit final offsets, release per-partition "
                        + "resources (database connections, file handles, "
                        + "downstream batching), or hand off in-flight "
                        + "work to the next owner. Without a listener: "
                        + "(1) in-flight records are silently dropped on "
                        + "revoke — consumer accumulates records in an "
                        + "in-memory buffer (batching writes to a "
                        + "downstream DB, building HTTP payloads, "
                        + "deduplicating a window); on rebalance the "
                        + "partition is reassigned mid-batch, the buffer "
                        + "is discarded, the new owner starts from the "
                        + "last committed offset which is the offset "
                        + "BEFORE the dropped batch — the records in the "
                        + "dropped batch are replayed and re-processed; "
                        + "(2) final commitSync before revoke is "
                        + "skipped — consumer commits at the end of every "
                        + "batch; on rebalance processing of the current "
                        + "batch is interrupted; the commit for the last "
                        + "fully-processed offset never happens; the new "
                        + "owner picks up from the previous commit and "
                        + "re-processes the interrupted batch; idempotent "
                        + "downstream writes mask the duplication, "
                        + "non-idempotent writes (charges, emails, "
                        + "payouts) produce visible double-effects; (3) "
                        + "per-partition resources leak — consumer opens "
                        + "a per-partition file handle, JDBC connection, "
                        + "or downstream batching context indexed by "
                        + "partition; on revoke the partition is "
                        + "reassigned to a different instance but the "
                        + "previous owner's resource is never closed "
                        + "because there is no callback firing on the "
                        + "previous owner; over a sequence of rebalances "
                        + "(rolling deploy, autoscaling), file handles or "
                        + "connections leak until EMFILE or pool "
                        + "exhaustion; (4) per-partition consumer state "
                        + "desync — consumer maintains per-partition "
                        + "stateful state (sliding window count, Bloom "
                        + "filter, session tracker); on revoke this state "
                        + "must be persisted (to a sidecar store, to "
                        + "disk, to a state topic) or it is lost; on "
                        + "assign the new partition's state must be "
                        + "loaded; without onPartitionsRevoked / "
                        + "onPartitionsAssigned, the state simply "
                        + "vanishes on every rebalance, the consumer "
                        + "behaves correctly between rebalances and "
                        + "degrades silently around them; (5) "
                        + "subscribe(Pattern) regex auto-discovery "
                        + "compounds the hazard — the pattern variant "
                        + "resubscribes automatically as new topics "
                        + "matching the regex are created and triggers a "
                        + "rebalance every time; without a listener, "
                        + "every such auto-discovery event silently drops "
                        + "in-flight work on every existing partition; "
                        + "(6) INVOKEDYNAMIC `consumer::subscribe` "
                        + "captures bypass naive MethodInsnNode-only "
                        + "lint — Consumer<Collection<String>> SAM `void "
                        + "accept(Object)` erases the parameter type, but "
                        + "the indy implMethod handle still carries the "
                        + "precise descriptor `(Ljava/util/Collection;)V` "
                        + "and the rule matches against that. Migration: "
                        + "use the bounded overload "
                        + "subscribe(Collection, ConsumerRebalanceListener) "
                        + "or subscribe(Pattern, ConsumerRebalanceListener) "
                        + "with a listener that implements (a) "
                        + "onPartitionsRevoked: flush in-memory buffers, "
                        + "commitSync(offsetsToCommit) the last fully-"
                        + "processed offset per partition, release "
                        + "per-partition resources; (b) "
                        + "onPartitionsAssigned: load per-partition state "
                        + "from the sidecar / state topic, open "
                        + "per-partition resources, seek to the correct "
                        + "starting offset; (c) onPartitionsLost (KIP-429, "
                        + "Kafka 2.4+): cleanup variant invoked when the "
                        + "consumer cooperatively-or-otherwise loses the "
                        + "partition without a clean revoke (session "
                        + "timeout, member kicked from group). The "
                        + "callback-carrying overloads have descriptors "
                        + "`(Ljava/util/Collection;Lorg/apache/kafka/"
                        + "clients/consumer/ConsumerRebalanceListener;)V` "
                        + "and `(Ljava/util/regex/Pattern;Lorg/apache/"
                        + "kafka/clients/consumer/"
                        + "ConsumerRebalanceListener;)V` and are never "
                        + "flagged by this rule.");
    }
}
