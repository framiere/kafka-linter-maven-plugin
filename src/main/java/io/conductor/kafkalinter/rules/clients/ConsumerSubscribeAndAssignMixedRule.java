package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.VarInsnNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Flags {@code KafkaConsumer.subscribe()} and {@code KafkaConsumer.assign()}
 * called on the same local slot in the same method without an intervening
 * {@code unsubscribe()}. The two APIs are documented as mutually exclusive —
 * the second-mode call throws {@code IllegalStateException("Subscription to
 * topics, partitions and pattern are mutually exclusive")} because the
 * consumer's internal {@code SubscriptionState} is already in the opposite
 * mode (AUTO_TOPICS / AUTO_PATTERN vs USER_ASSIGNED).
 *
 * <p>Method-scoped detection on the same slot-tracking infrastructure as
 * {@link StreamsStartedTwiceRule} and {@link ConsumerUsedAfterCloseRule}:
 * track {@code new KafkaConsumer + ASTORE}, walk instructions, maintain a
 * per-method {@code subscribeSlots} / {@code assignSlots} pair. A call of the
 * opposite mode on a slot already in the other set fires.
 * {@code unsubscribe()} resets the slot back to NONE (removed from both sets).
 * Method-reference captures ({@code executor.submit(consumer::subscribe)},
 * {@code consumer::assign}) are also tracked.
 */
public final class ConsumerSubscribeAndAssignMixedRule implements Rule {

    private final Severity severity;

    public ConsumerSubscribeAndAssignMixedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_SUBSCRIBE_AND_ASSIGN_MIXED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            scanMethod(out, ctx, mn);
        }
        return out;
    }

    private void scanMethod(List<Violation> out, RuleContext ctx, MethodNode mn) {
        Set<Integer> consumerSlots = new HashSet<>();
        Set<Integer> subscribeSlots = new HashSet<>();
        Set<Integer> assignSlots = new HashSet<>();

        for (AbstractInsnNode insn : mn.instructions) {
            // (1) Track KafkaConsumer constructions: new + <init> + astore N
            if (insn instanceof MethodInsnNode ctor
                    && ctor.getOpcode() == Opcodes.INVOKESPECIAL
                    && "<init>".equals(ctor.name)
                    && KafkaTypes.KAFKA_CONSUMER.equals(ctor.owner)) {
                AbstractInsnNode next = AsmUtil.nextSignificant(ctor);
                if (next instanceof VarInsnNode v && v.getOpcode() == Opcodes.ASTORE) {
                    consumerSlots.add(v.var);
                }
                continue;
            }

            // (2) Method-reference captures: indy bsmArgs containing a
            //     REF_invoke* handle for subscribe/assign/unsubscribe.
            if (insn instanceof InvokeDynamicInsnNode indy) {
                Handle subH = AsmUtil.indyTargetHandle(indy, KafkaTypes.CONSUMER_OWNERS, "subscribe", null);
                if (subH != null) {
                    Set<Integer> captured = AsmUtil.indyCapturedSlots(indy, consumerSlots);
                    if (handleCapture(out, ctx, mn, insn, captured, assignSlots, subscribeSlots,
                            "subscribe", "assign")) {
                        continue;
                    }
                }
                Handle asgH = AsmUtil.indyTargetHandle(indy, KafkaTypes.CONSUMER_OWNERS, "assign", null);
                if (asgH != null) {
                    Set<Integer> captured = AsmUtil.indyCapturedSlots(indy, consumerSlots);
                    if (handleCapture(out, ctx, mn, insn, captured, subscribeSlots, assignSlots,
                            "assign", "subscribe")) {
                        continue;
                    }
                }
                Handle unsubH = AsmUtil.indyTargetHandle(indy, KafkaTypes.CONSUMER_OWNERS, "unsubscribe", null);
                if (unsubH != null) {
                    Set<Integer> captured = AsmUtil.indyCapturedSlots(indy, consumerSlots);
                    for (Integer slot : captured) {
                        subscribeSlots.remove(slot);
                        assignSlots.remove(slot);
                    }
                }
                continue;
            }

            if (!(insn instanceof MethodInsnNode mi)) continue;
            if (!KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) continue;

            String name = mi.name;
            boolean isSubscribe = "subscribe".equals(name);
            boolean isAssign = "assign".equals(name);
            boolean isUnsubscribe = "unsubscribe".equals(name);
            if (!isSubscribe && !isAssign && !isUnsubscribe) continue;

            Integer slot = AsmUtil.resolveReceiverSlot(mi, consumerSlots);
            if (slot == null) continue;

            if (isUnsubscribe) {
                subscribeSlots.remove(slot);
                assignSlots.remove(slot);
                continue;
            }

            if (isSubscribe) {
                if (assignSlots.contains(slot)) {
                    out.add(new Violation(
                            RuleId.CONSUMER_SUBSCRIBE_AND_ASSIGN_MIXED, severity,
                            ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                            "subscribe() called on a KafkaConsumer that was already put into "
                                    + "manually-assigned mode by assign() earlier in this method. "
                                    + "KafkaConsumer.subscribe() and KafkaConsumer.assign() are "
                                    + "mutually exclusive — the consumer's SubscriptionState is "
                                    + "already in USER_ASSIGNED mode, so this subscribe() throws "
                                    + "IllegalStateException(\"Subscription to topics, partitions "
                                    + "and pattern are mutually exclusive\"). Pick one mode "
                                    + "(subscribe for group-managed deployments, assign for "
                                    + "manual partition control) and remove the other call. If "
                                    + "you genuinely need to switch modes mid-lifecycle, call "
                                    + "unsubscribe() first to reset the state to NONE."));
                } else {
                    subscribeSlots.add(slot);
                }
            } else { // isAssign
                if (subscribeSlots.contains(slot)) {
                    out.add(new Violation(
                            RuleId.CONSUMER_SUBSCRIBE_AND_ASSIGN_MIXED, severity,
                            ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                            "assign() called on a KafkaConsumer that was already put into "
                                    + "group-managed mode by subscribe() earlier in this method. "
                                    + "KafkaConsumer.subscribe() and KafkaConsumer.assign() are "
                                    + "mutually exclusive — the consumer's SubscriptionState is "
                                    + "already in AUTO_TOPICS / AUTO_PATTERN mode, so this "
                                    + "assign() throws IllegalStateException(\"Subscription to "
                                    + "topics, partitions and pattern are mutually exclusive\"). "
                                    + "Pick one mode (subscribe for group-managed deployments, "
                                    + "assign for manual partition control) and remove the other "
                                    + "call. If you genuinely need to switch modes mid-lifecycle, "
                                    + "call unsubscribe() first to reset the state to NONE."));
                } else {
                    assignSlots.add(slot);
                }
            }
        }
    }

    /**
     * Handle a method-reference capture of either {@code subscribe} or
     * {@code assign}. If the captured slot is in the OPPOSITE set, fire and
     * return true (caller should continue). Otherwise add to OWN set and
     * return true. Returns true unconditionally because an indy targeting
     * subscribe/assign was found.
     */
    private boolean handleCapture(List<Violation> out, RuleContext ctx, MethodNode mn,
                                  AbstractInsnNode insn, Set<Integer> captured,
                                  Set<Integer> oppositeSet, Set<Integer> ownSet,
                                  String thisName, String oppositeName) {
        for (Integer slot : captured) {
            if (oppositeSet.contains(slot)) {
                out.add(new Violation(
                        RuleId.CONSUMER_SUBSCRIBE_AND_ASSIGN_MIXED, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        thisName + "() reference captured on a KafkaConsumer that was already "
                                + "put into the opposite mode by " + oppositeName + "() earlier in "
                                + "this method. KafkaConsumer.subscribe() and KafkaConsumer.assign() "
                                + "are mutually exclusive — when the captured functional interface "
                                + "runs (on the executor / async thread), the call throws "
                                + "IllegalStateException(\"Subscription to topics, partitions and "
                                + "pattern are mutually exclusive\"). The executor's "
                                + "uncaught-exception handler typically swallows the exception, "
                                + "leaving the consumer in whatever mode the first call set. Pick "
                                + "ONE mode and remove the other; call unsubscribe() first if you "
                                + "must switch modes mid-lifecycle."));
            } else {
                ownSet.add(slot);
            }
        }
        return true;
    }
}
