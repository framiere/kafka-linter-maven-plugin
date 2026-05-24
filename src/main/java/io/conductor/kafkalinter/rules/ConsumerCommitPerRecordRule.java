package io.conductor.kafkalinter.rules;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.JumpInsnNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.LookupSwitchInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TableSwitchInsnNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Flags commitSync() calls that sit inside the per-record loop of a poll() — the
 * inner iteration over ConsumerRecords. Commit-per-batch (commitSync after the
 * inner loop, still inside the outer poll loop) is fine.
 *
 * <h2>Classical for-each shape (inner-loop back-edge)</h2>
 *
 * <p>For the explicit shape
 * {@code for (ConsumerRecord r : records) { ...; consumer.commitSync(); }} the
 * heuristic is: a commitSync is "per-record" iff it lives inside a back-edge
 * whose target label appears AFTER the first poll() call in the method. That
 * distinguishes the inner for-each-records loop (target after poll) from the
 * outer {@code while(true)} polling loop (target before poll).
 *
 * <h2>forEach-lambda shape (per-record blind spot the inner-loop scan misses)</h2>
 *
 * <p>The same anti-pattern can also be written as
 * {@code records.forEach(r -> consumer.commitSync())} (or any
 * {@code Stream.forEach / forEachOrdered / map / filter / peek / flatMap}
 * over the records). That compiles to:
 * <ul>
 *   <li>The USER method body — only an {@code INVOKEDYNAMIC} (a
 *       {@code LambdaMetafactory.metafactory} call site producing the
 *       {@code Consumer<ConsumerRecord>} instance) followed by an
 *       {@code INVOKEINTERFACE Iterable.forEach} (or {@code Stream.forEach}).
 *       <strong>The user method contains ZERO {@code INVOKE*} instructions
 *       targeting {@code commitSync}, and ZERO inner back-edges</strong>: the
 *       iteration happens inside the JDK collection / stream implementation,
 *       not in the user-class bytecode.</li>
 *   <li>A synthetic {@code lambda$N$M} sibling method whose body actually
 *       contains the {@code commitSync()} call. That synthetic has no
 *       {@code poll()} call, so the inner-loop back-edge scan above bails out
 *       on it (firstPollIndex == -1) — and never sees the commit.</li>
 * </ul>
 *
 * <p>A back-edge-only detector therefore concludes "no per-record commit" and
 * misses the anti-pattern entirely. The second pass below recovers it: it
 * walks every {@code INVOKEDYNAMIC} in the class, asks whether the impl
 * handle points at a sibling lambda whose body contains a
 * {@code consumer.commitSync()}, and whether the indy's
 * {@code instantiatedMethodType} ({@code bsmArgs[2]}) is a single-arg
 * {@code (Lorg/apache/kafka/clients/consumer/ConsumerRecord;)V} — i.e. the
 * lambda is invoked once per {@code ConsumerRecord}. The dispatching call is
 * required to be one of the iterating sinks tracked by
 * {@link io.conductor.kafkalinter.scanner.LambdaTracker} (i.e. the lambda is
 * passed directly to {@code forEach} / {@code stream.forEach} / etc.), which
 * is already precomputed by {@link RuleContext#isIteratingLambdaBody}. That
 * keeps the rule silent on captures that are stored in a field or passed to
 * non-iterating sinks (where per-record semantics aren't proven).
 *
 * <p>The per-record discriminator on the instantiated method type is what
 * prevents false positives on {@code records.partitions().forEach(p -> ...)}
 * (per-partition, instantiated type {@code (LTopicPartition;)V}) and on
 * sibling {@code Map.forEach((k,v) -> ...)} commits. Only a single-arg
 * {@code ConsumerRecord} lambda fires.
 */
public final class ConsumerCommitPerRecordRule implements Rule {

    private static final String COMMIT_SYNC = "commitSync";

    private final Severity severity;

    public ConsumerCommitPerRecordRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_COMMIT_PER_RECORD;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        Set<String> commitLambdas = perRecordCommitLambdaCandidates(ctx);

        for (MethodNode mn : ctx.classNode().methods) {
            AbstractInsnNode[] all = mn.instructions.toArray();
            int firstPollIdx = firstPollIndex(all);

            // Pass 1 — classical for-each: commitSync inside an inner back-edge after poll().
            if (firstPollIdx >= 0) {
                Map<LabelNode, Integer> labelIdx = indexLabels(all);
                List<int[]> innerLoops = innerLoopRanges(all, labelIdx, firstPollIdx);
                if (!innerLoops.isEmpty()) {
                    for (int i = 0; i < all.length; i++) {
                        AbstractInsnNode insn = all[i];
                        if (!(insn instanceof MethodInsnNode mi)) continue;
                        if (!KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) continue;
                        if (!COMMIT_SYNC.equals(mi.name)) continue;

                        for (int[] range : innerLoops) {
                            if (i >= range[0] && i <= range[1]) {
                                out.add(new Violation(
                                        RuleId.CONSUMER_COMMIT_PER_RECORD,
                                        severity,
                                        ctx.classNode().name,
                                        mn.name,
                                        AsmUtil.lineOf(insn),
                                        "commitSync() inside the per-record loop of a poll() — commit per batch, not per record."));
                                break;
                            }
                        }
                    }
                }
            }

            // Pass 2 — forEach-lambda: an INVOKEDYNAMIC handing a per-record ConsumerRecord
            // lambda whose body contains commitSync to an iterating sink (forEach / stream op).
            if (commitLambdas.isEmpty()) continue;
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof InvokeDynamicInsnNode indy)) continue;
                if (!isLambdaMetafactory(indy)) continue;
                Handle impl = AsmUtil.indyTargetHandle(indy, Set.of(ctx.classNode().name), null, null);
                if (impl == null) continue;
                if (!commitLambdas.contains(impl.getName())) continue;
                if (!isPerRecordInstantiated(indy)) continue;
                out.add(new Violation(
                        RuleId.CONSUMER_COMMIT_PER_RECORD,
                        severity,
                        ctx.classNode().name,
                        mn.name,
                        AsmUtil.lineOf(insn),
                        "commitSync() inside the body of a per-record forEach / stream lambda over "
                                + "ConsumerRecord — every record now pays a synchronous coordinator "
                                + "round-trip. The JDK-side iteration of `records.forEach(r -> ...)` "
                                + "(or `.stream().forEach(...)`, `.peek(...)`, `.map(...)`, etc.) invokes "
                                + "the lambda once per record, so a poll() returning 500 records becomes "
                                + "500 synchronous commits — coordinator-RTT-bound, saturating the "
                                + "__consumer_offsets topic and starving other consumer groups on the "
                                + "same coordinator. Iterate without committing inside the lambda and "
                                + "call commitSync() ONCE after the batch."));
            }
        }
        return out;
    }

    /**
     * Synthetic / sibling methods of this class that are bodies of "iterating" lambdas
     * (passed directly to {@code forEach} / {@code Stream.forEach} / etc., per
     * {@link RuleContext#isIteratingLambdaBody}) AND that contain at least one direct
     * {@code consumer.commitSync(...)} call. Their NAME is what {@code INVOKEDYNAMIC}
     * impl handles in the same class refer to.
     */
    private static Set<String> perRecordCommitLambdaCandidates(RuleContext ctx) {
        Set<String> out = new HashSet<>();
        for (MethodNode mn : ctx.classNode().methods) {
            if (!ctx.isIteratingLambdaBody(mn)) continue;
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)
                        && COMMIT_SYNC.equals(mi.name)) {
                    out.add(mn.name);
                    break;
                }
            }
        }
        return out;
    }

    private static boolean isLambdaMetafactory(InvokeDynamicInsnNode indy) {
        return indy.bsm != null
                && "java/lang/invoke/LambdaMetafactory".equals(indy.bsm.getOwner());
    }

    /**
     * True iff {@code indy} is a {@code LambdaMetafactory} call site whose
     * {@code instantiatedMethodType} ({@code bsmArgs[2]}) is exactly a single-arg
     * {@code (Lorg/apache/kafka/clients/consumer/ConsumerRecord;)V} — the shape
     * of a {@code Consumer<ConsumerRecord<K,V>>} captured for
     * {@code records.forEach(...)}.
     *
     * <p>This is the discriminator that keeps the rule silent on
     * {@code records.partitions().forEach(p -> commit)} (instantiated type
     * {@code (LTopicPartition;)V}) and other non-per-record iterations.
     */
    private static boolean isPerRecordInstantiated(InvokeDynamicInsnNode indy) {
        if (indy.bsmArgs == null || indy.bsmArgs.length < 3) return false;
        if (!(indy.bsmArgs[2] instanceof Type inst)) return false;
        Type[] args = Type.getArgumentTypes(inst.getDescriptor());
        if (args.length != 1) return false;
        return KafkaTypes.CONSUMER_RECORD.equals(args[0].getInternalName());
    }

    private static Map<LabelNode, Integer> indexLabels(AbstractInsnNode[] all) {
        Map<LabelNode, Integer> m = new IdentityHashMap<>();
        for (int i = 0; i < all.length; i++) {
            if (all[i] instanceof LabelNode ln) m.put(ln, i);
        }
        return m;
    }

    private static int firstPollIndex(AbstractInsnNode[] all) {
        for (int i = 0; i < all.length; i++) {
            if (all[i] instanceof MethodInsnNode mi
                    && KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)
                    && mi.name.equals("poll")) {
                return i;
            }
        }
        return -1;
    }

    private static List<int[]> innerLoopRanges(AbstractInsnNode[] all,
                                               Map<LabelNode, Integer> labelIdx,
                                               int firstPollIdx) {
        List<int[]> ranges = new ArrayList<>();
        for (int i = 0; i < all.length; i++) {
            AbstractInsnNode insn = all[i];
            if (insn instanceof JumpInsnNode j) {
                addInnerRange(ranges, labelIdx, i, j.label, firstPollIdx);
            } else if (insn instanceof TableSwitchInsnNode ts) {
                for (LabelNode l : ts.labels) addInnerRange(ranges, labelIdx, i, l, firstPollIdx);
            } else if (insn instanceof LookupSwitchInsnNode ls) {
                for (LabelNode l : ls.labels) addInnerRange(ranges, labelIdx, i, l, firstPollIdx);
            }
        }
        return ranges;
    }

    private static void addInnerRange(List<int[]> ranges,
                                      Map<LabelNode, Integer> labelIdx,
                                      int jumpIdx,
                                      LabelNode target,
                                      int firstPollIdx) {
        if (target == null) return;
        Integer t = labelIdx.get(target);
        if (t == null) return;
        if (t >= jumpIdx) return;      // not a back-edge
        if (t <= firstPollIdx) return; // outer loop, not the inner per-record loop
        ranges.add(new int[]{t, jumpIdx});
    }
}
