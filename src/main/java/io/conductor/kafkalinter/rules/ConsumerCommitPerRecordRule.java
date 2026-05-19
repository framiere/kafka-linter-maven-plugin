package io.conductor.kafkalinter.rules;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.JumpInsnNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.LookupSwitchInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TableSwitchInsnNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Flags commitSync() calls that sit inside the per-record loop of a poll() — the
 * inner iteration over ConsumerRecords. Commit-per-batch (commitSync after the
 * inner loop, still inside the outer poll loop) is fine.
 *
 * Heuristic: a commitSync is "per-record" iff it lives inside a back-edge whose
 * target label appears AFTER the first poll() call in the method. That distinguishes
 * the inner for-each-records loop (target after poll) from the outer while(true)
 * polling loop (target before poll).
 */
public final class ConsumerCommitPerRecordRule implements Rule {

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
        for (MethodNode mn : ctx.classNode().methods) {
            AbstractInsnNode[] all = mn.instructions.toArray();
            Map<LabelNode, Integer> labelIdx = indexLabels(all);
            int firstPollIdx = firstPollIndex(all);
            if (firstPollIdx < 0) continue;

            List<int[]> innerLoops = innerLoopRanges(all, labelIdx, firstPollIdx);
            if (innerLoops.isEmpty()) continue;

            for (int i = 0; i < all.length; i++) {
                AbstractInsnNode insn = all[i];
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) continue;
                if (!mi.name.equals("commitSync")) continue;

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
        return out;
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
