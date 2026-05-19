package io.conductor.kafkalinter.scanner;

import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.JumpInsnNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.LookupSwitchInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TableSwitchInsnNode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Detects which instructions in a method live inside a loop body, by finding
 * back-edges (jump whose target appears earlier in the InsnList than the jump itself).
 *
 * Handles for / while / do-while / foreach uniformly — they all compile to a backward jump.
 * Nested loops fall out naturally: an instruction inside any loop range is marked.
 */
public final class LoopFinder {

    private LoopFinder() {}

    public static Set<AbstractInsnNode> instructionsInLoops(MethodNode method) {
        Set<AbstractInsnNode> inLoop = new HashSet<>();
        Map<LabelNode, Integer> labelIndex = new IdentityHashMap<>();

        AbstractInsnNode[] all = method.instructions.toArray();
        for (int i = 0; i < all.length; i++) {
            if (all[i] instanceof LabelNode ln) {
                labelIndex.put(ln, i);
            }
        }

        for (int i = 0; i < all.length; i++) {
            AbstractInsnNode insn = all[i];
            if (insn instanceof JumpInsnNode j) {
                addBackwardRange(inLoop, all, labelIndex, i, j.label);
            } else if (insn instanceof TableSwitchInsnNode ts) {
                for (LabelNode l : ts.labels) addBackwardRange(inLoop, all, labelIndex, i, l);
                addBackwardRange(inLoop, all, labelIndex, i, ts.dflt);
            } else if (insn instanceof LookupSwitchInsnNode ls) {
                for (LabelNode l : ls.labels) addBackwardRange(inLoop, all, labelIndex, i, l);
                addBackwardRange(inLoop, all, labelIndex, i, ls.dflt);
            }
        }
        return inLoop;
    }

    private static void addBackwardRange(Set<AbstractInsnNode> inLoop,
                                         AbstractInsnNode[] all,
                                         Map<LabelNode, Integer> labelIndex,
                                         int jumpIdx,
                                         LabelNode target) {
        if (target == null) return;
        Integer t = labelIndex.get(target);
        if (t == null) return;
        if (t >= jumpIdx) return; // not a back-edge
        for (int k = t; k <= jumpIdx; k++) {
            inLoop.add(all[k]);
        }
    }
}
