package io.conductor.kafkalinter.scanner;

import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Per-class precomputed analysis shared across rules: loops per method, and the set
 * of synthetic lambda methods that are bodies of iterating lambdas (forEach, stream ops, …).
 *
 * Use {@link #isInLoopOrIteratingLambda} to query whether an instruction inside the given
 * method should be considered "executing inside a loop". This is the single check rules
 * use to decide whether something is in a hot iteration context.
 */
public final class RuleContext {

    private final ClassNode classNode;
    private final Set<String> iteratingLambdaMethods;
    private final Map<MethodNode, Set<AbstractInsnNode>> loopInsnsPerMethod = new HashMap<>();

    public RuleContext(ClassNode classNode) {
        this.classNode = classNode;
        this.iteratingLambdaMethods = LambdaTracker.iteratingLambdaMethodNames(classNode);
        for (MethodNode mn : classNode.methods) {
            loopInsnsPerMethod.put(mn, LoopFinder.instructionsInLoops(mn));
        }
    }

    public ClassNode classNode() {
        return classNode;
    }

    /** True if this method's full body should be treated as a loop (it's an iterating lambda body). */
    public boolean isIteratingLambdaBody(MethodNode mn) {
        return iteratingLambdaMethods.contains(mn.name);
    }

    public boolean isInLoopOrIteratingLambda(MethodNode mn, AbstractInsnNode insn) {
        if (isIteratingLambdaBody(mn)) return true;
        Set<AbstractInsnNode> loopInsns = loopInsnsPerMethod.get(mn);
        return loopInsns != null && loopInsns.contains(insn);
    }
}
