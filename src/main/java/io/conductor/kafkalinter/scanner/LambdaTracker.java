package io.conductor.kafkalinter.scanner;

import org.objectweb.asm.Handle;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Identifies synthetic lambda methods whose Functional interface is passed directly
 * to a well-known iterating API (Iterable.forEach, Stream.forEach, etc.).
 *
 * Strategy: when an INVOKEDYNAMIC creates a lambda and the very next significant
 * instruction is an INVOKE* to an iterating method, the lambda's synthetic method
 * is treated as a loop body.
 *
 * Limitations (intentional, kept simple):
 *   - The lambda must be consumed by the immediately-following call. Storing the
 *     lambda in a local first is not tracked (false negative).
 *   - The iterating method list is closed. Custom iteration wrappers go undetected.
 */
public final class LambdaTracker {

    private static final Map<String, Set<String>> ITERATING_METHODS = Map.of(
        "java/lang/Iterable", Set.of("forEach"),
        "java/util/Collection", Set.of("forEach"),
        "java/util/List", Set.of("forEach"),
        "java/util/Set", Set.of("forEach"),
        "java/util/Map", Set.of("forEach"),
        "java/util/stream/Stream", Set.of("forEach", "forEachOrdered", "map", "filter", "peek", "flatMap")
    );

    private LambdaTracker() {}

    /** Names of methods in this class that are bodies of "iterating" lambdas. */
    public static Set<String> iteratingLambdaMethodNames(ClassNode cn) {
        Set<String> result = new HashSet<>();
        for (MethodNode mn : cn.methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof InvokeDynamicInsnNode indy)) continue;
                if (!isLambdaMetafactory(indy)) continue;

                AbstractInsnNode next = AsmUtil.nextSignificant(insn);
                if (!(next instanceof MethodInsnNode call)) continue;
                if (!isIteratingCall(call)) continue;

                Handle impl = findImplHandle(indy);
                if (impl == null) continue;
                if (!impl.getOwner().equals(cn.name)) continue; // only lambdas in this class

                result.add(impl.getName());
            }
        }
        return result;
    }

    private static boolean isLambdaMetafactory(InvokeDynamicInsnNode indy) {
        Handle bsm = indy.bsm;
        if (bsm == null) return false;
        return bsm.getOwner().equals("java/lang/invoke/LambdaMetafactory");
    }

    private static Handle findImplHandle(InvokeDynamicInsnNode indy) {
        if (indy.bsmArgs == null) return null;
        // The implementation method handle is at bsmArgs[1] for both metafactory and altMetafactory.
        for (Object arg : indy.bsmArgs) {
            if (arg instanceof Handle h) return h;
        }
        return null;
    }

    private static boolean isIteratingCall(MethodInsnNode call) {
        Set<String> names = ITERATING_METHODS.get(call.owner);
        return names != null && names.contains(call.name);
    }
}
