package io.conductor.kafkalinter.scanner;

import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.FrameNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.LineNumberNode;
import org.objectweb.asm.tree.MethodInsnNode;

public final class AsmUtil {
    private AsmUtil() {}

    /** Walk forward skipping labels, line numbers, and frames. */
    public static AbstractInsnNode nextSignificant(AbstractInsnNode insn) {
        AbstractInsnNode n = insn == null ? null : insn.getNext();
        while (n != null && isTrivia(n)) {
            n = n.getNext();
        }
        return n;
    }

    /** Walk backward skipping labels, line numbers, and frames. */
    public static AbstractInsnNode prevSignificant(AbstractInsnNode insn) {
        AbstractInsnNode n = insn == null ? null : insn.getPrevious();
        while (n != null && isTrivia(n)) {
            n = n.getPrevious();
        }
        return n;
    }

    public static boolean isTrivia(AbstractInsnNode n) {
        return n instanceof LabelNode || n instanceof LineNumberNode || n instanceof FrameNode;
    }

    /** Find the nearest preceding line-number node for an instruction. Returns -1 if unknown. */
    public static int lineOf(AbstractInsnNode insn) {
        AbstractInsnNode n = insn;
        while (n != null) {
            if (n instanceof LineNumberNode ln) {
                return ln.line;
            }
            n = n.getPrevious();
        }
        return -1;
    }

    public static boolean isInvokeOn(AbstractInsnNode insn, java.util.Set<String> owners, String name) {
        if (!(insn instanceof MethodInsnNode m)) return false;
        if (!owners.contains(m.owner)) return false;
        return m.name.equals(name);
    }

    public static boolean isInvokeOn(AbstractInsnNode insn, String owner, String name) {
        if (!(insn instanceof MethodInsnNode m)) return false;
        if (!m.owner.equals(owner)) return false;
        return m.name.equals(name);
    }

    /** True if the descriptor's last argument is the Callback type. */
    public static boolean lastArgIsCallback(String desc) {
        Type[] args = Type.getArgumentTypes(desc);
        if (args.length == 0) return false;
        return args[args.length - 1].getInternalName().equals(KafkaTypes.CALLBACK);
    }
}
