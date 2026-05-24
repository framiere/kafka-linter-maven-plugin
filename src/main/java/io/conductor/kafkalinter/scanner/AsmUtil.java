package io.conductor.kafkalinter.scanner;

import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.FrameNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.LineNumberNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MultiANewArrayInsnNode;
import org.objectweb.asm.tree.VarInsnNode;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

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

    /**
     * Walk forward from {@code insn} skipping trivia and pure-push instructions
     * (constants, GETSTATIC, LDC, *LOAD) — instructions that push without
     * popping — and return the first {@link MethodInsnNode} encountered.
     * Returns {@code null} if a non-pure-push, non-method instruction is
     * reached first (POP, PUTFIELD, arithmetic, branch, INVOKEDYNAMIC, etc.).
     *
     * <p>Intended for finding the dispatch INVOKE that consumes a value pushed
     * by an earlier instruction (typically an INVOKEDYNAMIC producing a
     * functional-interface instance) when later args are pushed as constants
     * or static fields between the producer and the dispatch site.
     *
     * <p>Tolerates the common shape
     * {@code dispatch(lambda, c0, c1, TimeUnit.SECONDS)} — where the lambda
     * is the first/deepest arg of the dispatcher and the trailing args are
     * pure pushes. Does NOT tolerate intermediate method calls or POPs.
     */
    public static MethodInsnNode nextDispatchInvoke(AbstractInsnNode insn) {
        AbstractInsnNode n = insn == null ? null : insn.getNext();
        while (n != null) {
            if (isTrivia(n)) {
                n = n.getNext();
                continue;
            }
            if (n instanceof MethodInsnNode mi) {
                return mi;
            }
            if (isPurePush(n)) {
                n = n.getNext();
                continue;
            }
            return null;
        }
        return null;
    }

    private static boolean isPurePush(AbstractInsnNode n) {
        int op = n.getOpcode();
        switch (op) {
            case Opcodes.ACONST_NULL:
            case Opcodes.ICONST_M1: case Opcodes.ICONST_0: case Opcodes.ICONST_1:
            case Opcodes.ICONST_2: case Opcodes.ICONST_3: case Opcodes.ICONST_4: case Opcodes.ICONST_5:
            case Opcodes.LCONST_0: case Opcodes.LCONST_1:
            case Opcodes.FCONST_0: case Opcodes.FCONST_1: case Opcodes.FCONST_2:
            case Opcodes.DCONST_0: case Opcodes.DCONST_1:
            case Opcodes.BIPUSH: case Opcodes.SIPUSH:
            case Opcodes.LDC:
            case Opcodes.GETSTATIC:
            case Opcodes.ILOAD: case Opcodes.LLOAD: case Opcodes.FLOAD:
            case Opcodes.DLOAD: case Opcodes.ALOAD:
                return true;
            default:
                return false;
        }
    }

    /**
     * Inspect an {@link InvokeDynamicInsnNode}'s bootstrap-method arguments and
     * return the first {@link Handle} matching the given {@code owners} / {@code name} /
     * {@code desc}. Returns {@code null} if no such handle exists. Any of the
     * filters may be {@code null} to disable that filter.
     *
     * <p>Use case: a method reference like {@code client::close} is compiled to
     * an INVOKEDYNAMIC whose bootstrap-method args contain a direct
     * {@code REF_invokeVirtual} (or {@code REF_invokeInterface}) handle pointing
     * straight at the target method — no synthetic {@code lambda$N} body is
     * generated. The outer-method bytecode therefore contains zero
     * {@code INVOKE*} instructions pointing at the target, and naive
     * MethodInsnNode scans miss the callsite entirely. This helper recovers it
     * by reading the bootstrap arg list.
     *
     * <p>Concretely: this turns false-negatives like
     * {@code Runtime.getRuntime().addShutdownHook(new Thread(consumer::close))}
     * into a fire of CONSUMER_CLOSE_NO_TIMEOUT — the no-arg close descriptor
     * {@code ()V} is the very thing the method reference captures.
     */
    public static Handle indyTargetHandle(InvokeDynamicInsnNode indy, Set<String> owners, String name, String desc) {
        if (indy == null || indy.bsmArgs == null) return null;
        for (Object arg : indy.bsmArgs) {
            if (!(arg instanceof Handle h)) continue;
            if (owners != null && !owners.contains(h.getOwner())) continue;
            if (name != null && !name.equals(h.getName())) continue;
            if (desc != null && !desc.equals(h.getDesc())) continue;
            return h;
        }
        return null;
    }

    /**
     * Collect the tracked local-variable slots that are captured by an
     * {@link InvokeDynamicInsnNode} — i.e. {@code ALOAD N} pushes that sit
     * immediately before the indy and feed its bootstrap-method capture args.
     *
     * <p>Backward walk through preceding significant instructions, stopping
     * at the first non-pure-push (anything that pops, branches, or calls).
     * The walk is bounded by the indy descriptor's stack-slot count so it
     * never crosses a clean stack frame.
     *
     * <p>Use case: a method-local Kafka client closed via {@code client::close}
     * shutdown hook or {@code () -> client.close()} lambda. The slot escapes
     * via the indy capture, and the {@code *NotClosed} rules must not fire.
     * Treating any captured tracked slot as ESCAPED is conservative — we
     * acknowledge the slot is in the wild and another path may close it.
     */
    public static Set<Integer> indyCapturedSlots(InvokeDynamicInsnNode indy, Set<Integer> trackedSlots) {
        if (indy == null || trackedSlots == null || trackedSlots.isEmpty()) return Collections.emptySet();
        int sz = Type.getArgumentsAndReturnSizes(indy.desc);
        int slotsNeeded = (sz >> 2) - 1;
        if (slotsNeeded <= 0) return Collections.emptySet();
        Set<Integer> out = new HashSet<>();
        AbstractInsnNode cursor = prevSignificant(indy);
        while (cursor != null && slotsNeeded > 0) {
            // javac null-check idiom for `expr::method` references:
            //     ALOAD slot; DUP; INVOKESTATIC Objects.requireNonNull; POP; INVOKEDYNAMIC
            // The trio (DUP + requireNonNull + POP) has net stack effect 0 — the
            // captured value is still the ALOAD that came before. Peel it off so
            // the walk can reach the underlying ALOAD.
            AbstractInsnNode peeled = peelNullCheck(cursor);
            if (peeled != null) {
                cursor = peeled;
                continue;
            }
            int[] eff = stackEffect(cursor);
            if (eff == null || eff[0] != 0 || eff[1] <= 0) break;
            if (cursor instanceof VarInsnNode v && v.getOpcode() == Opcodes.ALOAD
                    && trackedSlots.contains(v.var)) {
                out.add(v.var);
            }
            slotsNeeded -= eff[1];
            cursor = prevSignificant(cursor);
        }
        return out;
    }

    /**
     * If {@code cursor} is the {@code POP} of the javac-emitted null-check trio
     * {@code DUP / Objects.requireNonNull / POP}, return the significant
     * instruction preceding the {@code DUP} (i.e. the value that was originally
     * pushed). Otherwise return {@code null}.
     */
    private static AbstractInsnNode peelNullCheck(AbstractInsnNode cursor) {
        if (cursor == null || cursor.getOpcode() != Opcodes.POP) return null;
        AbstractInsnNode req = prevSignificant(cursor);
        if (!(req instanceof MethodInsnNode m)
                || m.getOpcode() != Opcodes.INVOKESTATIC
                || !"java/util/Objects".equals(m.owner)
                || !"requireNonNull".equals(m.name)
                || !"(Ljava/lang/Object;)Ljava/lang/Object;".equals(m.desc)) {
            return null;
        }
        AbstractInsnNode dup = prevSignificant(req);
        if (dup == null || dup.getOpcode() != Opcodes.DUP) return null;
        return prevSignificant(dup);
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

    /**
     * Resolve the local-slot receiver of a method-invocation by backward stack-effect
     * simulation. Walks back from {@code mi}, accounting for each instruction's
     * (pops, pushes), until the "needed" counter (initialized to the number of stack
     * items the invoke consumes) drops to zero — that landing instruction is the
     * push that supplied the deepest consumed item, i.e. the receiver.
     *
     * <p>Returns the local-variable slot if the landing instruction is an
     * {@code ALOAD N} into {@code trackedSlots}. Returns {@code null} when:
     * <ul>
     *   <li>the simulation encounters an instruction with unknown stack effects;</li>
     *   <li>the landing instruction is not a simple {@code ALOAD};</li>
     *   <li>{@code needed} crosses zero in a single step (the receiver was produced
     *       by a multi-output instruction like {@code DUP}, too complex to attribute).</li>
     * </ul>
     * Bailing out is a safe false-negative.
     */
    public static Integer resolveReceiverSlot(MethodInsnNode mi, Set<Integer> trackedSlots) {
        int popCount = invokePopCount(mi);
        if (popCount <= 0) return null;
        int needed = popCount;
        AbstractInsnNode cursor = prevSignificant(mi);
        while (cursor != null) {
            int[] eff = stackEffect(cursor);
            if (eff == null) return null;
            needed -= eff[1];
            if (needed <= 0) {
                if (needed < 0) return null;
                if (cursor instanceof VarInsnNode v && v.getOpcode() == Opcodes.ALOAD
                        && trackedSlots.contains(v.var)) {
                    return v.var;
                }
                return null;
            }
            needed += eff[0];
            cursor = prevSignificant(cursor);
        }
        return null;
    }

    /** Total stack-slot pops performed by an INVOKE* instruction (args + receiver if instance). */
    public static int invokePopCount(MethodInsnNode mi) {
        int sz = Type.getArgumentsAndReturnSizes(mi.desc);
        int argSize = sz >> 2;
        return mi.getOpcode() == Opcodes.INVOKESTATIC ? argSize - 1 : argSize;
    }

    /**
     * Compute {@code [pops, pushes]} stack-slot effects of a single instruction,
     * counting long/double as 2 slots. Returns {@code null} when the effect cannot
     * be computed without control-flow context (branches, returns, athrow).
     */
    public static int[] stackEffect(AbstractInsnNode insn) {
        int op = insn.getOpcode();
        if (op < 0) return new int[]{0, 0};
        switch (op) {
            case Opcodes.NOP:
                return new int[]{0, 0};
            case Opcodes.ACONST_NULL:
            case Opcodes.ICONST_M1: case Opcodes.ICONST_0: case Opcodes.ICONST_1:
            case Opcodes.ICONST_2: case Opcodes.ICONST_3: case Opcodes.ICONST_4: case Opcodes.ICONST_5:
            case Opcodes.FCONST_0: case Opcodes.FCONST_1: case Opcodes.FCONST_2:
            case Opcodes.BIPUSH: case Opcodes.SIPUSH:
                return new int[]{0, 1};
            case Opcodes.LCONST_0: case Opcodes.LCONST_1:
            case Opcodes.DCONST_0: case Opcodes.DCONST_1:
                return new int[]{0, 2};
            case Opcodes.LDC: {
                if (!(insn instanceof LdcInsnNode ldc)) return null;
                Object v = ldc.cst;
                return new int[]{0, (v instanceof Long || v instanceof Double) ? 2 : 1};
            }
            case Opcodes.ILOAD: case Opcodes.FLOAD: case Opcodes.ALOAD:
                return new int[]{0, 1};
            case Opcodes.LLOAD: case Opcodes.DLOAD:
                return new int[]{0, 2};
            case Opcodes.ISTORE: case Opcodes.FSTORE: case Opcodes.ASTORE:
                return new int[]{1, 0};
            case Opcodes.LSTORE: case Opcodes.DSTORE:
                return new int[]{2, 0};
            case Opcodes.IALOAD: case Opcodes.FALOAD: case Opcodes.AALOAD:
            case Opcodes.BALOAD: case Opcodes.CALOAD: case Opcodes.SALOAD:
                return new int[]{2, 1};
            case Opcodes.LALOAD: case Opcodes.DALOAD:
                return new int[]{2, 2};
            case Opcodes.IASTORE: case Opcodes.FASTORE: case Opcodes.AASTORE:
            case Opcodes.BASTORE: case Opcodes.CASTORE: case Opcodes.SASTORE:
                return new int[]{3, 0};
            case Opcodes.LASTORE: case Opcodes.DASTORE:
                return new int[]{4, 0};
            case Opcodes.POP: return new int[]{1, 0};
            case Opcodes.POP2: return new int[]{2, 0};
            case Opcodes.DUP: return new int[]{1, 2};
            case Opcodes.DUP_X1: return new int[]{2, 3};
            case Opcodes.DUP_X2: return new int[]{3, 4};
            case Opcodes.DUP2: return new int[]{2, 4};
            case Opcodes.DUP2_X1: return new int[]{3, 5};
            case Opcodes.DUP2_X2: return new int[]{4, 6};
            case Opcodes.SWAP: return new int[]{2, 2};
            case Opcodes.IADD: case Opcodes.ISUB: case Opcodes.IMUL: case Opcodes.IDIV: case Opcodes.IREM:
            case Opcodes.FADD: case Opcodes.FSUB: case Opcodes.FMUL: case Opcodes.FDIV: case Opcodes.FREM:
            case Opcodes.IAND: case Opcodes.IOR: case Opcodes.IXOR:
            case Opcodes.ISHL: case Opcodes.ISHR: case Opcodes.IUSHR:
                return new int[]{2, 1};
            case Opcodes.LADD: case Opcodes.LSUB: case Opcodes.LMUL: case Opcodes.LDIV: case Opcodes.LREM:
            case Opcodes.DADD: case Opcodes.DSUB: case Opcodes.DMUL: case Opcodes.DDIV: case Opcodes.DREM:
            case Opcodes.LAND: case Opcodes.LOR: case Opcodes.LXOR:
                return new int[]{4, 2};
            case Opcodes.LSHL: case Opcodes.LSHR: case Opcodes.LUSHR:
                return new int[]{3, 2};
            case Opcodes.INEG: case Opcodes.FNEG: return new int[]{1, 1};
            case Opcodes.LNEG: case Opcodes.DNEG: return new int[]{2, 2};
            case Opcodes.IINC: return new int[]{0, 0};
            case Opcodes.I2L: case Opcodes.I2D: case Opcodes.F2L: case Opcodes.F2D:
                return new int[]{1, 2};
            case Opcodes.L2I: case Opcodes.L2F: case Opcodes.D2I: case Opcodes.D2F:
                return new int[]{2, 1};
            case Opcodes.L2D: case Opcodes.D2L:
                return new int[]{2, 2};
            case Opcodes.I2F: case Opcodes.F2I: case Opcodes.I2B: case Opcodes.I2C: case Opcodes.I2S:
                return new int[]{1, 1};
            case Opcodes.LCMP: return new int[]{4, 1};
            case Opcodes.FCMPL: case Opcodes.FCMPG: return new int[]{2, 1};
            case Opcodes.DCMPL: case Opcodes.DCMPG: return new int[]{4, 1};
            case Opcodes.GETSTATIC:
                if (!(insn instanceof FieldInsnNode f)) return null;
                return new int[]{0, Type.getType(f.desc).getSize()};
            case Opcodes.PUTSTATIC:
                if (!(insn instanceof FieldInsnNode f2)) return null;
                return new int[]{Type.getType(f2.desc).getSize(), 0};
            case Opcodes.GETFIELD:
                if (!(insn instanceof FieldInsnNode f3)) return null;
                return new int[]{1, Type.getType(f3.desc).getSize()};
            case Opcodes.PUTFIELD:
                if (!(insn instanceof FieldInsnNode f4)) return null;
                return new int[]{1 + Type.getType(f4.desc).getSize(), 0};
            case Opcodes.INVOKEVIRTUAL:
            case Opcodes.INVOKESPECIAL:
            case Opcodes.INVOKEINTERFACE: {
                if (!(insn instanceof MethodInsnNode m)) return null;
                int sz = Type.getArgumentsAndReturnSizes(m.desc);
                return new int[]{sz >> 2, sz & 3};
            }
            case Opcodes.INVOKESTATIC: {
                if (!(insn instanceof MethodInsnNode m)) return null;
                int sz = Type.getArgumentsAndReturnSizes(m.desc);
                return new int[]{(sz >> 2) - 1, sz & 3};
            }
            case Opcodes.INVOKEDYNAMIC: {
                if (!(insn instanceof InvokeDynamicInsnNode m)) return null;
                int sz = Type.getArgumentsAndReturnSizes(m.desc);
                return new int[]{(sz >> 2) - 1, sz & 3};
            }
            case Opcodes.NEW: return new int[]{0, 1};
            case Opcodes.NEWARRAY: case Opcodes.ANEWARRAY: return new int[]{1, 1};
            case Opcodes.MULTIANEWARRAY: {
                if (!(insn instanceof MultiANewArrayInsnNode m)) return null;
                return new int[]{m.dims, 1};
            }
            case Opcodes.ARRAYLENGTH: return new int[]{1, 1};
            case Opcodes.CHECKCAST: return new int[]{1, 1};
            case Opcodes.INSTANCEOF: return new int[]{1, 1};
            case Opcodes.MONITORENTER: case Opcodes.MONITOREXIT:
                return new int[]{1, 0};
            // Control flow / terminators / branches: receiver resolution should never need to cross these.
            default:
                return null;
        }
    }
}
