package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.VarInsnNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Flags an {@code AdminClient} constructed via {@code Admin.create(...)} or
 * {@code AdminClient.create(...)} and used in a method but never closed on the same
 * local slot. The non-daemon background thread, broker sockets, and metric registry
 * leak until JVM exit.
 *
 * <p>Mirrors {@link ConsumerNotClosedRule}; differs only in that construction is via
 * an INVOKESTATIC factory rather than a constructor.
 *
 * <p>Conservative escape suppression: if the admin reference is ever stored to a field
 * ({@code PUTFIELD} / {@code PUTSTATIC}) or returned ({@code ARETURN}) via the
 * immediately-preceding {@code ALOAD N}, the slot is marked ESCAPED and the rule does
 * not fire — another method may own the close.
 *
 * <p>Try-with-resources is handled automatically: javac emits a synthetic {@code close()}
 * call in the generated finally region, which is detected like any other close.
 */
public final class AdminNotClosedRule implements Rule {

    private static final String CREATE = "create";

    private final Severity severity;

    public AdminNotClosedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_NOT_CLOSED;
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
        Map<Integer, AbstractInsnNode> adminSlots = new HashMap<>();
        Set<Integer> usedSlots = new HashSet<>();
        Set<Integer> closedSlots = new HashSet<>();
        Set<Integer> escapedSlots = new HashSet<>();

        for (AbstractInsnNode insn : mn.instructions) {
            if (insn instanceof MethodInsnNode factory
                    && factory.getOpcode() == Opcodes.INVOKESTATIC
                    && CREATE.equals(factory.name)
                    && KafkaTypes.ADMIN_OWNERS.contains(factory.owner)) {
                AbstractInsnNode next = AsmUtil.nextSignificant(factory);
                if (next instanceof VarInsnNode v && v.getOpcode() == Opcodes.ASTORE) {
                    adminSlots.put(v.var, factory);
                }
                continue;
            }

            int op = insn.getOpcode();
            if (op == Opcodes.PUTFIELD || op == Opcodes.PUTSTATIC || op == Opcodes.ARETURN) {
                AbstractInsnNode prev = AsmUtil.prevSignificant(insn);
                if (prev instanceof VarInsnNode v && v.getOpcode() == Opcodes.ALOAD
                        && adminSlots.containsKey(v.var)) {
                    escapedSlots.add(v.var);
                }
                continue;
            }

            // Lambda / method-reference capture: an ALOAD of a tracked slot consumed
            // by an INVOKEDYNAMIC (e.g. `admin::close` shutdown hook,
            // `() -> admin.close()`). The slot escapes via the captured lambda;
            // another path may close it — mark ESCAPED.
            if (insn instanceof InvokeDynamicInsnNode indy) {
                escapedSlots.addAll(AsmUtil.indyCapturedSlots(indy, adminSlots.keySet()));
                continue;
            }

            if (!(insn instanceof MethodInsnNode mi)) continue;
            if (!KafkaTypes.ADMIN_OWNERS.contains(mi.owner)) continue;

            Integer slot = AsmUtil.resolveReceiverSlot(mi, adminSlots.keySet());
            if (slot == null) continue;

            if (mi.name.startsWith("close")) {
                closedSlots.add(slot);
            } else {
                usedSlots.add(slot);
            }
        }

        for (Map.Entry<Integer, AbstractInsnNode> e : adminSlots.entrySet()) {
            int slot = e.getKey();
            if (!usedSlots.contains(slot)) continue;
            if (closedSlots.contains(slot)) continue;
            if (escapedSlots.contains(slot)) continue;
            out.add(new Violation(
                    RuleId.ADMIN_NOT_CLOSED, severity,
                    ctx.classNode().name, mn.name, AsmUtil.lineOf(e.getValue()),
                    "AdminClient is constructed via Admin.create() and used in this method "
                            + "but close() is never called on the same local slot. AdminClient holds a "
                            + "non-daemon background thread, broker TCP sockets, and a metric registry — "
                            + "all of which leak until JVM exit because the background thread keeps a "
                            + "strong reference back to the client and GC cannot reclaim it. "
                            + "Use try-with-resources or call close(Duration) in a finally / shutdown hook."));
        }
    }
}
