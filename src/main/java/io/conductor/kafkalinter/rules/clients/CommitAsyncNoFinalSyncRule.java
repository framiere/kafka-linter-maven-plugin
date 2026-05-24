package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Handle;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Flags classes that call {@code Consumer.commitAsync(...)} but never call
 * {@code Consumer.commitSync(...)} anywhere in the same class — the last
 * async commit can be lost on shutdown. The canonical idiom is commitAsync
 * in the poll loop for throughput, commitSync once in a finally block for
 * durability. Scope is class rather than method because the loop and the
 * shutdown hook usually live in separate methods.
 *
 * <p><strong>Both commitAsync and commitSync are recognised in
 * {@link InvokeDynamicInsnNode} bootstrap-arg handles in addition to direct
 * {@link MethodInsnNode}s.</strong> javac compiles a method reference like
 * {@code consumer::commitSync} (commonly captured into a shutdown hook,
 * e.g. {@code Runtime.getRuntime().addShutdownHook(new Thread(consumer::commitSync))})
 * to an {@code INVOKEDYNAMIC} whose bsm-args contain a direct
 * {@code REF_invokeVirtual Consumer.commitSync:()V} (or
 * {@code REF_invokeInterface}) handle — and the user-class bytecode contains
 * zero {@code INVOKE*} instructions targeting {@code commitSync}. A scan that
 * walked only {@code MethodInsnNode}s would treat this as "no commitSync",
 * conclude the class is unbalanced, and fire a false positive on the
 * commitAsync site. Inspecting indy bsm-args avoids that.
 */
public final class CommitAsyncNoFinalSyncRule implements Rule {

    private final Severity severity;

    public CommitAsyncNoFinalSyncRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.COMMIT_ASYNC_NO_FINAL_SYNC;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        MethodNode firstAsyncMethod = null;
        AbstractInsnNode firstAsyncInsn = null;
        boolean syncFound = false;

        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) {
                    if ("commitSync".equals(mi.name)) {
                        syncFound = true;
                    } else if ("commitAsync".equals(mi.name) && firstAsyncInsn == null) {
                        firstAsyncMethod = mn;
                        firstAsyncInsn = insn;
                    }
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, KafkaTypes.CONSUMER_OWNERS, null, null);
                    if (h == null) continue;
                    if ("commitSync".equals(h.getName())) {
                        syncFound = true;
                    } else if ("commitAsync".equals(h.getName()) && firstAsyncInsn == null) {
                        firstAsyncMethod = mn;
                        firstAsyncInsn = insn;
                    }
                }
            }
            if (syncFound && firstAsyncInsn != null) break;
        }

        if (firstAsyncInsn == null || syncFound) return List.of();

        List<Violation> out = new ArrayList<>(1);
        out.add(new Violation(
                RuleId.COMMIT_ASYNC_NO_FINAL_SYNC, severity,
                ctx.classNode().name, firstAsyncMethod.name, AsmUtil.lineOf(firstAsyncInsn),
                "Consumer.commitAsync() is used in this class but no Consumer.commitSync() balances it on shutdown — the last async commit can be lost. Pair commitAsync (in the poll loop) with commitSync in a finally block."));
        return out;
    }
}
