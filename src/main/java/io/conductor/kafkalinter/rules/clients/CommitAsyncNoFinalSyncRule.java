package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
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
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.CONSUMER_OWNERS.contains(mi.owner)) continue;
                if ("commitSync".equals(mi.name)) {
                    syncFound = true;
                } else if ("commitAsync".equals(mi.name) && firstAsyncInsn == null) {
                    firstAsyncMethod = mn;
                    firstAsyncInsn = insn;
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
