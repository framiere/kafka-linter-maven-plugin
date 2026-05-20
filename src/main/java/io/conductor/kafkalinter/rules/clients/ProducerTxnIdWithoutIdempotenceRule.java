package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Flags the configuration contradiction "transactional.id set" + "enable.idempotence=false"
 * within the same class.
 *
 * <p>Modern kafka-clients (3.0+) default {@code enable.idempotence} to {@code true} and a
 * transactional producer requires it, so we only fire when idempotence is explicitly
 * set to {@code "false"}. Anything else is left to runtime (the broker will throw
 * {@code ConfigException} but we don't pile on).
 */
public final class ProducerTxnIdWithoutIdempotenceRule implements Rule {

    private final Severity severity;

    public ProducerTxnIdWithoutIdempotenceRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        AbstractInsnNode txnIdInsn = null;
        String txnIdMethod = null;
        AbstractInsnNode idempotenceFalseInsn = null;
        String idempotenceMethod = null;

        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.CONFIG_HOLDERS.contains(mi.owner)) continue;
                if (!KafkaTypes.CONFIG_PUT_METHODS.contains(mi.name)) continue;

                AbstractInsnNode valueLdc = AsmUtil.prevSignificant(insn);
                AbstractInsnNode keyLdc = AsmUtil.prevSignificant(valueLdc);
                if (!(keyLdc instanceof LdcInsnNode keyL)) continue;
                if (!(valueLdc instanceof LdcInsnNode valL)) continue;
                Object key = keyL.cst;
                Object val = valL.cst;

                if (KafkaTypes.TRANSACTIONAL_ID_KEY.equals(key) && txnIdInsn == null) {
                    txnIdInsn = insn;
                    txnIdMethod = mn.name;
                }
                if (KafkaTypes.ENABLE_IDEMPOTENCE_KEY.equals(key) && "false".equals(String.valueOf(val))) {
                    idempotenceFalseInsn = insn;
                    idempotenceMethod = mn.name;
                }
            }
        }

        if (txnIdInsn == null || idempotenceFalseInsn == null) return List.of();

        List<Violation> out = new ArrayList<>(1);
        out.add(new Violation(
                RuleId.PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE, severity,
                ctx.classNode().name, idempotenceMethod, AsmUtil.lineOf(idempotenceFalseInsn),
                "transactional.id is set (at " + txnIdMethod + ":" + AsmUtil.lineOf(txnIdInsn)
                        + ") but enable.idempotence=false in the same class — broker will reject this combination."));
        return out;
    }
}
