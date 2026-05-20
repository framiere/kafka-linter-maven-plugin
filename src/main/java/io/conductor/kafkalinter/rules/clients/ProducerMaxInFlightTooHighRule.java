package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.IntInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Flags {@code max.in.flight.requests.per.connection} set to a literal > 5 — the broker
 * rejects this when {@code enable.idempotence=true} (the kafka-clients 3.0+ default).
 *
 * <p>The cap of 5 is hard-coded by the idempotent-producer dedupe machinery (PID +
 * sequence number reorder window). We don't try to verify that idempotence is on; we
 * flag any literal > 5 because it is at best dead config and at worst a runtime crash.
 */
public final class ProducerMaxInFlightTooHighRule implements Rule {

    private static final int CAP = 5;

    private final Severity severity;

    public ProducerMaxInFlightTooHighRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_MAX_IN_FLIGHT_TOO_HIGH;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.CONFIG_HOLDERS.contains(mi.owner)) continue;
                if (!KafkaTypes.CONFIG_PUT_METHODS.contains(mi.name)) continue;

                AbstractInsnNode valueInsn = AsmUtil.prevSignificant(insn);
                AbstractInsnNode keyLdc = AsmUtil.prevSignificant(valueInsn);
                if (!(keyLdc instanceof LdcInsnNode keyL)) continue;
                if (!KafkaTypes.MAX_IN_FLIGHT_KEY.equals(keyL.cst)) continue;

                Integer parsed = extractIntLiteral(valueInsn);
                if (parsed == null || parsed <= CAP) continue;

                out.add(new Violation(
                        RuleId.PRODUCER_MAX_IN_FLIGHT_TOO_HIGH, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "max.in.flight.requests.per.connection=" + parsed
                                + " > " + CAP + " — incompatible with enable.idempotence=true (default since 3.0)."));
            }
        }
        return out;
    }

    /** Pull a small-int or long literal out of an LDC or BIPUSH/SIPUSH or ICONST_* node. */
    private static Integer extractIntLiteral(AbstractInsnNode n) {
        if (n instanceof LdcInsnNode l) {
            if (l.cst instanceof Integer i) return i;
            if (l.cst instanceof Long ll && ll <= Integer.MAX_VALUE) return ll.intValue();
            if (l.cst instanceof String s) {
                try { return Integer.parseInt(s.trim()); } catch (NumberFormatException ignored) { return null; }
            }
            return null;
        }
        if (n instanceof IntInsnNode in) return in.operand;
        if (n == null) return null;
        int op = n.getOpcode();
        // ICONST_M1..ICONST_5
        if (op >= 0x02 && op <= 0x08) return op - 0x03;
        return null;
    }
}
