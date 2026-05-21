package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.IntInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Fires when {@code new NewTopic(name, partitions, (short) 1)} is constructed with a
 * literal {@code 1} replication factor — every partition lives on exactly one broker,
 * so any broker reboot or disk loss takes the topic offline (or loses it).
 *
 * <p>Detection is descriptor-pure + literal-int precondition: {@code INVOKESPECIAL
 * NewTopic.<init>(Ljava/lang/String;IS)V} whose previous-significant instruction is
 * an {@code ICONST_1} / {@code BIPUSH 1} / {@code SIPUSH 1} / {@code LDC 1}. The
 * {@code Optional<Short>} and {@code Map<Integer, List<Integer>>} ctor overloads are
 * different descriptors and are deliberately out of scope.
 */
public final class AdminNewTopicReplicationFactorOneRule implements Rule {

    private static final String NEW_TOPIC = "org/apache/kafka/clients/admin/NewTopic";
    private static final String CTOR_DESC = "(Ljava/lang/String;IS)V";

    private final Severity severity;

    public AdminNewTopicReplicationFactorOneRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_NEW_TOPIC_REPLICATION_FACTOR_ONE;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (mi.getOpcode() != Opcodes.INVOKESPECIAL) continue;
                if (!NEW_TOPIC.equals(mi.owner)) continue;
                if (!"<init>".equals(mi.name)) continue;
                if (!CTOR_DESC.equals(mi.desc)) continue;

                Integer rf = extractIntLiteral(AsmUtil.prevSignificant(insn));
                if (rf == null || rf != 1) continue;

                out.add(new Violation(
                        RuleId.ADMIN_NEW_TOPIC_REPLICATION_FACTOR_ONE, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "new NewTopic(name, partitions, (short) 1) — replication factor 1 means "
                                + "every partition lives on a single broker. Any broker restart "
                                + "(rolling upgrade, kernel patch, hardware failure) takes the "
                                + "topic offline; a disk loss loses the data. Use (short) 3 in "
                                + "production, or the Optional<Short> overload with Optional.empty() "
                                + "to inherit the cluster's default.replication.factor."));
            }
        }
        return out;
    }

    private static Integer extractIntLiteral(AbstractInsnNode n) {
        if (n == null) return null;
        if (n instanceof IntInsnNode in) return in.operand; // BIPUSH / SIPUSH
        if (n instanceof LdcInsnNode l && l.cst instanceof Integer i) return i;
        int op = n.getOpcode();
        // ICONST_M1..ICONST_5 are opcodes 0x02..0x08
        if (op >= 0x02 && op <= 0x08) return op - 0x03;
        return null;
    }
}
