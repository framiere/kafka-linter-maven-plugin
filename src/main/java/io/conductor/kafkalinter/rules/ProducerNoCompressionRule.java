package io.conductor.kafkalinter.rules;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TypeInsnNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Flags methods that construct a KafkaProducer without ever mentioning the
 * 'compression.type' configuration key. Same-method scan only — keeps the
 * heuristic dumb and predictable. False positives are possible when the
 * config is built in a helper method; documented limitation.
 *
 * Notes:
 *   - ProducerConfig.COMPRESSION_TYPE_CONFIG is a compile-time String constant
 *     that javac inlines, so checking for the literal "compression.type"
 *     LDC covers both source-level forms.
 */
public final class ProducerNoCompressionRule implements Rule {

    private final Severity severity;

    public ProducerNoCompressionRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_NO_COMPRESSION;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            AbstractInsnNode newProducerInsn = findNewKafkaProducer(mn);
            if (newProducerInsn == null) continue;
            if (methodMentions(mn, KafkaTypes.COMPRESSION_TYPE_KEY)) continue;

            out.add(new Violation(
                    RuleId.PRODUCER_NO_COMPRESSION,
                    severity,
                    ctx.classNode().name,
                    mn.name,
                    AsmUtil.lineOf(newProducerInsn),
                    "KafkaProducer is built here but 'compression.type' is never set in this method."));
        }
        return out;
    }

    private static AbstractInsnNode findNewKafkaProducer(MethodNode mn) {
        for (AbstractInsnNode insn : mn.instructions) {
            if (insn.getOpcode() != Opcodes.NEW) continue;
            if (!(insn instanceof TypeInsnNode tn)) continue;
            if (tn.desc.equals(KafkaTypes.KAFKA_PRODUCER)) return insn;
        }
        return null;
    }

    private static boolean methodMentions(MethodNode mn, String key) {
        for (AbstractInsnNode insn : mn.instructions) {
            if (insn instanceof LdcInsnNode ldc && key.equals(ldc.cst)) return true;
        }
        return false;
    }
}
