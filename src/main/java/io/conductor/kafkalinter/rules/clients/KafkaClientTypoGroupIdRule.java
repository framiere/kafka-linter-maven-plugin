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
import java.util.Set;

/**
 * Flags common typos for Kafka client config keys: {@code groupId}, {@code group_id},
 * {@code bootstrapServers}, {@code bootstrap_servers}, {@code enableAutoCommit},
 * {@code enable_auto_commit}. Kafka silently ignores unknown keys, so these typos
 * lead to "the config doesn't seem to apply" bugs that look like infra issues.
 */
public final class KafkaClientTypoGroupIdRule implements Rule {

    private static final Set<String> TYPOS = Set.of(
            "groupId", "group_id",
            "bootstrapServers", "bootstrap_servers",
            "enableAutoCommit", "enable_auto_commit",
            "autoOffsetReset", "auto_offset_reset",
            "compressionType", "compression_type"
    );

    private final Severity severity;

    public KafkaClientTypoGroupIdRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.KAFKA_CLIENT_TYPO_GROUP_ID;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.CONFIG_HOLDERS.contains(mi.owner)) continue;
                if (!KafkaTypes.CONFIG_PUT_METHODS.contains(mi.name)) continue;

                AbstractInsnNode valueLdc = AsmUtil.prevSignificant(insn);
                AbstractInsnNode keyLdc = AsmUtil.prevSignificant(valueLdc);
                if (!(keyLdc instanceof LdcInsnNode keyL)) continue;
                if (!(keyL.cst instanceof String typo)) continue;
                if (!TYPOS.contains(typo)) continue;

                out.add(new Violation(
                        RuleId.KAFKA_CLIENT_TYPO_GROUP_ID, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "Config key \"" + typo + "\" looks like a typo — Kafka uses dot-separated keys ('"
                                + suggest(typo) + "')."));
            }
        }
        return out;
    }

    private static String suggest(String typo) {
        return switch (typo) {
            case "groupId", "group_id" -> "group.id";
            case "bootstrapServers", "bootstrap_servers" -> "bootstrap.servers";
            case "enableAutoCommit", "enable_auto_commit" -> "enable.auto.commit";
            case "autoOffsetReset", "auto_offset_reset" -> "auto.offset.reset";
            case "compressionType", "compression_type" -> "compression.type";
            default -> typo;
        };
    }
}
