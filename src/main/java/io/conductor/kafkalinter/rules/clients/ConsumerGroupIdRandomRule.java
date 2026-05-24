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
 * Fires when {@code group.id} is set to {@code UUID.randomUUID().toString()}
 * in a Consumer config.
 *
 * <h2>Why this is dangerous</h2>
 *
 * <p>{@code group.id} is the application-stable identity Kafka uses to track
 * BOTH partition ownership (the group-membership protocol) AND committed-offset
 * history. A random per-process group.id means every restart establishes a
 * brand-new consumer group with no committed offsets — the consumer falls
 * through to {@code auto.offset.reset}, whose default is {@code latest},
 * silently skipping every record produced while the consumer was down.
 *
 * <h2>Bytecode shape matched</h2>
 *
 * <p>For {@code props.put("group.id", UUID.randomUUID().toString())}:
 * <pre>
 *   ALOAD       props
 *   LDC         "group.id"              &lt;- key
 *   INVOKESTATIC  java/util/UUID.randomUUID()Ljava/util/UUID;
 *   INVOKEVIRTUAL java/util/UUID.toString()Ljava/lang/String;   &lt;- value
 *   INVOKE      Properties.put / Map.put / setProperty
 * </pre>
 *
 * <p>{@code ConsumerConfig.GROUP_ID_CONFIG} is a compile-time {@code String}
 * constant whose value is {@code "group.id"} — javac inlines such constants
 * per JLS §15.28, so both source styles compile to the same
 * {@code LDC "group.id"} and the rule matches them identically.
 *
 * <h2>Walk strategy</h2>
 *
 * <p>For each {@code put}/{@code setProperty} on a {@link KafkaTypes#CONFIG_HOLDERS}
 * owner, walk backwards via {@link AsmUtil#prevSignificant} through the three
 * preceding significant instructions: toString → randomUUID → key-LDC. If all
 * three match and the key is {@link KafkaTypes#GROUP_ID_KEY}, emit a violation
 * at the {@code randomUUID()} call site.
 */
public final class ConsumerGroupIdRandomRule implements Rule {

    private static final String UUID_OWNER = "java/util/UUID";
    private static final String RANDOM_UUID = "randomUUID";
    private static final String TO_STRING = "toString";
    private static final String RANDOM_UUID_DESC = "()Ljava/util/UUID;";
    private static final String TO_STRING_DESC = "()Ljava/lang/String;";

    private final Severity severity;

    public ConsumerGroupIdRandomRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_GROUP_ID_RANDOM;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode put)) continue;
                if (!KafkaTypes.CONFIG_HOLDERS.contains(put.owner)) continue;
                if (!KafkaTypes.CONFIG_PUT_METHODS.contains(put.name)) continue;

                AbstractInsnNode v1 = AsmUtil.prevSignificant(put);
                if (!isInvokeOn(v1, UUID_OWNER, TO_STRING, TO_STRING_DESC)) continue;

                AbstractInsnNode v2 = AsmUtil.prevSignificant(v1);
                if (!isInvokeOn(v2, UUID_OWNER, RANDOM_UUID, RANDOM_UUID_DESC)) continue;

                AbstractInsnNode keyInsn = AsmUtil.prevSignificant(v2);
                if (!(keyInsn instanceof LdcInsnNode keyL)) continue;
                if (!KafkaTypes.GROUP_ID_KEY.equals(keyL.cst)) continue;

                out.add(new Violation(
                        RuleId.CONSUMER_GROUP_ID_RANDOM, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(v2),
                        "group.id = UUID.randomUUID().toString() — a random per-process group has no "
                                + "committed-offset history. Every restart establishes a brand-new "
                                + "consumer group: with auto.offset.reset=latest (the default) every "
                                + "record produced between crash and restart is silently skipped; with "
                                + "earliest the consumer replays the entire topic on every restart. "
                                + "Either way at-least-once is broken — gap or duplication on every pod "
                                + "rotation. Use a stable orchestrator-assigned id (deployment name, "
                                + "helm release, application name) and version it explicitly when "
                                + "consumer semantics change incompatibly. Random group.id is only "
                                + "acceptable for ephemeral debug tools (kafka-console-consumer-style "
                                + "fan-out reads) — never for persistent business workloads."));
            }
        }
        return out;
    }

    private static boolean isInvokeOn(AbstractInsnNode insn, String owner, String name, String desc) {
        return insn instanceof MethodInsnNode mi
                && owner.equals(mi.owner)
                && name.equals(mi.name)
                && desc.equals(mi.desc);
    }
}
