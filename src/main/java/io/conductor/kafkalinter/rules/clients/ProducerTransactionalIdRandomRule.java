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
 * Fires when {@code transactional.id} is set to
 * {@code UUID.randomUUID().toString()} in a Producer config.
 *
 * <h2>Bytecode shape matched</h2>
 *
 * <p>For {@code props.put("transactional.id", UUID.randomUUID().toString())}:
 * <pre>
 *   ALOAD       props
 *   LDC         "transactional.id"      &lt;- key
 *   INVOKESTATIC  java/util/UUID.randomUUID()Ljava/util/UUID;
 *   INVOKEVIRTUAL java/util/UUID.toString()Ljava/lang/String;   &lt;- value
 *   INVOKE      Properties.put / Map.put / setProperty
 * </pre>
 *
 * <p>{@code ProducerConfig.TRANSACTIONAL_ID_CONFIG} is a compile-time
 * {@code String} constant whose value is {@code "transactional.id"} — javac
 * inlines such constants per JLS §15.28, so both source styles compile to the
 * same {@code LDC "transactional.id"} and the rule matches them identically.
 *
 * <h2>Walk strategy</h2>
 *
 * <p>For each {@code put}/{@code setProperty} on a {@link KafkaTypes#CONFIG_HOLDERS}
 * owner, walk backwards via {@link AsmUtil#prevSignificant} through the three
 * preceding significant instructions: toString → randomUUID → key-LDC. If all
 * three match and the key is {@link KafkaTypes#TRANSACTIONAL_ID_KEY}, emit a
 * violation at the {@code randomUUID()} call site (most informative for the
 * engineer).
 */
public final class ProducerTransactionalIdRandomRule implements Rule {

    private static final String UUID_OWNER = "java/util/UUID";
    private static final String RANDOM_UUID = "randomUUID";
    private static final String TO_STRING = "toString";
    private static final String RANDOM_UUID_DESC = "()Ljava/util/UUID;";
    private static final String TO_STRING_DESC = "()Ljava/lang/String;";

    private final Severity severity;

    public ProducerTransactionalIdRandomRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_TRANSACTIONAL_ID_RANDOM;
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
                if (!KafkaTypes.TRANSACTIONAL_ID_KEY.equals(keyL.cst)) continue;

                out.add(new Violation(
                        RuleId.PRODUCER_TRANSACTIONAL_ID_RANDOM, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(v2),
                        "transactional.id = UUID.randomUUID().toString() — a random per-process id defeats "
                                + "Kafka's zombie-producer fencing. On crash recovery, the replacement registers "
                                + "under a brand-new id, the broker has no prior state for it, no epoch bump "
                                + "happens, and any zombie producer from the prior incarnation keeps committing "
                                + "transactions under its old id; read_committed consumers downstream see BOTH "
                                + "timelines as legitimate, so 'exactly once' becomes 'duplicated on every "
                                + "restart'. Use an orchestrator-stable identity instead: pod-name, application-id "
                                + "+ partition-id, or (for Kafka Streams) set processing.guarantee=exactly_once_v2 "
                                + "and let Streams derive task-scoped ids."));
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
