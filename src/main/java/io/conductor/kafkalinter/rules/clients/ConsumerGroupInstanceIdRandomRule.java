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
 * Fires when {@code group.instance.id} is set to {@code UUID.randomUUID().toString()}
 * in a Consumer config.
 *
 * <h2>Why this is dangerous</h2>
 *
 * <p>{@code group.instance.id} is the opt-in to KIP-345 static membership: a stable,
 * application-supplied identity that the broker recognises across pod restarts and uses
 * to skip the group rebalance protocol entirely. The whole point is that the SAME
 * instance ID, presented by a restarted consumer within {@code session.timeout.ms},
 * returns the previous partition assignment unchanged — no rebalance, no offset commit
 * storm, no state-store warm-up. {@code UUID.randomUUID().toString()} guarantees the
 * opposite: every restart produces a NEW string, the broker sees the new ID as a brand
 * new member, the old ID waits out its session timeout as a phantom, and a full
 * rebalance fires — exactly what static membership was configured to avoid.
 *
 * <p>The result is strictly worse than the dynamic-membership baseline: the application
 * pays the protocol cost of static membership (extra coordinator state, typically a
 * longer {@code session.timeout.ms} to absorb deploy windows) AND the rebalance cost on
 * every restart. There is no scenario in which random {@code group.instance.id} is
 * correct.
 *
 * <h2>Bytecode shape matched</h2>
 *
 * <p>For {@code props.put("group.instance.id", UUID.randomUUID().toString())}:
 * <pre>
 *   ALOAD       props
 *   LDC         "group.instance.id"         &lt;- key
 *   INVOKESTATIC  java/util/UUID.randomUUID()Ljava/util/UUID;
 *   INVOKEVIRTUAL java/util/UUID.toString()Ljava/lang/String;   &lt;- value
 *   INVOKE      Properties.put / Map.put / setProperty
 * </pre>
 *
 * <p>{@code ConsumerConfig.GROUP_INSTANCE_ID_CONFIG} is a compile-time {@code String}
 * constant whose value is {@code "group.instance.id"} — javac inlines such constants
 * per JLS §15.28, so both source styles compile to the same {@code LDC} and the rule
 * matches them identically.
 *
 * <h2>Walk strategy</h2>
 *
 * <p>For each {@code put}/{@code setProperty} on a {@link KafkaTypes#CONFIG_HOLDERS}
 * owner, walk backwards via {@link AsmUtil#prevSignificant} through the three
 * preceding significant instructions: toString → randomUUID → key-LDC. If all three
 * match and the key is {@link KafkaTypes#GROUP_INSTANCE_ID_KEY}, emit a violation at
 * the {@code randomUUID()} call site.
 */
public final class ConsumerGroupInstanceIdRandomRule implements Rule {

    private static final String UUID_OWNER = "java/util/UUID";
    private static final String RANDOM_UUID = "randomUUID";
    private static final String TO_STRING = "toString";
    private static final String RANDOM_UUID_DESC = "()Ljava/util/UUID;";
    private static final String TO_STRING_DESC = "()Ljava/lang/String;";

    private final Severity severity;

    public ConsumerGroupInstanceIdRandomRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_GROUP_INSTANCE_ID_RANDOM;
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
                if (!KafkaTypes.GROUP_INSTANCE_ID_KEY.equals(keyL.cst)) continue;

                out.add(new Violation(
                        RuleId.CONSUMER_GROUP_INSTANCE_ID_RANDOM, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(v2),
                        "group.instance.id = UUID.randomUUID().toString() — KIP-345 static membership "
                                + "requires a STABLE identity across restarts so the broker can skip the "
                                + "group rebalance protocol. A random per-process ID makes every restart "
                                + "look like a brand-new member to the coordinator: the previous ID waits "
                                + "out its session timeout (typically 45s on a static-membership group) "
                                + "as a phantom, the new ID triggers a full rebalance — exactly what "
                                + "static membership exists to prevent. Net: the consumer pays the "
                                + "protocol cost of static membership with NONE of the benefit, strictly "
                                + "worse than leaving group.instance.id unset. Replace with a stable "
                                + "per-pod identity: ${POD_NAME} from the Kubernetes downward API, "
                                + "${HOSTNAME} on bare-metal, or an orchestrator-assigned per-replica "
                                + "index. If a stable identity is unavailable, drop group.instance.id "
                                + "entirely and use dynamic membership."));
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
