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
 * Fires when {@code application.id} is set to
 * {@code UUID.randomUUID().toString()} in a Kafka Streams config.
 *
 * <h2>Why this is dangerous</h2>
 *
 * <p>In Kafka Streams, {@code application.id} is not "just an identifier" — it
 * is the single value that simultaneously becomes:
 *
 * <ul>
 *   <li>the {@code group.id} of all source consumers, so the broker uses it to
 *       track committed offsets;</li>
 *   <li>the prefix of every internal repartition/changelog topic the topology
 *       creates;</li>
 *   <li>the prefix of every EOS-v2 producer's {@code transactional.id}, which
 *       is what the broker fences zombies against;</li>
 *   <li>the directory name under {@code state.dir} where RocksDB state stores
 *       are materialized.</li>
 * </ul>
 *
 * <p>Randomizing it per process makes every restart look like a brand-new
 * application: no committed offsets (so {@code auto.offset.reset=latest}
 * silently drops everything between crash and restart), empty state directory
 * (so changelog topics must be fully replayed from earliest — minutes to hours
 * of warm-up), orphan internal topics accreting on the broker, and broken EOS
 * fencing (no epoch bump because the broker has never seen this txn id).
 *
 * <h2>Bytecode shape matched</h2>
 *
 * <p>For {@code props.put("application.id", UUID.randomUUID().toString())}:
 * <pre>
 *   ALOAD       props
 *   LDC         "application.id"        &lt;- key
 *   INVOKESTATIC  java/util/UUID.randomUUID()Ljava/util/UUID;
 *   INVOKEVIRTUAL java/util/UUID.toString()Ljava/lang/String;   &lt;- value
 *   INVOKE      Properties.put / Map.put / setProperty
 * </pre>
 *
 * <p>{@code StreamsConfig.APPLICATION_ID_CONFIG} is a compile-time
 * {@code String} constant whose value is {@code "application.id"} — javac
 * inlines such constants per JLS §15.28, so both source styles compile to the
 * same {@code LDC "application.id"} and the rule matches them identically.
 *
 * <h2>Walk strategy</h2>
 *
 * <p>For each {@code put}/{@code setProperty} on a {@link KafkaTypes#CONFIG_HOLDERS}
 * owner, walk backwards via {@link AsmUtil#prevSignificant} through the three
 * preceding significant instructions: toString → randomUUID → key-LDC. If all
 * three match and the key is {@link KafkaTypes#STREAMS_APPLICATION_ID_KEY},
 * emit a violation at the {@code randomUUID()} call site (most informative for
 * the engineer).
 */
public final class StreamsApplicationIdRandomRule implements Rule {

    private static final String UUID_OWNER = "java/util/UUID";
    private static final String RANDOM_UUID = "randomUUID";
    private static final String TO_STRING = "toString";
    private static final String RANDOM_UUID_DESC = "()Ljava/util/UUID;";
    private static final String TO_STRING_DESC = "()Ljava/lang/String;";

    private final Severity severity;

    public StreamsApplicationIdRandomRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_APPLICATION_ID_RANDOM;
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
                if (!KafkaTypes.STREAMS_APPLICATION_ID_KEY.equals(keyL.cst)) continue;

                out.add(new Violation(
                        RuleId.STREAMS_APPLICATION_ID_RANDOM, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(v2),
                        "application.id = UUID.randomUUID().toString() — a random per-process id "
                                + "destroys every Streams identity at once: (1) the consumer group becomes "
                                + "brand-new so there are no committed offsets and auto.offset.reset=latest "
                                + "silently drops every record between crash and restart; (2) the state "
                                + "directory is empty so changelog topics must be replayed from earliest, "
                                + "blocking the app for minutes-to-hours of warm-up; (3) internal "
                                + "repartition/changelog topics accrete forever on the broker because each "
                                + "restart creates a new prefix and nothing ever deletes the old ones; "
                                + "(4) EOS-v2 fencing is broken — the broker never sees a prior "
                                + "transactional.id so no epoch bump occurs and zombies keep committing. "
                                + "Use a stable orchestrator-assigned id (deployment name, helm release, "
                                + "etc.) and version it explicitly when the topology changes "
                                + "incompatibly — never derive it from a random source."));
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
