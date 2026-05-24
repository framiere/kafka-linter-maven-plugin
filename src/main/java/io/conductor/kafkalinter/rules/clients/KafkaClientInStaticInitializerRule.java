package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TypeInsnNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Fires when a class's {@code <clinit>} contains a {@code NEW} of any
 * kafka-clients / kafka-streams client type — KafkaProducer, KafkaConsumer,
 * KafkaAdminClient, or KafkaStreams. Static-initializer construction blocks
 * class loading on broker connectivity, leaks background threads and sockets
 * (no symmetric finalizer), and turns any class load — including by tests —
 * into a real Kafka network connection.
 */
public final class KafkaClientInStaticInitializerRule implements Rule {

    private static final String CLINIT_NAME = "<clinit>";
    private static final String CLINIT_DESC = "()V";

    /**
     * Static-factory call sites that produce a Kafka client without a visible
     * {@code NEW} in the caller's bytecode. {@code AdminClient.create(props)}
     * and {@code Admin.create(props)} are the canonical examples — they
     * compile to {@code INVOKESTATIC} on the abstract/interface owner and the
     * {@code NEW KafkaAdminClient} happens inside the factory body, invisible
     * to a caller-side {@code NEW} scan.
     */
    private static final Set<String> ADMIN_FACTORY_OWNERS = Set.of(
            KafkaTypes.ADMIN_CLIENT, KafkaTypes.ADMIN_INTERFACE);
    private static final String CREATE = "create";

    private final Severity severity;

    public KafkaClientInStaticInitializerRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.KAFKA_CLIENT_IN_STATIC_INITIALIZER;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            if (!CLINIT_NAME.equals(mn.name)) continue;
            if (!CLINIT_DESC.equals(mn.desc)) continue;
            for (AbstractInsnNode insn : mn.instructions) {
                String clientName = matchedClient(insn);
                if (clientName == null) continue;
                out.add(new Violation(
                        RuleId.KAFKA_CLIENT_IN_STATIC_INITIALIZER, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        clientName + " constructed inside class's static initializer "
                                + "(<clinit>) — class-loading blocks on broker connectivity, the client "
                                + "has no shutdown path bound to the class lifecycle, and any future class "
                                + "load (including by a test classpath scan) opens real Kafka connections. "
                                + "Move construction into a @Bean / @Produces / @PostConstruct / main() — "
                                + "any scope where the application controls lifecycle and can call close()."));
            }
        }
        return out;
    }

    /**
     * Return the display name of the kafka client instantiated at {@code insn},
     * or {@code null} if {@code insn} is not a client-producing site.
     *
     * <p>Matches two shapes:
     * <ul>
     *   <li>{@code NEW} of a concrete client type in {@link KafkaTypes#KAFKA_CLIENT_TYPES}
     *       (KafkaProducer / KafkaConsumer / KafkaAdminClient / KafkaStreams).</li>
     *   <li>{@code INVOKESTATIC AdminClient.create(...)} or {@code Admin.create(...)} —
     *       the static factory that constructs KafkaAdminClient inside its body.</li>
     * </ul>
     */
    private static String matchedClient(AbstractInsnNode insn) {
        if (insn.getOpcode() == Opcodes.NEW) {
            if (insn instanceof TypeInsnNode tn && KafkaTypes.KAFKA_CLIENT_TYPES.contains(tn.desc)) {
                return clientShortName(tn.desc);
            }
            return null;
        }
        if (insn.getOpcode() == Opcodes.INVOKESTATIC) {
            if (insn instanceof MethodInsnNode mi
                    && ADMIN_FACTORY_OWNERS.contains(mi.owner)
                    && CREATE.equals(mi.name)) {
                return clientShortName(KafkaTypes.ADMIN_CLIENT);
            }
        }
        return null;
    }

    private static String clientShortName(String internal) {
        int slash = internal.lastIndexOf('/');
        return slash < 0 ? internal : internal.substring(slash + 1);
    }
}
