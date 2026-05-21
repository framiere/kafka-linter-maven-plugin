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
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TypeInsnNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Flags methods that construct a Kafka client (Producer, Consumer, or Streams)
 * without ever mentioning the 'client.id' configuration key. Same-method scan
 * only — keeps the heuristic dumb and predictable. The {@code *_CONFIG} String
 * constants from {@code ProducerConfig}/{@code ConsumerConfig}/{@code StreamsConfig}
 * are javac-inlined to the literal {@code "client.id"} at the call site, so both
 * string-literal and constant forms are caught.
 */
public final class ClientIdMissingRule implements Rule {

    private final Severity severity;

    public ClientIdMissingRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CLIENT_ID_MISSING;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            AbstractInsnNode site = findKafkaClientConstruction(mn);
            if (site == null) continue;
            if (methodMentions(mn, KafkaTypes.CLIENT_ID_KEY)) continue;

            String kind = ((TypeInsnNode) site).desc;
            String label;
            if (KafkaTypes.KAFKA_PRODUCER.equals(kind)) label = "KafkaProducer";
            else if (KafkaTypes.KAFKA_CONSUMER.equals(kind)) label = "KafkaConsumer";
            else label = "KafkaStreams";

            out.add(new Violation(
                    RuleId.CLIENT_ID_MISSING,
                    severity,
                    ctx.classNode().name,
                    mn.name,
                    AsmUtil.lineOf(site),
                    label + " is built here but 'client.id' is never set in this method — broker logs and metrics will see an auto-generated 'producer-1' / 'consumer-1'."));
        }
        return out;
    }

    private static AbstractInsnNode findKafkaClientConstruction(MethodNode mn) {
        for (AbstractInsnNode insn : mn.instructions) {
            if (insn.getOpcode() != Opcodes.NEW) continue;
            if (!(insn instanceof TypeInsnNode tn)) continue;
            if (tn.desc.equals(KafkaTypes.KAFKA_PRODUCER)
                    || tn.desc.equals(KafkaTypes.KAFKA_CONSUMER)
                    || tn.desc.equals(KafkaTypes.KAFKA_STREAMS)) {
                return insn;
            }
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
