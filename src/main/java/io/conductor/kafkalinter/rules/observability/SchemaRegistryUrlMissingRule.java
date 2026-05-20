package io.conductor.kafkalinter.rules.observability;

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
import org.objectweb.asm.tree.TypeInsnNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Flags a class that constructs a Confluent schema-registry (de)serializer but does not
 * configure {@code schema.registry.url} anywhere in the same class.
 *
 * <p>The check is class-local: we look for {@code NEW} on one of the SR serializer types
 * and verify that some {@code Properties.put("schema.registry.url", ...)} appears in the
 * same class. If not, the rule fires on the NEW site.
 *
 * <p>This will miss the case where the URL is configured in a different class than the
 * one constructing the serializer (e.g. a shared config factory). Project-wide checking
 * would catch more — at the cost of false positives when the URL comes from external
 * config (Spring property binding). The class-local form is the high-confidence subset.
 */
public final class SchemaRegistryUrlMissingRule implements Rule {

    private final Severity severity;

    public SchemaRegistryUrlMissingRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SCHEMA_REGISTRY_URL_MISSING;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        boolean configured = classConfiguresSchemaRegistryUrl(ctx);
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof TypeInsnNode tn)) continue;
                if (tn.getOpcode() != 0xBB /* NEW */) continue;
                if (!KafkaTypes.SR_SERIALIZER_OWNERS.contains(tn.desc)) continue;
                if (configured) continue;
                out.add(new Violation(
                        RuleId.SCHEMA_REGISTRY_URL_MISSING, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "new " + tn.desc.substring(tn.desc.lastIndexOf('/') + 1)
                                + "() but no schema.registry.url config set in this class — serializer cannot resolve schemas."));
            }
        }
        return out;
    }

    private static boolean classConfiguresSchemaRegistryUrl(RuleContext ctx) {
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.CONFIG_HOLDERS.contains(mi.owner)) continue;
                if (!KafkaTypes.CONFIG_PUT_METHODS.contains(mi.name)) continue;
                AbstractInsnNode valueLdc = AsmUtil.prevSignificant(insn);
                AbstractInsnNode keyLdc = AsmUtil.prevSignificant(valueLdc);
                if (!(keyLdc instanceof LdcInsnNode keyL)) continue;
                if (KafkaTypes.SCHEMA_REGISTRY_URL_KEY.equals(keyL.cst)) return true;
            }
        }
        return false;
    }
}
