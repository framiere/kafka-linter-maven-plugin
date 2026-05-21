package io.conductor.kafkalinter.rules.spring;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Flags Java-code calls of the form
 * {@code jsonDeserializer.addTrustedPackages("*")} /
 * {@code jsonDeserializer.trustedPackages("*")} (fluent setter) and the same
 * on {@code DefaultJackson2JavaTypeMapper} — the code-level twin of the
 * property-level {@code spring.json.trusted.packages=*} anti-pattern.
 *
 * <p>The varargs {@code String...} argument compiles to
 * {@code ANEWARRAY String / DUP / ICONST_i / LDC value / AASTORE} per element.
 * The detector walks backward from each matching invoke and inspects every
 * {@code LDC} that immediately precedes an {@code AASTORE} between the call
 * site and the array allocation; if any of those LDC constants equals
 * {@code "*"}, the call is reported.
 */
public final class SpringJsonDeserializerTrustedPackagesWildcardRule implements Rule {

    private static final String JSON_DESERIALIZER = "org/springframework/kafka/support/serializer/JsonDeserializer";
    private static final String TYPE_MAPPER = "org/springframework/kafka/support/mapping/DefaultJackson2JavaTypeMapper";
    private static final String ABSTRACT_TYPE_MAPPER = "org/springframework/kafka/support/mapping/AbstractJavaTypeMapper";

    private static final Set<String> TARGET_OWNERS = Set.of(JSON_DESERIALIZER, TYPE_MAPPER, ABSTRACT_TYPE_MAPPER);

    // addTrustedPackages: void return — descriptor ([Ljava/lang/String;)V
    // trustedPackages   : fluent setter — descriptor ([Ljava/lang/String;)Lorg/springframework/kafka/support/serializer/JsonDeserializer;
    private static final String ADD_TRUSTED_DESC = "([Ljava/lang/String;)V";
    private static final String FLUENT_TRUSTED_DESC = "([Ljava/lang/String;)L" + JSON_DESERIALIZER + ";";

    private final Severity severity;

    public SpringJsonDeserializerTrustedPackagesWildcardRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SPRING_JSON_DESERIALIZER_TRUSTED_PACKAGES_WILDCARD;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (mi.getOpcode() != Opcodes.INVOKEVIRTUAL) continue;
                if (!TARGET_OWNERS.contains(mi.owner)) continue;
                boolean isAdd = "addTrustedPackages".equals(mi.name) && ADD_TRUSTED_DESC.equals(mi.desc);
                boolean isFluent = "trustedPackages".equals(mi.name) && FLUENT_TRUSTED_DESC.equals(mi.desc);
                if (!isAdd && !isFluent) continue;
                if (!argArrayContainsWildcard(mi)) continue;

                String simpleOwner = mi.owner.substring(mi.owner.lastIndexOf('/') + 1);
                out.add(new Violation(
                        RuleId.SPRING_JSON_DESERIALIZER_TRUSTED_PACKAGES_WILDCARD, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        simpleOwner + "." + mi.name + "(..., \"*\", ...) — wildcard trust lets a producer load any FQCN via the __TypeId__ header. "
                                + "Replace with an explicit allow-list of your own packages, e.g. "
                                + "`addTrustedPackages(\"com.acme.events\", \"com.acme.shared\")`."));
            }
        }
        return out;
    }

    /**
     * Walks backward from the invoke until it hits the {@code ANEWARRAY} that built the
     * varargs array, inspecting every {@code LDC} whose next-significant instruction is
     * {@code AASTORE} (i.e. the LDC is the value being stored). Returns true if any such
     * LDC's constant equals {@code "*"}.
     */
    private static boolean argArrayContainsWildcard(MethodInsnNode call) {
        AbstractInsnNode cursor = AsmUtil.prevSignificant(call);
        while (cursor != null) {
            if (cursor.getOpcode() == Opcodes.ANEWARRAY) return false;
            if (cursor instanceof LdcInsnNode ldc && "*".equals(ldc.cst)) {
                AbstractInsnNode next = AsmUtil.nextSignificant(ldc);
                if (next != null && next.getOpcode() == Opcodes.AASTORE) return true;
            }
            cursor = AsmUtil.prevSignificant(cursor);
        }
        return false;
    }
}
