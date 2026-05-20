package io.conductor.kafkalinter.rules.observability;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Flags any call to {@code ObjectMapper.activateDefaultTyping(...)} or
 * {@code ObjectMapper.enableDefaultTyping(...)} — the Jackson polymorphic-typing
 * footgun that has produced RCE-class CVEs across the ecosystem.
 *
 * <p>The lint cannot tell whether the caller passes a {@code PolymorphicTypeValidator}
 * with an allow-list, so the rule fires regardless. The fix is either to remove the
 * call entirely (use explicit {@code @JsonSubTypes}) or audit the validator carefully.
 */
public final class JacksonDefaultTypingRule implements Rule {

    private final Severity severity;

    public JacksonDefaultTypingRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.DESER_JSON_TYPE_INFO_NO_ALLOWLIST;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.OBJECT_MAPPER.equals(mi.owner)) continue;
                if (!KafkaTypes.JACKSON_ACTIVATE_DEFAULT_TYPING_METHOD.equals(mi.name)
                        && !KafkaTypes.JACKSON_ENABLE_DEFAULT_TYPING_METHOD.equals(mi.name)) {
                    continue;
                }
                out.add(new Violation(
                        RuleId.DESER_JSON_TYPE_INFO_NO_ALLOWLIST, severity,
                        ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                        "ObjectMapper." + mi.name + "(...) — Jackson polymorphic typing. Even with a validator, this is a deserialization-RCE class footgun on Kafka records."));
            }
        }
        return out;
    }
}
