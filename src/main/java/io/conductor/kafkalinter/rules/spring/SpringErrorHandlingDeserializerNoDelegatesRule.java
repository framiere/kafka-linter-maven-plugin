package io.conductor.kafkalinter.rules.spring;

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
 * Flags a class that configures {@code ErrorHandlingDeserializer} as key/value deserializer
 * without also setting {@code spring.deserializer.value.delegate.class} or
 * {@code spring.deserializer.key.delegate.class}. Without a delegate the wrapper has
 * nothing to delegate to and every record fails to deserialize.
 */
public final class SpringErrorHandlingDeserializerNoDelegatesRule implements Rule {

    private static final String EHD_FQCN = "org.springframework.kafka.support.serializer.ErrorHandlingDeserializer";
    private static final String VALUE_DELEGATE_KEY = "spring.deserializer.value.delegate.class";
    private static final String KEY_DELEGATE_KEY = "spring.deserializer.key.delegate.class";
    private static final String VALUE_DESER_KEY = "value.deserializer";
    private static final String KEY_DESER_KEY = "key.deserializer";

    private final Severity severity;

    public SpringErrorHandlingDeserializerNoDelegatesRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SPRING_ERROR_HANDLING_DESERIALIZER_NO_DELEGATES;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        boolean ehdAsValue = false;
        boolean ehdAsKey = false;
        boolean valueDelegateSet = false;
        boolean keyDelegateSet = false;
        AbstractInsnNode anchor = null;
        String anchorMethod = null;

        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.CONFIG_HOLDERS.contains(mi.owner)) continue;
                if (!KafkaTypes.CONFIG_PUT_METHODS.contains(mi.name)) continue;

                AbstractInsnNode valueLdc = AsmUtil.prevSignificant(insn);
                AbstractInsnNode keyLdc = AsmUtil.prevSignificant(valueLdc);
                if (!(keyLdc instanceof LdcInsnNode keyL)) continue;
                if (!(keyL.cst instanceof String key)) continue;

                String val = valueLdc instanceof LdcInsnNode vL ? String.valueOf(vL.cst) : null;

                if (VALUE_DESER_KEY.equals(key) && val != null && val.contains(EHD_FQCN)) {
                    ehdAsValue = true;
                    if (anchor == null) { anchor = insn; anchorMethod = mn.name; }
                } else if (KEY_DESER_KEY.equals(key) && val != null && val.contains(EHD_FQCN)) {
                    ehdAsKey = true;
                    if (anchor == null) { anchor = insn; anchorMethod = mn.name; }
                } else if (VALUE_DELEGATE_KEY.equals(key)) {
                    valueDelegateSet = true;
                } else if (KEY_DELEGATE_KEY.equals(key)) {
                    keyDelegateSet = true;
                }
            }
        }

        List<Violation> out = new ArrayList<>();
        if (ehdAsValue && !valueDelegateSet && anchor != null) {
            out.add(new Violation(
                    RuleId.SPRING_ERROR_HANDLING_DESERIALIZER_NO_DELEGATES, severity,
                    ctx.classNode().name, anchorMethod, AsmUtil.lineOf(anchor),
                    "value.deserializer=ErrorHandlingDeserializer without " + VALUE_DELEGATE_KEY + " — wrapper has no delegate."));
        }
        if (ehdAsKey && !keyDelegateSet && anchor != null) {
            out.add(new Violation(
                    RuleId.SPRING_ERROR_HANDLING_DESERIALIZER_NO_DELEGATES, severity,
                    ctx.classNode().name, anchorMethod, AsmUtil.lineOf(anchor),
                    "key.deserializer=ErrorHandlingDeserializer without " + KEY_DELEGATE_KEY + " — wrapper has no delegate."));
        }
        return out;
    }
}
