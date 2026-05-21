package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.VarInsnNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Flags {@code Properties.put} / {@code Map.put} on a local-variable slot AFTER that slot
 * was passed to {@code new KafkaProducer(...)} or {@code new KafkaConsumer(...)}. The
 * constructor copies the config into an immutable internal {@code AbstractConfig}; the
 * post-constructor mutation has no effect on the live client.
 */
public final class PropertiesMutatedAfterCtorRule implements Rule {

    private static final Set<String> CTOR_OWNERS = Set.of(KafkaTypes.KAFKA_PRODUCER, KafkaTypes.KAFKA_CONSUMER);
    private static final String PROPERTIES_DESC = "Ljava/util/Properties;";
    private static final String MAP_DESC = "Ljava/util/Map;";

    private final Severity severity;

    public PropertiesMutatedAfterCtorRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PROPERTIES_MUTATED_AFTER_CTOR;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            scanMethod(out, ctx, mn);
        }
        return out;
    }

    private void scanMethod(List<Violation> out, RuleContext ctx, MethodNode mn) {
        Map<Integer, String> frozenSlots = new HashMap<>();
        for (AbstractInsnNode insn : mn.instructions) {
            if (!(insn instanceof MethodInsnNode mi)) continue;

            if (mi.getOpcode() == Opcodes.INVOKESPECIAL
                    && "<init>".equals(mi.name)
                    && CTOR_OWNERS.contains(mi.owner)) {
                Integer slot = firstArgSlotForCtor(mi);
                if (slot != null) frozenSlots.put(slot, mi.owner);
                continue;
            }

            if (frozenSlots.isEmpty()) continue;
            if (!KafkaTypes.CONFIG_HOLDERS.contains(mi.owner)) continue;
            if (!KafkaTypes.CONFIG_PUT_METHODS.contains(mi.name)) continue;
            Integer recv = receiverSlotForPutCall(mi);
            if (recv == null) continue;
            String ctorOwner = frozenSlots.get(recv);
            if (ctorOwner == null) continue;

            String ctorSimple = ctorOwner.substring(ctorOwner.lastIndexOf('/') + 1);
            out.add(new Violation(
                    RuleId.PROPERTIES_MUTATED_AFTER_CTOR, severity,
                    ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                    mi.name + "(...) on a Properties/Map that was already passed to new "
                            + ctorSimple
                            + "(...). The constructor copies the config — this mutation has no effect on the live client. Finish configuring before constructing, or use a fresh Properties."));
        }
    }

    /**
     * Identify the local slot that supplied the FIRST argument (Properties or Map) of a Kafka
     * client constructor. Conservative — only handles the canonical single-arg form
     * {@code (Properties)V} / {@code (Map)V} where the previous significant insn is an ALOAD.
     */
    private static Integer firstArgSlotForCtor(MethodInsnNode invokeSpecial) {
        Type[] params = Type.getArgumentTypes(invokeSpecial.desc);
        if (params.length != 1) return null;
        String first = params[0].getDescriptor();
        if (!PROPERTIES_DESC.equals(first) && !MAP_DESC.equals(first)) return null;
        AbstractInsnNode prev = AsmUtil.prevSignificant(invokeSpecial);
        if (prev instanceof VarInsnNode v && v.getOpcode() == Opcodes.ALOAD) return v.var;
        return null;
    }

    /**
     * Identify the local slot that supplied the receiver of a {@code put}/{@code setProperty}
     * call. The pre-call stack is [receiver, key, value]; we walk back two push-one-slot
     * instructions and expect an ALOAD. Returns null when the form is not the canonical
     * {@code aload N; ldc key; ldc value; invokevirtual put}.
     */
    private static Integer receiverSlotForPutCall(MethodInsnNode put) {
        AbstractInsnNode valuePush = AsmUtil.prevSignificant(put);
        if (valuePush == null) return null;
        AbstractInsnNode keyPush = AsmUtil.prevSignificant(valuePush);
        if (keyPush == null) return null;
        AbstractInsnNode receiverLoad = AsmUtil.prevSignificant(keyPush);
        if (receiverLoad instanceof VarInsnNode v && v.getOpcode() == Opcodes.ALOAD) return v.var;
        return null;
    }
}
