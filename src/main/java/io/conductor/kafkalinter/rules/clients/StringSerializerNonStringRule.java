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
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TypeInsnNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Flags {@code new ProducerRecord<>(...)} calls whose value argument is statically NOT a
 * String, when the class also configures {@code value.serializer = StringSerializer}.
 *
 * <p>{@code StringSerializer.serialize(topic, T data)} returns {@code data.toString().getBytes(charset)}.
 * With a non-String value, the payload becomes the accidental {@code Object.toString()} — typically
 * {@code "[B@5e9f23b4"} for {@code byte[]} or {@code "DomainObject{id=...}"} for a generated record.
 *
 * <p>The rule is conservative: it only fires when the value's producing instruction has a
 * statically-known non-String type (method return type, field descriptor, array creation).
 * For {@code ALOAD} parameters / locals we cannot determine the type without a full analyzer,
 * so those are not flagged — accepted as a false-negative.
 */
public final class StringSerializerNonStringRule implements Rule {

    private static final String PRODUCER_RECORD = KafkaTypes.PRODUCER_RECORD;
    private static final String STRING_SERIALIZER_FQCN = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String STRING_SERIALIZER_INTERNAL = "org/apache/kafka/common/serialization/StringSerializer";
    private static final String STRING_DESC = "Ljava/lang/String;";
    private static final String ITERABLE_DESC = "Ljava/lang/Iterable;";

    private final Severity severity;

    public StringSerializerNonStringRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STRING_SERIALIZER_NON_STRING;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        ClassNode cn = ctx.classNode();
        if (!classConfiguresStringValueSerializer(cn)) return List.of();

        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : cn.methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (mi.getOpcode() != Opcodes.INVOKESPECIAL) continue;
                if (!"<init>".equals(mi.name)) continue;
                if (!PRODUCER_RECORD.equals(mi.owner)) continue;

                int distanceFromTop = valueArgDistanceFromTop(mi.desc);
                if (distanceFromTop < 0) continue;

                AbstractInsnNode producer = findArgProducer(mi, distanceFromTop);
                if (producer == null) continue;
                if (!provablyNonString(producer)) continue;

                out.add(new Violation(
                        RuleId.STRING_SERIALIZER_NON_STRING, severity,
                        cn.name, mn.name, AsmUtil.lineOf(mi),
                        "value.serializer = StringSerializer is configured in this class but a "
                                + "ProducerRecord is constructed with a non-String value (" + describe(producer)
                                + "). StringSerializer calls .toString() on the value — non-String "
                                + "payloads ship as their accidental Object.toString() (`[B@…` for byte[], "
                                + "`DomainObject{…}` for generated records). Match the serializer to the "
                                + "actual value type, or serialize at the application layer."));
            }
        }
        return out;
    }

    private static boolean classConfiguresStringValueSerializer(ClassNode cn) {
        for (MethodNode mn : cn.methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.CONFIG_HOLDERS.contains(mi.owner)) continue;
                if (!KafkaTypes.CONFIG_PUT_METHODS.contains(mi.name)) continue;
                AbstractInsnNode valueInsn = AsmUtil.prevSignificant(mi);
                if (valueInsn == null) continue;
                AbstractInsnNode keyInsn = AsmUtil.prevSignificant(valueInsn);
                if (keyInsn == null) continue;
                if (!(keyInsn instanceof LdcInsnNode keyLdc)) continue;
                if (!"value.serializer".equals(keyLdc.cst)) continue;
                if (isStringSerializerLdc(valueInsn)) return true;
            }
        }
        return false;
    }

    private static boolean isStringSerializerLdc(AbstractInsnNode insn) {
        if (!(insn instanceof LdcInsnNode ldc)) return false;
        Object cst = ldc.cst;
        if (cst instanceof String s) {
            return STRING_SERIALIZER_FQCN.equals(s);
        }
        if (cst instanceof Type t) {
            return STRING_SERIALIZER_INTERNAL.equals(t.getInternalName());
        }
        return false;
    }

    private static int valueArgDistanceFromTop(String desc) {
        Type[] args = Type.getArgumentTypes(desc);
        if (args.length < 2) return -1;
        boolean lastIsIterable = ITERABLE_DESC.equals(args[args.length - 1].getDescriptor());
        return lastIsIterable ? 2 : 1;
    }

    private static AbstractInsnNode findArgProducer(MethodInsnNode call, int distanceFromTop) {
        int needed = distanceFromTop;
        AbstractInsnNode cursor = AsmUtil.prevSignificant(call);
        while (cursor != null) {
            int[] eff = AsmUtil.stackEffect(cursor);
            if (eff == null) return null;
            int op = cursor.getOpcode();
            boolean isDup = op == Opcodes.DUP || op == Opcodes.DUP_X1 || op == Opcodes.DUP_X2
                    || op == Opcodes.DUP2 || op == Opcodes.DUP2_X1 || op == Opcodes.DUP2_X2;
            if (needed <= eff[1] && !isDup) {
                return cursor;
            }
            needed -= eff[1];
            needed += eff[0];
            cursor = AsmUtil.prevSignificant(cursor);
        }
        return null;
    }

    private static boolean provablyNonString(AbstractInsnNode producer) {
        int op = producer.getOpcode();
        if (op == Opcodes.ACONST_NULL) return false;
        if (producer instanceof LdcInsnNode ldc) {
            return !(ldc.cst instanceof String);
        }
        if (producer instanceof MethodInsnNode mi) {
            Type ret = Type.getReturnType(mi.desc);
            return ret.getSort() == Type.OBJECT || ret.getSort() == Type.ARRAY
                    ? !STRING_DESC.equals(ret.getDescriptor())
                    : false;
        }
        if (producer instanceof FieldInsnNode fi
                && (op == Opcodes.GETFIELD || op == Opcodes.GETSTATIC)) {
            return !STRING_DESC.equals(fi.desc);
        }
        if (producer instanceof TypeInsnNode tn) {
            if (op == Opcodes.ANEWARRAY || op == Opcodes.NEW) {
                return !"java/lang/String".equals(tn.desc);
            }
            if (op == Opcodes.CHECKCAST) {
                return !"java/lang/String".equals(tn.desc);
            }
        }
        if (op == Opcodes.NEWARRAY || op == Opcodes.MULTIANEWARRAY) return true;
        return false;
    }

    private static String describe(AbstractInsnNode producer) {
        if (producer instanceof MethodInsnNode mi) {
            Type ret = Type.getReturnType(mi.desc);
            return ret.getClassName() + " from " + mi.owner.substring(mi.owner.lastIndexOf('/') + 1) + "#" + mi.name;
        }
        if (producer instanceof FieldInsnNode fi) {
            return Type.getType(fi.desc).getClassName() + " from field " + fi.owner.substring(fi.owner.lastIndexOf('/') + 1) + "#" + fi.name;
        }
        if (producer instanceof TypeInsnNode tn) {
            return tn.desc;
        }
        if (producer instanceof LdcInsnNode ldc && ldc.cst instanceof Type t) {
            return t.getClassName();
        }
        if (producer.getOpcode() == Opcodes.NEWARRAY || producer.getOpcode() == Opcodes.MULTIANEWARRAY) {
            return "array";
        }
        return "non-String value";
    }
}
