package io.conductor.kafkalinter.rules.streams;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Fires when {@code KStream.foreach(...)} or {@code KStream.peek(...)} is
 * called with a lambda whose body writes to {@code System.out} or
 * {@code System.err} — the lambda-equivalent of {@code KStream.print()}.
 *
 * <p>Detected shapes:
 * <ul>
 *   <li>Inline lambda: {@code stream.foreach((k, v) -> System.out.println(k))}.
 *       The compiler emits an {@code INVOKEDYNAMIC} returning
 *       {@code ForeachAction} whose impl {@link Handle} points at a synthetic
 *       {@code lambda$X$N} method in the same class. The synthetic body
 *       contains a {@code GETSTATIC java/lang/System.out|err} pair.</li>
 *   <li>Method reference: {@code stream.foreach(System.out::println)}.
 *       Same {@code INVOKEDYNAMIC} shape, but the impl {@link Handle} owner
 *       is {@code java/io/PrintStream} and the name is a
 *       {@code PrintStream} write method.</li>
 * </ul>
 *
 * <p>One {@link Violation} per call site. {@code foreach} and {@code peek}
 * are recognised by owner+name (descriptor-agnostic) so the rule catches
 * both the single-arg overload and the {@code (ForeachAction, Named)}
 * overload added in newer Kafka Streams versions.
 */
public final class StreamsForeachPeekPrintsStdoutRule implements Rule {

    private static final String FOREACH = "foreach";
    private static final String PEEK = "peek";
    private static final String FOREACH_ACTION_DESC =
            "Lorg/apache/kafka/streams/kstream/ForeachAction;";
    private static final String SYSTEM = "java/lang/System";
    private static final String PRINT_STREAM = "java/io/PrintStream";
    private static final Set<String> PRINT_STREAM_WRITE_METHODS = Set.of(
            "println", "print", "printf", "format", "write", "append"
    );

    private final Severity severity;

    public StreamsForeachPeekPrintsStdoutRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_FOREACH_PEEK_PRINTS_STDOUT;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        ClassNode cn = ctx.classNode();
        Map<String, MethodNode> methodsByKey = new HashMap<>();
        for (MethodNode mn : cn.methods) {
            methodsByKey.put(mn.name + mn.desc, mn);
        }

        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : cn.methods) {
            boolean lastForeachActionLambdaIsBad = false;
            boolean lastForeachActionLambdaSeen = false;
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof InvokeDynamicInsnNode indy
                        && isForeachActionLambdaFactory(indy)) {
                    Handle impl = findImplHandle(indy);
                    lastForeachActionLambdaSeen = true;
                    lastForeachActionLambdaIsBad =
                            impl != null && lambdaPrintsToStdout(impl, cn, methodsByKey);
                    continue;
                }
                if (!(insn instanceof MethodInsnNode mi)) continue;
                if (!KafkaTypes.KSTREAM.equals(mi.owner)) continue;
                if (!FOREACH.equals(mi.name) && !PEEK.equals(mi.name)) continue;
                if (!lastForeachActionLambdaSeen) continue;
                if (!lastForeachActionLambdaIsBad) {
                    // consume the pending lambda so a later good->good chain isn't tainted
                    lastForeachActionLambdaSeen = false;
                    continue;
                }
                out.add(new Violation(
                        RuleId.STREAMS_FOREACH_PEEK_PRINTS_STDOUT, severity,
                        cn.name, mn.name, AsmUtil.lineOf(insn),
                        "KStream." + mi.name + "(...) here is called with a lambda whose body "
                                + "writes to System.out or System.err. This is the lambda-equivalent of "
                                + "KStream.print(Printed.toSysOut()): the call site sits inside the topology "
                                + "and runs once per record on a StreamThread. Every record routed through "
                                + "this operator serialises on PrintStream's intrinsic lock (System.out and "
                                + "System.err are line-synchronized), so throughput collapses to the rate one "
                                + "core can println. The output also goes to container stdout, which "
                                + "production log collectors typically truncate at a few MB and which nobody "
                                + "reads. Replace this with one of: peek() that increments a metrics counter, "
                                + "an SLF4J logger gated off at INFO/WARN in production, or to() into a "
                                + "dedicated debug topic. Never inline System.out into a topology operator."));
                lastForeachActionLambdaSeen = false;
                lastForeachActionLambdaIsBad = false;
            }
        }
        return out;
    }

    private static boolean isForeachActionLambdaFactory(InvokeDynamicInsnNode indy) {
        Handle bsm = indy.bsm;
        if (bsm == null) return false;
        if (!"java/lang/invoke/LambdaMetafactory".equals(bsm.getOwner())) return false;
        return indy.desc != null && indy.desc.endsWith(")" + FOREACH_ACTION_DESC);
    }

    private static Handle findImplHandle(InvokeDynamicInsnNode indy) {
        if (indy.bsmArgs == null) return null;
        for (Object arg : indy.bsmArgs) {
            if (arg instanceof Handle h) return h;
        }
        return null;
    }

    private static boolean lambdaPrintsToStdout(
            Handle impl, ClassNode cn, Map<String, MethodNode> methodsByKey) {
        if (PRINT_STREAM.equals(impl.getOwner())
                && PRINT_STREAM_WRITE_METHODS.contains(impl.getName())) {
            // Method-reference shorthand: System.out::println, System.err::println, etc.
            return true;
        }
        if (!cn.name.equals(impl.getOwner())) return false;
        MethodNode lambdaBody = methodsByKey.get(impl.getName() + impl.getDesc());
        if (lambdaBody == null) return false;
        return methodReadsSystemOutOrErr(lambdaBody);
    }

    private static boolean methodReadsSystemOutOrErr(MethodNode mn) {
        for (AbstractInsnNode insn : mn.instructions) {
            if (!(insn instanceof FieldInsnNode fi)) continue;
            if (fi.getOpcode() != Opcodes.GETSTATIC) continue;
            if (!SYSTEM.equals(fi.owner)) continue;
            if ("out".equals(fi.name) || "err".equals(fi.name)) return true;
        }
        return false;
    }
}
