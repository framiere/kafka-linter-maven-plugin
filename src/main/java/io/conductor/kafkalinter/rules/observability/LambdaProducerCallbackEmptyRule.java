package io.conductor.kafkalinter.rules.observability;

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
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Fires when {@code producer.send(record, (md, ex) -> {})} is called with a
 * lambda Callback whose body is empty — runtime semantics are identical to
 * {@code send(record, null)} (every sender-thread failure is silently
 * swallowed) but the source-level shape looks deliberate: a reviewer sees the
 * {@code (metadata, exception)} parameter pair and assumes error handling was
 * considered.
 *
 * <h2>Bytecode shape</h2>
 *
 * <p>For {@code producer.send(record, (md, ex) -> {})}, javac emits:
 * <pre>
 *   ALOAD producer
 *   ... build record ...
 *   INVOKEDYNAMIC onCompletion()Lorg/apache/kafka/clients/producer/Callback;
 *                 bsm = java/lang/invoke/LambdaMetafactory.metafactory(...)
 *                 bsmArgs[1] = Handle REF_invokeStatic
 *                              outer$class.lambda$&lt;outerMethod&gt;$N(RecordMetadata, Exception)V
 *   INVOKEVIRTUAL/INVOKEINTERFACE Producer.send(ProducerRecord, Callback)Future
 * </pre>
 *
 * <p>The synthetic method {@code lambda$<outerMethodName>$<N>}, after stripping
 * {@code LabelNode} / {@code LineNumberNode} / {@code FrameNode} trivia, holds
 * exactly one significant instruction: {@code RETURN}. That is the signature
 * this detector matches.
 *
 * <h2>Out of scope by design</h2>
 *
 * <ul>
 *   <li>Method references — {@code producer.send(record, this::onSend)} —
 *       the indy's impl handle points at a real method (not a
 *       {@code lambda$...} synthetic). Could be empty but is not the
 *       same source-level anti-pattern.</li>
 *   <li>Instance-field callbacks — {@code producer.send(record,
 *       this.onSendCallback)} — preceding insn is {@code GETFIELD}, not
 *       {@code INVOKEDYNAMIC}.</li>
 *   <li>Lambdas with non-empty bodies that nevertheless ignore the
 *       exception parameter (e.g. {@code (md, ex) -> log.info("sent {}", md)})
 *       — requires slot-flow analysis past lambda-capture offsets; deferred
 *       to a separate rule.</li>
 * </ul>
 *
 * <p>Keeping the detector strictly on the empty-body shape preserves HIGH
 * confidence: the bytecode signal is unambiguous, and the only way to
 * produce it from Java source is to write {@code (md, ex) -> {}}.
 */
public final class LambdaProducerCallbackEmptyRule implements Rule {

    private static final String LAMBDA_METAFACTORY = "java/lang/invoke/LambdaMetafactory";
    private static final String LAMBDA_NAME_PREFIX = "lambda$";

    private final Severity severity;

    public LambdaProducerCallbackEmptyRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.LAMBDA_PRODUCER_CALLBACK_EMPTY;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        ClassNode cn = ctx.classNode();
        for (MethodNode mn : cn.methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (!isProducerSendWithCallback(insn)) continue;

                AbstractInsnNode prev = AsmUtil.prevSignificant(insn);
                if (!(prev instanceof InvokeDynamicInsnNode indy)) continue;
                if (indy.bsm == null || !LAMBDA_METAFACTORY.equals(indy.bsm.getOwner())) continue;

                Handle impl = AsmUtil.indyTargetHandle(indy, null, null, null);
                if (impl == null) continue;
                if (!cn.name.equals(impl.getOwner())) continue;
                if (!impl.getName().startsWith(LAMBDA_NAME_PREFIX)) continue;

                MethodNode body = findMethod(cn, impl.getName(), impl.getDesc());
                if (body == null) continue;
                if (!isEmptyBody(body)) continue;

                out.add(new Violation(
                        RuleId.LAMBDA_PRODUCER_CALLBACK_EMPTY, severity,
                        cn.name, mn.name, AsmUtil.lineOf(insn),
                        "producer.send(record, (md, ex) -> {}) — Callback lambda body is empty. "
                                + "Identical runtime semantics to send(record, null): every sender-thread "
                                + "failure (serialization error, broker NACK, retry exhaustion, idempotence "
                                + "gap, transactional fencing, delivery.timeout.ms expiry) is silently "
                                + "swallowed. The source LOOKS like a real callback was supplied — reviewers "
                                + "see the (metadata, exception) pair and assume error handling exists. "
                                + "Replace the body with `if (ex != null) { log.error(\"send failed\", ex); "
                                + "errorMeter.increment(); }`, or drop the second argument so fire-and-forget "
                                + "is syntactically explicit at the call site."));
            }
        }
        return out;
    }

    private static boolean isProducerSendWithCallback(AbstractInsnNode insn) {
        if (!(insn instanceof MethodInsnNode mi)) return false;
        if (mi.getOpcode() != Opcodes.INVOKEVIRTUAL && mi.getOpcode() != Opcodes.INVOKEINTERFACE) return false;
        if (!KafkaTypes.PRODUCER_OWNERS.contains(mi.owner)) return false;
        if (!"send".equals(mi.name)) return false;
        return AsmUtil.lastArgIsCallback(mi.desc);
    }

    private static MethodNode findMethod(ClassNode cn, String name, String desc) {
        for (MethodNode mn : cn.methods) {
            if (mn.name.equals(name) && mn.desc.equals(desc)) {
                return mn;
            }
        }
        return null;
    }

    private static boolean isEmptyBody(MethodNode body) {
        AbstractInsnNode first = body.instructions.getFirst();
        while (first != null && AsmUtil.isTrivia(first)) {
            first = first.getNext();
        }
        if (first == null) return false;
        if (first.getOpcode() != Opcodes.RETURN) return false;
        AbstractInsnNode next = first.getNext();
        while (next != null && AsmUtil.isTrivia(next)) {
            next = next.getNext();
        }
        return next == null;
    }
}
