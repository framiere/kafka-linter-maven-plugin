package io.conductor.kafkalinter.rules;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Flags producer.send(record) when:
 *   1. The send overload taken is the single-arg one (no Callback), AND
 *   2. The returned Future is immediately discarded (next instruction is POP).
 *
 * That's the classic fire-and-forget pattern — send failures are silently
 * swallowed because nothing ever observes the Future or its callback.
 *
 * Acceptable forms (NOT flagged):
 *   - producer.send(record, (md, ex) -> { ... })   // has Callback
 *   - Future f = producer.send(record); ... f.get()  // Future is observed
 *   - return producer.send(record);                  // caller handles it
 *
 * <p><strong>Method-reference capture is also flagged.</strong> The
 * idiomatic batch-send shape {@code records.forEach(producer::send)} is
 * fire-and-forget by construction: {@code Consumer.accept(T)} returns
 * {@code void}, so when the SAM adapter dispatches the captured
 * {@code send(record)} the {@code Future<RecordMetadata>} it returns is
 * silently discarded by the adapter — there is no point in the user's
 * code where the Future could ever be observed or a callback attached.
 * The result is identical to {@code send(record); /* future discarded *&#47;}
 * called once per record, which is exactly the bug this rule exists to
 * catch.
 *
 * <p>The discriminator is the indy's {@code samMethodType}
 * (i.e. {@code bsmArgs[0]} of the LambdaMetafactory call): if its return
 * type is {@code void}, the deferred call's result is discarded by the
 * adapter — fire. If non-void (e.g. {@code Function.apply}), the caller
 * of {@code apply()} observes the return and the rule stays silent
 * (conservative — that caller may still discard it, but at least the
 * Future surface exists at that level and a wrapper like
 * {@code .stream().map(producer::send).forEach(Future::get)} is a
 * legitimate observation pattern). Lambda forms like
 * {@code records.forEach(r -> producer.send(r))} compile to a synthetic
 * {@code lambda$N} method whose body holds the explicit
 * {@code INVOKEVIRTUAL send} followed by {@code POP} — already caught
 * by the MethodInsnNode walk over every method, including synthetics.
 */
public final class ProducerSendNoCallbackRule implements Rule {

    private final Severity severity;

    public ProducerSendNoCallbackRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.PRODUCER_SEND_NO_CALLBACK;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && KafkaTypes.PRODUCER_OWNERS.contains(mi.owner)
                        && "send".equals(mi.name)
                        && !AsmUtil.lastArgIsCallback(mi.desc)) {
                    AbstractInsnNode next = AsmUtil.nextSignificant(insn);
                    if (next instanceof InsnNode in && in.getOpcode() == Opcodes.POP) {
                        out.add(new Violation(
                                RuleId.PRODUCER_SEND_NO_CALLBACK,
                                severity,
                                ctx.classNode().name,
                                mn.name,
                                AsmUtil.lineOf(insn),
                                "send(record) without a Callback and the returned Future is discarded — errors will go unnoticed."));
                    }
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, KafkaTypes.PRODUCER_OWNERS, "send", null);
                    if (h == null) continue;
                    if (AsmUtil.lastArgIsCallback(h.getDesc())) continue; // callback overload
                    Type sam = AsmUtil.indySamMethodType(indy);
                    if (sam == null || sam.getReturnType().getSort() != Type.VOID) continue;
                    out.add(new Violation(
                            RuleId.PRODUCER_SEND_NO_CALLBACK,
                            severity,
                            ctx.classNode().name,
                            mn.name,
                            AsmUtil.lineOf(insn),
                            "send(record) reference captured (e.g. records.forEach(producer::send)) into a void-returning functional interface — the deferred call's Future is silently discarded by the SAM adapter and no callback is attached, so send errors will go unnoticed. Pass a record-by-record Callback or capture into a Function/Stream that observes each Future."));
                }
            }
        }
        return out;
    }
}
