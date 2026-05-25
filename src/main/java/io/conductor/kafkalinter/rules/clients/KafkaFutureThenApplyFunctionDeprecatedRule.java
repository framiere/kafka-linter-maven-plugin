package io.conductor.kafkalinter.rules.clients;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Handle;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Fires for every reach of the deprecated
 * {@link org.apache.kafka.common.KafkaFuture#thenApply(org.apache.kafka.common.KafkaFuture.Function)}
 * overload — whether the call lands directly via {@code INVOKEVIRTUAL}
 * or indirectly through an {@code INVOKEDYNAMIC} method-reference capture
 * (e.g. {@code future::thenApply} bound to a
 * {@code UnaryOperator<KafkaFuture<?>>} or
 * {@code Function<KafkaFuture.Function<T, R>, KafkaFuture<R>>} factory
 * used by promise-chain builders, Admin-result-shape fixtures, or
 * reactor-style adapter test harnesses).
 *
 * <h2>Why this overload is deprecated, not merely cosmetic</h2>
 *
 * <p>{@link org.apache.kafka.common.KafkaFuture} is the
 * {@code CompletableFuture}-style abstraction returned by every
 * AdminClient call: {@code createTopics(...).values().get("orders")},
 * {@code listTopics().listings()}, {@code describeCluster().nodes()}.
 * Callers chain transformations with {@code thenApply}; the result is
 * another {@code KafkaFuture} that completes with the transformed value
 * or propagates the upstream exception.
 *
 * <p>{@code KafkaFuture} predates {@code java.util.function.Function} —
 * the first kafka-clients release defined its own {@code Function<T, R>}
 * inner type whose {@code apply(T)} threw {@code Throwable}. That
 * choice forces every callsite to wrap the lambda body in
 * {@code try/catch(Throwable)}, which (a) silently swallows
 * unrecoverable errors like {@code OutOfMemoryError},
 * {@code StackOverflowError}, and {@code ThreadDeath}, and (b) hides
 * NPEs and ClassCastExceptions inside the chained future, surfacing as
 * a {@code CompletionException} two callbacks downstream.
 *
 * <p>KIP-707 (Kafka 3.0) introduced
 * {@code KafkaFuture.BaseFunction<T, R>} — same {@code apply(T)} shape,
 * <em>no checked exception</em>. The runtime semantics are identical;
 * the only difference is the SAM's exception signature. The new
 * {@code thenApply(BaseFunction)} is the supported overload; the legacy
 * {@code thenApply(Function)} remains for source compatibility with
 * code that already declares a {@code KafkaFuture.Function} variable.
 *
 * <p>Four consequences flow from continued use of the legacy overload:
 *
 * <ul>
 *   <li><b>Checked-{@code Throwable} catch blocks suppress unrecoverable
 *       errors.</b> Lambdas passed to the legacy overload that need to
 *       compile must catch {@code Throwable} (or declare it in the
 *       enclosing method) — a {@code catch(Throwable)} on user code
 *       catches the JVM's "fatal" Errors that should kill the process.</li>
 *   <li><b>Future chains lose stack-trace fidelity.</b> Exceptions
 *       thrown from the legacy SAM are wrapped twice (once by the
 *       checked-exception declaration, once by the
 *       {@code KafkaFuture} completion mechanism), making the eventual
 *       {@code CompletionException} cause-chain deeper and harder to
 *       read in production stack traces.</li>
 *   <li><b>Migration to {@code java.util.concurrent.CompletionStage}
 *       breaks at the type boundary.</b>
 *       {@code KafkaFuture.toCompletionStage()} returns a
 *       {@code CompletionStage<T>} whose {@code thenApply} takes a
 *       {@code java.util.function.Function} — code that built
 *       transformations with {@code KafkaFuture.Function} cannot be
 *       reused on the stage side without reimplementing every step.</li>
 *   <li><b>INVOKEDYNAMIC {@code future::thenApply} captures silently
 *       bind to the deprecated overload when the enclosing context
 *       expects a SAM whose {@code apply} signature happens to match
 *       the legacy {@code Function} shape.</b> The user-class bytecode
 *       contains zero direct {@code INVOKEVIRTUAL} on the legacy
 *       overload and a name-only MethodInsnNode walk misses it.</li>
 * </ul>
 *
 * <h2>The replacement API</h2>
 *
 * <p>Use {@code thenApply(KafkaFuture.BaseFunction)} — identical
 * {@code apply(T)} shape, no checked exception. Existing lambdas need
 * no source changes; the only edit is the declared SAM type on the
 * receiving variable (when one exists) and any try/catch wrappers can
 * be deleted. For new code, switch to
 * {@code kafkaFuture.toCompletionStage().thenApply(...)} with the
 * standard {@code java.util.function.Function} so the chain is
 * portable across the rest of the JDK's async ecosystem.
 *
 * <h2>Descriptor discrimination</h2>
 *
 * <p>{@code thenApply} is overloaded on {@code KafkaFuture}: the
 * deprecated overload has descriptor
 * {@code (Lorg/apache/kafka/common/KafkaFuture$Function;)Lorg/apache/kafka/common/KafkaFuture;}
 * and the supported overload has descriptor
 * {@code (Lorg/apache/kafka/common/KafkaFuture$BaseFunction;)Lorg/apache/kafka/common/KafkaFuture;}.
 * The rule matches the legacy descriptor exactly. The modern
 * descriptor differs by the SAM-type parameter, so it never matches —
 * descriptor discrimination is mandatory because a name-only filter
 * would false-positive on the supported migration target.
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code future::thenApply} bound to a functional interface compiles
 * to {@code INVOKEDYNAMIC} whose bsm-args contain a
 * {@code REF_invokeVirtual} handle pointing at the resolved method.
 * The rule's bsm-arg walk catches this case by checking the handle's
 * {@code (owner, name, desc)} triple against the same filter used for
 * direct calls.
 */
public final class KafkaFutureThenApplyFunctionDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KAFKA_FUTURE);
    private static final String METHOD_NAME = "thenApply";
    private static final String LEGACY_DESC =
            "(Lorg/apache/kafka/common/KafkaFuture$Function;)Lorg/apache/kafka/common/KafkaFuture;";

    private final Severity severity;

    public KafkaFutureThenApplyFunctionDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.KAFKA_FUTURE_THENAPPLY_FUNCTION_DEPRECATED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && LEGACY_DESC.equals(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, LEGACY_DESC);
                    if (h != null) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.KAFKA_FUTURE_THENAPPLY_FUNCTION_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KafkaFuture.thenApply(KafkaFuture.Function) is reached here — either as "
                        + "a direct call or as an INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `future::thenApply` bound to a UnaryOperator or other SAM "
                        + "factory used by a promise-chain builder, Admin-result-shape "
                        + "fixture, or reactor-style adapter test harness). This overload "
                        + "is deprecated since Kafka 3.0 (KIP-707) because the legacy "
                        + "`KafkaFuture.Function<T, R>` SAM declared a checked-exception "
                        + "`apply(T) throws Throwable` — every lambda passed to it must "
                        + "either catch Throwable (which swallows unrecoverable Errors like "
                        + "OutOfMemoryError, StackOverflowError, and ThreadDeath) or "
                        + "declare them up the call chain. Four consequences for continued "
                        + "use: (1) catch(Throwable) blocks on lambda bodies suppress JVM "
                        + "fatal Errors that should kill the process; (2) future-chain "
                        + "stack traces lose fidelity — exceptions are wrapped twice "
                        + "(checked-exception declaration + KafkaFuture completion "
                        + "mechanism), making CompletionException cause-chains deeper and "
                        + "harder to read; (3) migration to "
                        + "java.util.concurrent.CompletionStage breaks at the type "
                        + "boundary — KafkaFuture.toCompletionStage().thenApply takes "
                        + "java.util.function.Function so code built around "
                        + "KafkaFuture.Function cannot be reused on the stage side; "
                        + "(4) INVOKEDYNAMIC `future::thenApply` captures silently bind to "
                        + "the deprecated overload when the enclosing SAM signature happens "
                        + "to match — the user-class bytecode contains zero direct "
                        + "INVOKEVIRTUAL on the legacy overload and a name-only walk "
                        + "misses it. Migrate to `thenApply(KafkaFuture.BaseFunction)` "
                        + "(identical apply(T) shape, no checked exception — existing "
                        + "lambdas need no source changes; any try/catch(Throwable) "
                        + "wrappers can be deleted). For new code, prefer "
                        + "`kafkaFuture.toCompletionStage().thenApply(...)` with the "
                        + "standard java.util.function.Function so the chain is portable "
                        + "across the rest of the JDK's async ecosystem.");
    }
}
