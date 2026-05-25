package sample;

import org.apache.kafka.common.KafkaFuture;

/**
 * RULE: KAFKA_FUTURE_THENAPPLY_FUNCTION_DEPRECATED — must fire on both
 * methods.
 *
 * <p>The two methods below exercise the two distinct bytecode shapes the
 * rule is required to catch for the deprecated
 * {@code KafkaFuture.thenApply(KafkaFuture.Function)} overload:
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on the legacy overload — the
 *       classic call site, where the user-class bytecode contains an
 *       explicit {@code INVOKEVIRTUAL
 *       org/apache/kafka/common/KafkaFuture.thenApply(Lorg/apache/kafka/common/KafkaFuture$Function;)Lorg/apache/kafka/common/KafkaFuture;}.
 *       The lambda is declared as the legacy
 *       {@code KafkaFuture.Function<T, R>} SAM to force javac's overload
 *       resolution onto the deprecated entry — without the explicit
 *       SAM-typed local, javac would prefer the modern
 *       {@code BaseFunction} overload.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture {@code future::thenApply}
 *       bound to a custom 1-arg {@code @FunctionalInterface} whose
 *       {@code apply} signature is
 *       {@code (KafkaFuture.Function<T, R>) -> KafkaFuture<R>}. javac
 *       resolves the method-ref to the overload whose parameter type
 *       matches the SAM's argument type — the legacy
 *       {@code thenApply(Function)} overload — and emits an
 *       {@code INVOKEDYNAMIC} site whose bsm-args contain a
 *       {@code REF_invokeVirtual} handle pointing at
 *       {@code KafkaFuture.thenApply(Lorg/apache/kafka/common/KafkaFuture$Function;)Lorg/apache/kafka/common/KafkaFuture;}.
 *       The user-class bytecode at this site contains ZERO direct
 *       {@code INVOKEVIRTUAL} on the legacy overload — only the
 *       {@code INVOKEDYNAMIC} + {@code LambdaMetafactory} bridge. A
 *       name-only MethodInsnNode walk misses this case entirely; the
 *       rule's bsm-arg walk via {@code AsmUtil.indyTargetHandle} catches
 *       it.</li>
 * </ol>
 *
 * <h2>Why this overload is deprecated</h2>
 *
 * <p>{@code KafkaFuture.Function<T, R>} predates
 * {@code java.util.function.Function} and declares
 * {@code apply(T) throws Throwable}. KIP-707 (Kafka 3.0) introduced
 * {@code KafkaFuture.BaseFunction<T, R>} with identical
 * {@code apply(T)} shape but no checked exception, so every lambda
 * passed to the legacy overload must either catch {@code Throwable}
 * (which suppresses JVM-fatal Errors like {@code OutOfMemoryError},
 * {@code StackOverflowError}, {@code ThreadDeath}) or declare them up
 * the call chain. The runtime semantics are identical to the modern
 * overload; the migration is a pure source-level cleanup.
 */
public final class BadKafkaFutureThenApplyFunction {

    @FunctionalInterface
    interface LegacyApplier<T, R> {
        KafkaFuture<R> apply(KafkaFuture.Function<T, R> fn);
    }

    @SuppressWarnings("deprecation")
    public KafkaFuture<Integer> buildDirect(KafkaFuture<String> future) {
        // MUST FIRE — direct INVOKEVIRTUAL on the deprecated overload
        // KafkaFuture.thenApply(KafkaFuture.Function).
        //
        // Note: in modern kafka-clients (3.x), KafkaFuture.Function is an
        // ABSTRACT CLASS (not a functional interface) — it predates
        // BaseFunction and only survives for binary compatibility. So
        // the only way to instantiate it is via an anonymous class
        // extending it. The static type of `legacyFn` is the legacy
        // Function, which forces javac to resolve future.thenApply(...)
        // onto the deprecated overload — emitting an INVOKEVIRTUAL with
        // descriptor
        // (Lorg/apache/kafka/common/KafkaFuture$Function;)Lorg/apache/kafka/common/KafkaFuture;.
        KafkaFuture.Function<String, Integer> legacyFn = new KafkaFuture.Function<String, Integer>() {
            @Override
            public Integer apply(String s) {
                return s.length();
            }
        };
        return future.thenApply(legacyFn);
    }

    @SuppressWarnings("deprecation")
    public LegacyApplier<String, Integer> capturedLegacyFactory(KafkaFuture<String> future) {
        // MUST FIRE — INVOKEDYNAMIC method-ref capture targeting the
        // deprecated overload. javac resolves `future::thenApply` by
        // matching the SAM's argument type (KafkaFuture.Function<T, R>)
        // against the legacy overload, emitting an INVOKEDYNAMIC site
        // whose bsm-args contain a REF_invokeVirtual handle on
        // (Lorg/apache/kafka/common/KafkaFuture$Function;)Lorg/apache/kafka/common/KafkaFuture;.
        // The user-class bytecode here contains ZERO direct
        // INVOKEVIRTUAL on the legacy overload — only the INVOKEDYNAMIC
        // + LambdaMetafactory bridge. A name-only MethodInsnNode walk
        // misses this entirely; the rule's bsm-arg walk catches it.
        return future::thenApply;
    }
}
