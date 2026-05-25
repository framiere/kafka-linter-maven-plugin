package sample;

import org.apache.kafka.common.KafkaFuture;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * RULE: KAFKA_FUTURE_THENAPPLY_FUNCTION_DEPRECATED — must NOT fire.
 *
 * <p>The three methods below exercise the supported migration targets:
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on the modern
 *       {@code thenApply(KafkaFuture.BaseFunction)} overload whose
 *       descriptor is
 *       {@code (Lorg/apache/kafka/common/KafkaFuture$BaseFunction;)Lorg/apache/kafka/common/KafkaFuture;}.
 *       The rule's filter matches the legacy descriptor exactly
 *       ({@code KafkaFuture$Function} vs {@code KafkaFuture$BaseFunction}
 *       in the parameter slot), so the modern overload never matches.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture {@code future::thenApply}
 *       bound to a custom 1-arg {@code @FunctionalInterface} whose
 *       {@code apply} signature takes a {@code BaseFunction} — the
 *       bsm-arg handle's descriptor points at the modern overload, so
 *       the rule's descriptor filter rejects the site.</li>
 *   <li>Bonus migration target: {@code future.toCompletionStage().thenApply(...)}
 *       with the standard {@code java.util.function.Function}. Every
 *       call here is on
 *       {@code java.util.concurrent.CompletionStage}, not
 *       {@code KafkaFuture} — the owner filter rejects the site
 *       regardless of method name.</li>
 * </ol>
 *
 * <h2>Why this is safe</h2>
 *
 * <p>The supported {@code thenApply(KafkaFuture.BaseFunction)} overload
 * has the same runtime semantics as the legacy
 * {@code thenApply(KafkaFuture.Function)} overload — the only
 * difference is the SAM's exception signature. {@code BaseFunction}'s
 * {@code apply(T)} does NOT declare {@code throws Throwable}, so
 * lambda bodies can throw {@code RuntimeException} freely and any
 * unrecoverable JVM error propagates normally instead of being
 * silently caught by a {@code try/catch(Throwable)} wrapper. Migration
 * is source-only: existing lambdas need no changes; the only edit is
 * the declared SAM type on the receiving variable, and any
 * {@code try/catch(Throwable)} wrappers can be deleted.
 *
 * <p>Descriptor discrimination is mandatory here because
 * {@code thenApply} is overloaded on {@code KafkaFuture}: a name-only
 * filter would false-positive on the supported migration target.
 */
public final class GoodKafkaFutureThenApplyBaseFunction {

    @FunctionalInterface
    interface ModernApplier<T, R> {
        KafkaFuture<R> apply(KafkaFuture.BaseFunction<T, R> fn);
    }

    public KafkaFuture<Integer> buildModern(KafkaFuture<String> future) {
        // DOES NOT FIRE — modern thenApply(BaseFunction) overload.
        // Descriptor:
        // (Lorg/apache/kafka/common/KafkaFuture$BaseFunction;)Lorg/apache/kafka/common/KafkaFuture;.
        // The BaseFunction SAM in the parameter slot places the
        // descriptor outside the rule's exact-match filter on the
        // legacy Function-parametered descriptor.
        KafkaFuture.BaseFunction<String, Integer> modernFn = String::length;
        return future.thenApply(modernFn);
    }

    public ModernApplier<String, Integer> capturedModernFactory(KafkaFuture<String> future) {
        // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture resolving to
        // the modern thenApply(BaseFunction) overload (the SAM
        // signature here takes BaseFunction). The bsm-arg handle's
        // descriptor points at the modern overload, so the rule's
        // descriptor filter rejects this site. This is precisely why
        // descriptor discrimination is mandatory: the name thenApply
        // and owner KafkaFuture both match, but the descriptor
        // difference protects the supported migration target.
        return future::thenApply;
    }

    public CompletionStage<Integer> viaCompletionStage(KafkaFuture<String> future) {
        // DOES NOT FIRE — KafkaFuture.toCompletionStage() returns a
        // java.util.concurrent.CompletionStage whose thenApply takes a
        // java.util.function.Function. The INVOKEINTERFACE here is on
        // CompletionStage, not KafkaFuture — the owner filter rejects
        // the site outright regardless of method name. This is the
        // recommended path for new code (KIP-707): bridge once to the
        // stdlib's async chain and use the rest of the JDK's
        // CompletionStage API.
        Function<String, Integer> stdFn = String::length;
        return future.toCompletionStage().thenApply(stdFn);
    }
}
