package sample;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.streams.KafkaStreams;

/**
 * RULE: STREAMS_REMOVE_THREAD_NO_TIMEOUT — method-reference variant.
 *
 * <p>Each method in this fixture captures
 * {@code streams::removeStreamThread} into a no-arg functional interface.
 * The compiler resolves the bound method-ref to the
 * <strong>no-argument overload</strong> {@code ()Ljava/util/Optional;}
 * because the SAM has zero parameters; the bounded overload
 * {@code (Ljava/time/Duration;)Ljava/util/Optional;} cannot bind here. So
 * the deferred call inherits the no-arg variant's hard-coded
 * {@code Duration.ofMillis(Long.MAX_VALUE)} wait — exactly the
 * "autoscaler hangs forever in PENDING_SHUTDOWN" hazard the rule exists
 * to surface.
 *
 * <h2>Why the naive scan misses this</h2>
 *
 * Each capture compiles to an {@code INVOKEDYNAMIC} whose bsm-arg list
 * holds a direct
 * {@code REF_invokeVirtual KafkaStreams.removeStreamThread:()Ljava/util/Optional;}
 * handle. The actual {@code removeStreamThread()} call materialises
 * inside the SAM adapter on whatever worker thread later runs the
 * captured target — typically a {@code ScheduledExecutorService} task, a
 * worker pool job, or the JVM shutdown hook thread. <strong>The user
 * class's bytecode contains zero {@code INVOKE*} instructions targeting
 * {@code removeStreamThread}.</strong>
 *
 * <p>A MethodInsnNode-only walk reports {@code "no removeStreamThread →
 * no rule"} and emits ZERO violations on this entire file — leaving the
 * canonical dynamic-scale-down anti-pattern silently unmarked. With the
 * indy walk plus the handle-descriptor discriminator
 * ({@code ()Ljava/util/Optional;}), the rule fires once per capture site.
 *
 * <h2>Why SAM return type is NOT the discriminator here</h2>
 *
 * Unlike {@code PRODUCER_SEND_NO_CALLBACK} — where the void-vs-non-void
 * SAM return decides whether the impl-method's {@code Future} can be
 * observed — the hazard here is the no-arg overload's
 * {@code Long.MAX_VALUE} wait <em>itself</em>. Whether the caller of
 * {@code Callable.call()} observes the returned {@code Optional<String>}
 * or whether {@code Runnable.run()} discards it, the wait has already
 * happened on the executor's worker thread before the SAM returns. So we
 * fire on every indy whose target handle is the no-arg overload,
 * regardless of {@code samMethodType}.
 *
 * <p>Expected fires from this fixture: FOUR — one for each capture site
 * below. The Good controls below must NOT fire.
 */
public final class BadRemoveThreadNoTimeoutMethodRef {

    /**
     * Autoscaler reconciliation tick — submits the no-arg removal onto a
     * worker pool. The cast to {@code Callable<Optional<String>>}
     * disambiguates the {@code submit(Runnable)} / {@code submit(Callable)}
     * overload set on {@code ExecutorService}; the bound method-ref then
     * resolves to the no-arg {@code ()LOptional;} overload because
     * Callable's SAM has zero parameters.
     */
    public void submitRemoveRef(ExecutorService exec, KafkaStreams streams) {
        exec.submit((Callable<Optional<String>>) streams::removeStreamThread); // reported
    }

    /**
     * Scheduled scale-down task — pure fire-and-forget Runnable. The
     * SAM is {@code run()V}; the captured {@code Optional<String>}
     * return value is silently discarded by the adapter. The
     * {@code Long.MAX_VALUE} wait happens on the executor thread before
     * the SAM ever returns.
     */
    public void executeRemoveRef(ExecutorService exec, KafkaStreams streams) {
        exec.execute(streams::removeStreamThread); // reported
    }

    /**
     * JVM shutdown hook — graceful scale-down on SIGTERM, parks the
     * shutdown-hook thread forever if the targeted StreamThread is
     * wedged. SAM is {@code Thread} ctor's {@code Runnable.run()V};
     * Optional silently discarded.
     */
    public void shutdownHookRemoveRef(KafkaStreams streams) {
        Runtime.getRuntime().addShutdownHook(new Thread(streams::removeStreamThread)); // reported
    }

    /**
     * CompletableFuture pipeline — even with {@code Supplier.get()} that
     * returns the {@code Optional}, the wait still occurred inside the
     * worker that ran the supplier. The Optional is observable to the
     * downstream chain but the autoscaler-deadlock has already happened.
     */
    public CompletableFuture<Optional<String>> supplyAsyncRemoveRef(
            ExecutorService exec, KafkaStreams streams) {
        @SuppressWarnings("unchecked")
        CompletableFuture<Optional<String>> f = (CompletableFuture<Optional<String>>)
                (CompletableFuture<?>) CompletableFuture.supplyAsync(streams::removeStreamThread, exec); // reported
        return f;
    }

    // ---- Good controls — must NOT fire -------------------------------

    /**
     * Lambda that calls the bounded overload — the indy targets a
     * synthetic {@code lambda$N} method whose body holds the direct
     * {@code INVOKEVIRTUAL removeStreamThread(Duration)}, descriptor
     * {@code (Ljava/time/Duration;)Ljava/util/Optional;}. The
     * MethodInsnNode walk's existing desc check rejects it; the indy
     * branch never matches (its target handle is on {@code lambda$N},
     * not on {@code removeStreamThread}).
     */
    public void scheduledBoundedRemove(ScheduledExecutorService sched, KafkaStreams streams) {
        sched.schedule(() -> streams.removeStreamThread(Duration.ofSeconds(30)),
                30, TimeUnit.SECONDS);
    }

    /**
     * Direct call on the bounded overload — direct walk's desc check
     * rejects it. Indy walk doesn't apply.
     */
    public Optional<String> boundedDirect(KafkaStreams streams) {
        return streams.removeStreamThread(Duration.ofSeconds(30));
    }
}
