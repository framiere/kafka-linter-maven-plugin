package sample;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.kafka.streams.KafkaStreams;

/**
 * RULE: STREAMS_CLEANUP_IN_PROD — method-reference variant.
 *
 * <p>Each method in this fixture captures {@code streams::cleanUp} into
 * a no-arg {@link Runnable}-shaped functional interface and hands it to
 * a thread sink: an executor, a fresh {@link Thread}, the JVM shutdown
 * hook, or {@link CompletableFuture#runAsync}. The compiler resolves
 * the bound method-ref to {@code KafkaStreams.cleanUp:()V} — Runnable's
 * SAM has zero parameters and void return, and {@code cleanUp} matches.
 *
 * <h2>Why this is exactly as catastrophic as a direct call</h2>
 *
 * {@code cleanUp()} deletes everything under {@code state.dir} for this
 * {@code application.id} — every RocksDB store this instance owns. The
 * fact that the call materialises on a worker thread / scheduled task /
 * shutdown hook rather than synchronously on the calling thread changes
 * nothing about the side effect: by the time the SAM adapter returns,
 * the state stores are gone. The next {@code start()} replays the
 * entire changelog topic from offset 0 to rebuild them, the topology
 * pauses for that window, and recovery time scales with changelog size.
 *
 * <h2>Why the naive scan misses this</h2>
 *
 * Each capture compiles to an {@code INVOKEDYNAMIC} whose bsm-arg list
 * holds a direct
 * {@code REF_invokeVirtual KafkaStreams.cleanUp:()V} handle. The
 * actual {@code cleanUp()} invocation happens inside the SAM adapter
 * on the executor/Thread that later runs the captured target.
 * <strong>The user-class bytecode contains zero {@code INVOKE*}
 * instructions targeting {@code cleanUp}.</strong>
 *
 * <p>A MethodInsnNode-only walk reports {@code "no cleanUp → no rule"}
 * and emits ZERO violations on this entire file — leaving a
 * production-state-wipe hazard silently unmarked. With the indy walk
 * plus the handle-descriptor discriminator ({@code ()V}), the rule
 * fires once per capture site.
 *
 * <h2>Why no SAM-return discriminator</h2>
 *
 * Unlike {@code PRODUCER_SEND_NO_CALLBACK}, the hazard here is the
 * side effect of {@code cleanUp()} itself (file-tree deletion). The
 * method returns {@code void}; the SAM adapter cannot mask the side
 * effect even if it wanted to. So we fire on every indy whose target
 * handle is {@code cleanUp:()V}, regardless of {@code samMethodType}.
 *
 * <p>Expected fires from this fixture: FOUR — one for each capture
 * site below.
 */
public final class BadCleanupInProdMethodRef {

    /**
     * Executor task — the SAM is {@code Runnable.run()V}; the Runnable
     * adapter invokes the captured cleanUp on a worker thread. Same
     * side effect as a direct call.
     */
    public void executeCleanUpRef(ExecutorService exec, KafkaStreams streams) {
        exec.execute(streams::cleanUp); // reported
    }

    /**
     * Bare Thread — a developer who has been told "don't call cleanUp
     * synchronously on the request thread" might naively shift it to
     * a fresh thread. Same hazard.
     */
    public void freshThreadCleanUpRef(KafkaStreams streams) {
        new Thread(streams::cleanUp).start(); // reported
    }

    /**
     * Shutdown hook — perversely the most common variant: "on JVM
     * shutdown, clean up our state". This wipes RocksDB on every
     * graceful shutdown, forcing a full changelog replay on the next
     * boot. The fact that it's a shutdown hook is irrelevant to the
     * rule — it fires the same way.
     */
    public void shutdownHookCleanUpRef(KafkaStreams streams) {
        Runtime.getRuntime().addShutdownHook(new Thread(streams::cleanUp)); // reported
    }

    /**
     * CompletableFuture pipeline — {@code runAsync(Runnable)} dispatches
     * the captured cleanUp onto the common ForkJoinPool (or a supplied
     * executor). State stores are deleted by the time the returned
     * CompletableFuture completes.
     */
    public CompletableFuture<Void> runAsyncCleanUpRef(KafkaStreams streams) {
        return CompletableFuture.runAsync(streams::cleanUp); // reported
    }
}
