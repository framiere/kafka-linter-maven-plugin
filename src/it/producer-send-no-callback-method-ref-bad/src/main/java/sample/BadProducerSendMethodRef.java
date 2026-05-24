package sample;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * RULE: PRODUCER_SEND_NO_CALLBACK — method-reference variant.
 *
 * <p>Each method in this fixture captures {@code producer::send} into a
 * <strong>void-returning functional interface</strong>: a
 * {@link java.util.function.Consumer}, a {@link Runnable}, a method that
 * accepts {@link Runnable}, etc. The {@code Future<RecordMetadata>} that
 * {@code Producer.send(record)} returns is silently discarded by the SAM
 * adapter — there is no place in user code where the Future is
 * observable and no callback is attached. Any of the failure modes the
 * rule exists to surface — broker-down, queue-full, serialiser-throw,
 * authentication-revoked — silently disappears.
 *
 * <h2>Why the naive scan misses this entirely</h2>
 *
 * Each of these shapes compiles to an {@code INVOKEDYNAMIC} whose
 * bootstrap-arg list contains a direct
 * {@code REF_invokeInterface Producer.send:(LProducerRecord;)LFuture;}
 * handle. The deferred invocation of {@code send(record)} materialises
 * inside the SAM adapter's synthetic dispatch code (on whatever thread
 * later invokes the {@code Consumer.accept(record)} / {@code Runnable.run()}
 * call), NOT inside the user's class. <strong>The user-class bytecode
 * contains zero {@code INVOKE*} instructions targeting
 * {@code Producer.send}.</strong>
 *
 * <p>A scan that walks only {@code MethodInsnNode} reports
 * {@code "no send → no rule"} and emits ZERO violations on this entire
 * file — leaving the canonical batch-send anti-pattern silently
 * unmarked. With the indy walk plus the
 * {@code samMethodType.getReturnType() == VOID} discriminator, the rule
 * fires once per fire-and-forget method-ref site.
 *
 * <h2>Why void-returning SAM is the discriminator</h2>
 *
 * Both {@code records.forEach(producer::send)} and
 * {@code records.stream().map(producer::send)} compile the EXACT SAME
 * impl-method handle ({@code REF_invokeInterface Producer.send}). The
 * only difference is the SAM type at the call site:
 * {@code Consumer.accept(T)V} vs. {@code Function.apply(T)R}. If the
 * SAM's return is {@code void}, the adapter has no choice but to discard
 * whatever the impl returns — the Future is gone. If the SAM's return
 * is non-void, the caller of {@code apply()} receives the Future and can
 * still observe it ({@code map(producer::send).forEach(Future::get)}).
 *
 * <p>Conservative direction: only fire on void-returning SAMs. The
 * {@code Function}-shaped capture might still discard the result one
 * level up — but at that level it would (almost always) show up as
 * something we already catch (a {@code POP} after a {@code stream().map()}
 * terminal-not-collected pattern). Keeping the discriminator narrow
 * avoids false positives in the very common observed-Future shape.
 *
 * <p>Expected fires from this fixture: FIVE — one for each of the four
 * fire-and-forget method-ref sites (caught by the indy branch) plus one
 * for the explicit {@code () -> producer.send(r)} lambda body inside
 * {@code executorSubmitSendRef} (caught by the existing direct
 * {@code INVOKE* send} + {@code POP} walk over the synthetic
 * {@code lambda$N} method). Both detection paths fire side-by-side on
 * the same fixture — a useful regression signal that the indy walk has
 * not displaced or shadowed the original lambda-body walk.
 */
public final class BadProducerSendMethodRef {

    /**
     * The canonical fire-and-forget batch-send shape:
     * {@code records.forEach(producer::send)}. The bsm-target handle is
     * {@code Producer.send(ProducerRecord)} returning {@code Future};
     * the SAM is {@code Consumer.accept(T)V} — return is void, Future is
     * discarded by the adapter on every iteration.
     */
    public void forEachSendRef(Producer<String, String> producer,
                               List<ProducerRecord<String, String>> records) {
        records.forEach(producer::send); // reported
    }

    /**
     * Same idea but with a Stream pipeline that terminates in a
     * void-returning forEach.
     */
    public void streamForEachSendRef(Producer<String, String> producer,
                                     List<ProducerRecord<String, String>> records) {
        records.stream().forEach(producer::send); // reported
    }

    /**
     * Executor-task variant — a {@code Consumer<ProducerRecord>} captured
     * for later dispatch on a worker thread. Same discard semantics as
     * forEach.
     */
    public void executorSubmitSendRef(ExecutorService exec,
                                      Producer<String, String> producer,
                                      List<ProducerRecord<String, String>> records) {
        records.forEach(r -> exec.execute(() -> producer.send(r))); // lambda — handled by direct scan
        exec.execute(() -> records.forEach(producer::send)); // reported (nested method-ref inside the Runnable's lambda)
    }

    /**
     * CompletableFuture.thenAccept(...) — another classic
     * {@code Consumer<T>} sink. {@code thenAccept(producer::send)} fires
     * the captured send on whatever record is produced upstream and
     * silently drops the resulting {@code Future<RecordMetadata>}.
     */
    public void thenAcceptSendRef(Producer<String, String> producer,
                                  CompletableFuture<ProducerRecord<String, String>> upstream) {
        upstream.thenAccept(producer::send); // reported
    }
}
