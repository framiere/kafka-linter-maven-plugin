package sample;

import org.apache.kafka.clients.consumer.Consumer;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * DOCUMENTED FALSE NEGATIVE: the rule abstains here, but the
 * shape IS a genuine thread-safety violation at runtime.
 *
 * Why the rule misses it:
 *   The rule's dispatch-recognition step is
 *     AbstractInsnNode next = AsmUtil.nextSignificant(indy);
 *     if (!(next instanceof MethodInsnNode dispatch)) continue;
 *   — i.e., it walks past Label/LineNumber/Frame trivia and expects
 *   the dispatch INVOKE to be the very next "significant"
 *   instruction after the INVOKEDYNAMIC that produces the lambda.
 *
 *   That assumption holds for the no-extra-args dispatch overloads:
 *     executor.submit(lambda)          — indy then INVOKEINTERFACE
 *     executor.execute(lambda)         — indy then INVOKEINTERFACE
 *     new Thread(lambda)               — indy then INVOKESPECIAL
 *     CompletableFuture.runAsync(lam)  — indy then INVOKESTATIC
 *
 *   It BREAKS for dispatchers that take additional args after the
 *   Runnable / Callable — most notably
 *     ScheduledExecutorService.schedule(lambda, delay, unit)
 *     ScheduledExecutorService.scheduleAtFixedRate(lambda, d0, p, u)
 *     ScheduledExecutorService.scheduleWithFixedDelay(lambda, ...)
 *   For these, the bytecode pattern is
 *     INVOKEDYNAMIC ... (Runnable)
 *     LCONST_0                      <- not "trivia", not a MethodInsnNode
 *     LCONST_1
 *     GETSTATIC TimeUnit.SECONDS
 *     INVOKEINTERFACE scheduleAtFixedRate(Runnable, J, J, TimeUnit)
 *   AsmUtil.nextSignificant lands on LCONST_0, the
 *   `instanceof MethodInsnNode` check fails, and the rule abstains.
 *
 * Runtime behavior is unchanged from the Bad shapes that DO fire:
 *   - The scheduler runs the Runnable on a worker thread, not the
 *     caller.
 *   - The lambda invokes `consumer.poll(...)` on that worker thread.
 *   - At the first invocation OR the first concurrent invocation,
 *     KafkaConsumer's internal acquire() check throws
 *       ConcurrentModificationException: KafkaConsumer is not safe
 *         for multi-threaded access
 *
 * Fix path for the rule (tracked separately): broaden the dispatch
 * search to walk forward past the lambda's INVOKEDYNAMIC through
 * stack-pushing instructions (LDC, *CONST_*, BIPUSH, SIPUSH, *LOAD,
 * GETFIELD, GETSTATIC, NEW+DUP, etc.) until landing on the
 * MethodInsnNode that consumes the lambda — provided the lambda is
 * the first arg. The current rule design implicitly assumes that
 * already; broadening only matters when args come AFTER the lambda.
 *
 * This file documents the gap explicitly so future contributors
 * see the limitation and the IT records it as a known-silent shape.
 */
public class FalseNegativeScheduledDispatch {

    private final Consumer<String, String> consumer;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public FalseNegativeScheduledDispatch(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    /**
     * Genuine bug, but the rule does not fire — the
     * scheduleAtFixedRate dispatch has timing args between the
     * Runnable and the INVOKEINTERFACE, so the
     * nextSignificant(indy)-must-be-MethodInsnNode check abstains.
     */
    public void periodicPollerSilentlyBroken() {
        scheduler.scheduleAtFixedRate(
                () -> consumer.poll(Duration.ofMillis(100)),
                0L, 1L, TimeUnit.SECONDS);
    }

    /**
     * Same shape with `schedule(Runnable, long, TimeUnit)` — also
     * silent for the same reason: LCONST_1 sits between the indy
     * and the INVOKEINTERFACE schedule(...).
     */
    public void delayedPollSilentlyBroken() {
        scheduler.schedule(
                () -> consumer.poll(Duration.ofMillis(100)),
                5L, TimeUnit.SECONDS);
    }
}
