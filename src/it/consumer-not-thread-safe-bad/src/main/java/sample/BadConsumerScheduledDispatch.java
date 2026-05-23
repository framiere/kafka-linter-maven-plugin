package sample;

import org.apache.kafka.clients.consumer.Consumer;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * RULE: CONSUMER_NOT_THREAD_SAFE — coverage extension to
 * ScheduledExecutorService dispatch overloads that pass timing
 * args AFTER the Runnable.
 *
 * Why this needed a rule extension:
 *
 *   The rule's original dispatch-recognition step was
 *     AbstractInsnNode next = AsmUtil.nextSignificant(indy);
 *     if (!(next instanceof MethodInsnNode dispatch)) continue;
 *   — i.e., walked past Label/LineNumber/Frame trivia and expected
 *   the dispatch INVOKE to be the very next "significant"
 *   instruction after the INVOKEDYNAMIC that produces the lambda.
 *
 *   That works for the no-extra-args dispatch overloads:
 *     executor.submit(lambda)          — indy then INVOKEINTERFACE
 *     executor.execute(lambda)         — indy then INVOKEINTERFACE
 *     new Thread(lambda)               — indy then INVOKESPECIAL
 *     CompletableFuture.runAsync(lam)  — indy then INVOKESTATIC
 *
 *   It BROKE for dispatchers that take additional args after the
 *   Runnable / Callable — most notably
 *     ScheduledExecutorService.schedule(lambda, delay, unit)
 *     ScheduledExecutorService.scheduleAtFixedRate(lambda, d0, p, u)
 *     ScheduledExecutorService.scheduleWithFixedDelay(lambda, ...)
 *   For these, the bytecode is
 *     INVOKEDYNAMIC ... (Runnable)
 *     LCONST_0                      <- not "trivia", not a MethodInsnNode
 *     LCONST_1
 *     GETSTATIC TimeUnit.SECONDS
 *     INVOKEINTERFACE scheduleAtFixedRate(Runnable, J, J, TimeUnit)
 *   The old nextSignificant(indy) landed on LCONST_0; the
 *   `instanceof MethodInsnNode` check failed; the rule abstained.
 *
 * The fix:
 *
 *   AsmUtil.nextDispatchInvoke now walks forward from the indy
 *   past trivia AND past pure-push instructions (constants,
 *   GETSTATIC, LDC, *LOAD) — the bytecode shape javac emits for
 *   constant/local-var args between the lambda and the dispatch
 *   INVOKE. It returns the first MethodInsnNode it lands on, OR
 *   null if it hits an instruction that pops the stack (POP,
 *   PUTFIELD, INVOKE*, arithmetic, branches). Returning null on a
 *   stack-popping instruction is the safe abstain: such an
 *   instruction could consume the lambda before the dispatch site,
 *   and we cannot prove the lambda is still at the deepest arg
 *   position when we eventually reach a method-insn.
 *
 *   The lambda is assumed to be the FIRST (deepest) method arg of
 *   the dispatch — which is true for every recognized dispatcher
 *   in DISPATCH_METHODS (Runnable/Callable is conventionally arg 0).
 *
 * Runtime semantics of the shapes below are identical to the
 * sibling BadConsumerNotThreadSafe shapes: the scheduler runs the
 * Runnable on a worker thread, the lambda body invokes a non-wakeup
 * consumer method, and at the first concurrent call the consumer's
 * `acquireAndEnsureOpen()` check throws
 *   ConcurrentModificationException:
 *     "KafkaConsumer is not safe for multi-threaded access"
 *
 * Each method below contributes one violation. Total: 2 violations.
 */
public class BadConsumerScheduledDispatch {

    private final Consumer<String, String> consumer;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public BadConsumerScheduledDispatch(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    /**
     * Shape A: scheduleAtFixedRate. The periodic-poller anti-pattern.
     * Bytecode dispatch (relevant fragment):
     *   INVOKEDYNAMIC ... (Runnable)
     *   LCONST_0
     *   LCONST_1
     *   GETSTATIC java/util/concurrent/TimeUnit.SECONDS
     *   INVOKEINTERFACE ScheduledExecutorService.scheduleAtFixedRate
     *     (Runnable, J, J, TimeUnit) ScheduledFuture
     * The new nextDispatchInvoke helper walks past the LCONST_0,
     * LCONST_1, GETSTATIC (all pure pushes) and lands on the
     * INVOKEINTERFACE — matching
     * DISPATCH_METHODS["ScheduledExecutorService"] =
     *   { "schedule", "scheduleAtFixedRate", "scheduleWithFixedDelay" }
     */
    public void periodicPoller() {
        // FIRES: lambda touching consumer.poll, scheduled via scheduleAtFixedRate.
        scheduler.scheduleAtFixedRate(
                () -> consumer.poll(Duration.ofMillis(100)),
                0L, 1L, TimeUnit.SECONDS);
    }

    /**
     * Shape B: schedule(Runnable, long, TimeUnit) — single delayed
     * dispatch. Same dispatch family, simpler arg shape (one Long
     * + TimeUnit between the indy and the INVOKEINTERFACE).
     */
    public void delayedPoll() {
        // FIRES: lambda touching consumer.poll, scheduled via schedule.
        scheduler.schedule(
                () -> consumer.poll(Duration.ofMillis(100)),
                5L, TimeUnit.SECONDS);
    }
}
