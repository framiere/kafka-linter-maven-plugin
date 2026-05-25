package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * RULE: CONSUMER_COMMIT_ASYNC_NO_CALLBACK — must fire on EVERY method
 * below.
 *
 * <p>Each method exercises one of the bytecode shapes the rule is
 * required to catch:
 *
 * <ol>
 *   <li>direct {@code INVOKEVIRTUAL} on
 *       {@code KafkaConsumer.commitAsync()V};</li>
 *   <li>direct {@code INVOKEINTERFACE} on
 *       {@code Consumer.commitAsync()V};</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code consumer::commitAsync} bound to a custom no-arg SAM
 *       ({@code CommitTrigger}) and to {@link Runnable};</li>
 *   <li>{@code INVOKEDYNAMIC} capture against the {@link Consumer}
 *       interface-typed receiver (REF_invokeInterface) vs. against
 *       the {@link KafkaConsumer} typed receiver (REF_invokeVirtual)
 *       — both are caught;</li>
 *   <li>{@link Runnable}-wrapped dispatch via
 *       {@link ScheduledExecutorService} and {@link ExecutorService}.</li>
 * </ol>
 *
 * <h2>Why no-callback commitAsync is a correctness hazard</h2>
 *
 * <p>{@link Consumer#commitAsync()} is fire-and-forget by design: the
 * method dispatches an OffsetCommit request to the group coordinator
 * and returns immediately, before the broker has acknowledged the
 * commit. The Consumer does NOT auto-retry async commits — retriable
 * errors (COORDINATOR_NOT_AVAILABLE, COORDINATOR_LOAD_IN_PROGRESS,
 * NOT_COORDINATOR, REBALANCE_IN_PROGRESS) and fatal errors
 * (ILLEGAL_GENERATION, GROUP_AUTHORIZATION_FAILED,
 * TOPIC_AUTHORIZATION_FAILED) are delivered to the callback (if any)
 * and discarded otherwise. The no-callback overload has no path to
 * surface any of these — the commit either silently succeeds or
 * silently fails, and the application has no signal whatsoever to
 * detect, retry, alert, or log the failure. Eventually the process
 * restarts and the consumer rejoins the group with a committed offset
 * from minutes-to-hours ago; every record since the last successful
 * commit is replayed; any downstream write that is not idempotent
 * produces duplicates.
 */
public final class BadConsumerCommitAsyncNoCallback {

    @FunctionalInterface
    interface CommitTrigger {
        void fire();
    }

    // ===== Direct INVOKEVIRTUAL on KafkaConsumer.commitAsync() =====

    /**
     * MUST FIRE — direct INVOKEVIRTUAL on
     * {@code KafkaConsumer.commitAsync()V}. Consumer loop calls
     * {@code consumer.commitAsync()} at the end of every batch. The
     * coordinator fails over (rolling restart, network partition,
     * leader election on {@code __consumer_offsets}); for the
     * duration of the failover every async commit returns a retriable
     * error (typically {@code NOT_COORDINATOR}), silently dropped;
     * the consumer continues, processes thousands of records, and is
     * restarted (deploy, OOM, eviction); on rejoin the committed
     * offset is the last successful commit before the failover; every
     * record since is replayed; downstream non-idempotent writes
     * produce duplicates.
     */
    public void consumerLoopBatchCommit(KafkaConsumer<String, String> consumer) {
        consumer.commitAsync();
    }

    /**
     * MUST FIRE — direct INVOKEVIRTUAL. Health-check / heartbeat tick
     * that commits whatever offsets have been processed since the
     * last tick. During a rebalance ongoing async commits return
     * {@code REBALANCE_IN_PROGRESS}; without a callback the
     * application has no signal that the commits were rejected; the
     * partition is reassigned to a different consumer instance which
     * then re-processes records the previous owner had already
     * handled.
     */
    public void heartbeatTickCommit(KafkaConsumer<String, String> consumer) {
        consumer.commitAsync();
    }

    // ===== Direct INVOKEINTERFACE on Consumer.commitAsync() =====

    /**
     * MUST FIRE — interface-typed receiver. INVOKEINTERFACE on
     * {@code org/apache/kafka/clients/consumer/Consumer.commitAsync(
     * )V}. Helper that hides the consumer behind the
     * {@link Consumer} interface; during ACL rotation
     * {@code OffsetCommit} responses come back with
     * {@code GROUP_AUTHORIZATION_FAILED} or
     * {@code TOPIC_AUTHORIZATION_FAILED}; without a callback these
     * never surface; metrics show 'healthy' while offsets stop
     * advancing — silent stuck commit; the bug is discovered hours
     * later when a restart reveals a huge backlog of replayed work.
     */
    public void acmlessHelperInterfaceCommit(Consumer<String, String> consumer) {
        consumer.commitAsync();
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /**
     * MUST FIRE — {@code consumer::commitAsync} bound to a custom
     * no-arg functional interface whose erased SAM signature matches
     * {@code ()V}. INVOKEDYNAMIC bsm-args contain a
     * REF_invokeVirtual handle pointing at
     * {@code KafkaConsumer.commitAsync()V}.
     */
    public CommitTrigger buildTrigger(KafkaConsumer<String, String> consumer) {
        return consumer::commitAsync;
    }

    /**
     * MUST FIRE — same indy capture against the {@link Consumer}
     * interface-typed receiver. Handle is REF_invokeInterface; the
     * (owner, name, desc) tuple matches.
     */
    public CommitTrigger buildTriggerInterface(Consumer<String, String> consumer) {
        return consumer::commitAsync;
    }

    /**
     * MUST FIRE — {@code consumer::commitAsync} bound to
     * {@link Runnable}. The SAM {@code void run()} erases to
     * {@code ()V} which matches the no-callback descriptor exactly.
     * This shape is very common when the commit is dispatched via
     * {@link ScheduledExecutorService}.
     */
    public Runnable buildRunnable(KafkaConsumer<String, String> consumer) {
        return consumer::commitAsync;
    }

    /**
     * MUST FIRE — explicit lambda body that invokes
     * {@code consumer.commitAsync()}. The synthetic lambda method's
     * bytecode contains a direct INVOKEVIRTUAL on the no-callback
     * overload — the rule walks all methods on the class, including
     * synthetic lambda bodies, and fires there. Scheduler context
     * amplifies the hazard: no log sink, no exception path, no test
     * coverage.
     */
    public ScheduledFuture<?> scheduleCommitFixedRate(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer) {
        return scheduler.scheduleAtFixedRate(
                () -> consumer.commitAsync(), 0, 5, TimeUnit.SECONDS);
    }

    /**
     * MUST FIRE — Runnable method-reference captured into a local
     * binding and submitted to an {@link ExecutorService}. The indy
     * site itself targets {@code Consumer.commitAsync()V}.
     */
    public Future<?> submitCommit(
            ExecutorService executor,
            Consumer<String, String> consumer) {
        Runnable trigger = consumer::commitAsync;
        return executor.submit(trigger);
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new BadConsumerCommitAsyncNoCallback().consumerLoopBatchCommit(c);
        }
    }
}
