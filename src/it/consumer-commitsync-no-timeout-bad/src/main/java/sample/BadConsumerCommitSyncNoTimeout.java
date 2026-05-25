package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * RULE: CONSUMER_COMMITSYNC_NO_TIMEOUT — must fire on EVERY method
 * below.
 *
 * <p>Each method exercises one of the bytecode shapes the rule is
 * required to catch:
 *
 * <ol>
 *   <li>direct {@code INVOKEVIRTUAL} on
 *       {@code KafkaConsumer.commitSync()V};</li>
 *   <li>direct {@code INVOKEINTERFACE} on
 *       {@code Consumer.commitSync()V};</li>
 *   <li>direct {@code INVOKEVIRTUAL} on
 *       {@code KafkaConsumer.commitSync(Ljava/util/Map;)V};</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code consumer::commitSync} bound to {@link Runnable}
 *       (typical when scheduling out-of-band offset flushes via
 *       {@link ScheduledExecutorService});</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture against a
 *       {@link KafkaConsumer} typed receiver (REF_invokeVirtual)
 *       vs. against a {@link Consumer} interface-typed receiver
 *       (REF_invokeInterface) — both are caught.</li>
 * </ol>
 *
 * <h2>Why the bounded {@code commitSync(Duration)} overload exists</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer#commitSync()}
 * is documented as "equivalent to commitSync(Duration.ofMillis(
 * Long.MAX_VALUE))" — it sends an OffsetCommit request to the group
 * coordinator and parks the calling thread until the response
 * arrives, with no caller-side deadline. Under a slow / unreachable /
 * moving coordinator the call hangs indefinitely; the bounded
 * overload {@code commitSync(Duration)} surfaces a TimeoutException
 * so the caller can retry, surface the failure, or shed load.
 */
public final class BadConsumerCommitSyncNoTimeout {

    // ===== Direct INVOKEVIRTUAL on KafkaConsumer.commitSync()V =====

    /**
     * MUST FIRE — direct INVOKEVIRTUAL on
     * {@code KafkaConsumer.commitSync()V}. Poll-loop pinning hazard:
     * session.timeout.ms can expire while we wait for the
     * coordinator, partitions get rebalanced away, and the eventual
     * commit fails with CommitFailedException.
     */
    public void pollLoopCommit(KafkaConsumer<String, String> consumer) {
        consumer.commitSync();
    }

    /**
     * MUST FIRE — graceful-shutdown final commit. SIGTERM-triggered
     * shutdown sequence drains in-flight work, calls commitSync()
     * once, then close(). A slow coordinator overshoots the Pod
     * terminationGracePeriodSeconds budget; kubelet sends SIGKILL,
     * offsets are not committed, the next instance reprocesses the
     * un-committed batch.
     */
    public void gracefulShutdown(KafkaConsumer<String, String> consumer) {
        consumer.commitSync();
        consumer.close();
    }

    // ===== Direct INVOKEVIRTUAL on KafkaConsumer.commitSync(Map)V =====

    /**
     * MUST FIRE — per-partition commitSync(Map). Same hazard at a
     * finer granularity; the smaller commit payload does not bound
     * the wait.
     */
    public void perPartitionCommit(
            KafkaConsumer<String, String> consumer,
            Map<TopicPartition, OffsetAndMetadata> offsets) {
        consumer.commitSync(offsets);
    }

    // ===== Direct INVOKEINTERFACE on Consumer.commitSync()V =====

    /**
     * MUST FIRE — interface-typed receiver. INVOKEINTERFACE on
     * {@code org/apache/kafka/clients/consumer/Consumer.commitSync()V}.
     * Common in code that programs against the Consumer interface
     * for testability.
     */
    public void commitViaInterface(Consumer<String, String> consumer) {
        consumer.commitSync();
    }

    /**
     * MUST FIRE — interface-typed receiver, per-partition overload.
     */
    public void commitViaInterfacePerPartition(
            Consumer<String, String> consumer,
            Map<TopicPartition, OffsetAndMetadata> offsets) {
        consumer.commitSync(offsets);
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /**
     * MUST FIRE — {@code consumer::commitSync} bound to
     * {@link Runnable}, scheduled at fixed rate. Scheduler-thread
     * pinning hazard: when the coordinator is slow the scheduler's
     * thread is stuck inside commitSync indefinitely; subsequent
     * scheduled tasks queue behind it and the executor's queue
     * eventually saturates.
     *
     * <p>The user-class bytecode contains ZERO direct INVOKEVIRTUAL
     * on KafkaConsumer.commitSync; only an INVOKEDYNAMIC +
     * LambdaMetafactory bridge whose bsm-args contain a
     * REF_invokeVirtual handle pointing at
     * {@code KafkaConsumer.commitSync()V}. A naïve MethodInsnNode-
     * only lint misses this entirely.
     */
    public ScheduledFuture<?> scheduleCommitViaMethodRef(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer) {
        return scheduler.scheduleAtFixedRate(consumer::commitSync, 0, 30, TimeUnit.SECONDS);
    }

    /**
     * MUST FIRE — same indy capture shape against the Consumer
     * interface-typed receiver. The bsm-args handle is
     * REF_invokeInterface instead of REF_invokeVirtual, but the
     * (owner, name, desc) tuple matches and the rule fires.
     */
    public ScheduledFuture<?> scheduleCommitViaInterfaceMethodRef(
            ScheduledExecutorService scheduler,
            Consumer<String, String> consumer) {
        return scheduler.scheduleAtFixedRate(consumer::commitSync, 0, 30, TimeUnit.SECONDS);
    }

    /**
     * MUST FIRE — {@code consumer::commitSync} bound to {@link Runnable}
     * via a JVM shutdown hook. Production-shape pattern: the JVM
     * shutdown hook flushes offsets one final time before close.
     * Same hazard as the explicit gracefulShutdown shape, exposed
     * through an indy.
     */
    public Thread shutdownHookViaMethodRef(KafkaConsumer<String, String> consumer) {
        Thread hook = new Thread(consumer::commitSync);
        Runtime.getRuntime().addShutdownHook(hook);
        return hook;
    }

    public static void main(String[] args) {
        new BadConsumerCommitSyncNoTimeout()
                .pollLoopCommit(new KafkaConsumer<>(new Properties()));
    }
}
