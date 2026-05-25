package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * RULE: CONSUMER_COMMITSYNC_NO_TIMEOUT — must NOT fire on any method
 * below.
 *
 * <p>Mirror of {@code BadConsumerCommitSyncNoTimeout} where every call
 * site uses the bounded {@code commitSync(Duration)} or
 * {@code commitSync(Map, Duration)} overload. The descriptors are
 * {@code (Ljava/time/Duration;)V} and
 * {@code (Ljava/util/Map;Ljava/time/Duration;)V} respectively — neither
 * matches the rule's no-Duration descriptor set
 * {@code Set.of("()V", "(Ljava/util/Map;)V")}.
 *
 * <p>The lambdas below capture the same call sites as the BAD fixture's
 * INVOKEDYNAMIC method references but bind to SAMs whose erased
 * descriptors are {@code (Ljava/time/Duration;)V} or are wrapped in
 * explicit lambdas — neither shape matches the rule's predicate. (A
 * method reference {@code consumer::commitSync} cannot be used directly
 * to a Runnable when the bound overload takes a Duration argument; the
 * caller must supply the Duration somehow, which means the indy site
 * either targets the bounded overload — descriptor mismatch — or wraps
 * in an explicit lambda that hard-codes the Duration before invoking
 * the bounded overload.)
 *
 * <p>Migration guidance carried into the rule's violation message:
 * pick the {@link Duration} matched to the surrounding deadline (poll
 * budget, terminationGracePeriodSeconds minus a buffer, scheduler tick
 * interval) and let {@code TimeoutException} surface the
 * coordinator-outage as an actionable error rather than an indefinite
 * hang.
 */
public final class GoodConsumerCommitSyncNoTimeout {

    private static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration SHUTDOWN_COMMIT_TIMEOUT = Duration.ofSeconds(15);

    /**
     * OK — bounded INVOKEVIRTUAL on
     * {@code KafkaConsumer.commitSync(Duration)}. Descriptor
     * {@code (Ljava/time/Duration;)V}; the rule's predicate matches
     * only {@code ()V} and {@code (Ljava/util/Map;)V}.
     */
    public void pollLoopCommitBounded(KafkaConsumer<String, String> consumer) {
        consumer.commitSync(COMMIT_TIMEOUT);
    }

    /**
     * OK — graceful-shutdown commit with a deadline budgeted under the
     * Pod terminationGracePeriodSeconds; a slow coordinator surfaces a
     * TimeoutException that the shutdown sequence can log and proceed
     * with close() rather than overrunning the grace period.
     */
    public void gracefulShutdownBounded(KafkaConsumer<String, String> consumer) {
        consumer.commitSync(SHUTDOWN_COMMIT_TIMEOUT);
        consumer.close(Duration.ofSeconds(5));
    }

    /**
     * OK — bounded per-partition overload. Descriptor
     * {@code (Ljava/util/Map;Ljava/time/Duration;)V}; the rule's
     * predicate does not match.
     */
    public void perPartitionCommitBounded(
            KafkaConsumer<String, String> consumer,
            Map<TopicPartition, OffsetAndMetadata> offsets) {
        consumer.commitSync(offsets, COMMIT_TIMEOUT);
    }

    /**
     * OK — interface-typed receiver, bounded overload. INVOKEINTERFACE
     * on {@code Consumer.commitSync(Duration)V}.
     */
    public void commitViaInterfaceBounded(Consumer<String, String> consumer) {
        consumer.commitSync(COMMIT_TIMEOUT);
    }

    /**
     * OK — interface-typed receiver, bounded per-partition overload.
     */
    public void commitViaInterfacePerPartitionBounded(
            Consumer<String, String> consumer,
            Map<TopicPartition, OffsetAndMetadata> offsets) {
        consumer.commitSync(offsets, COMMIT_TIMEOUT);
    }

    /**
     * OK — scheduled bounded commit. The lambda wraps the bounded
     * overload; the INVOKEDYNAMIC site bound to {@link Runnable} bsm-
     * args contains a target handle pointing at
     * {@code KafkaConsumer.commitSync(Duration)V} (or a synthetic
     * lambda method that calls it) — descriptor mismatch on either
     * shape.
     */
    public ScheduledFuture<?> scheduleCommitBounded(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer) {
        return scheduler.scheduleAtFixedRate(
                () -> consumer.commitSync(COMMIT_TIMEOUT),
                0, 30, TimeUnit.SECONDS);
    }

    /**
     * OK — interface-typed receiver scheduled bounded commit.
     */
    public ScheduledFuture<?> scheduleCommitInterfaceBounded(
            ScheduledExecutorService scheduler,
            Consumer<String, String> consumer) {
        return scheduler.scheduleAtFixedRate(
                () -> consumer.commitSync(COMMIT_TIMEOUT),
                0, 30, TimeUnit.SECONDS);
    }

    /**
     * OK — JVM shutdown hook flushes offsets with a bounded timeout
     * budgeted under terminationGracePeriodSeconds. A slow coordinator
     * surfaces a TimeoutException rather than overshooting the grace
     * period and being SIGKILL'd.
     */
    public Thread shutdownHookBounded(KafkaConsumer<String, String> consumer) {
        Thread hook = new Thread(() -> consumer.commitSync(SHUTDOWN_COMMIT_TIMEOUT));
        Runtime.getRuntime().addShutdownHook(hook);
        return hook;
    }

    public static void main(String[] args) {
        new GoodConsumerCommitSyncNoTimeout()
                .pollLoopCommitBounded(new KafkaConsumer<>(new Properties()));
    }
}
