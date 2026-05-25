package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * RULE: CONSUMER_COMMIT_ASYNC_NO_CALLBACK — must NOT fire on any
 * method below.
 *
 * <p>Mirror of {@code BadConsumerCommitAsyncNoCallback} where every
 * call site uses one of the two callback-carrying overloads:
 * {@code commitAsync(OffsetCommitCallback)} (descriptor
 * {@code (Lorg/apache/kafka/clients/consumer/OffsetCommitCallback;)V})
 * or {@code commitAsync(Map, OffsetCommitCallback)} (descriptor
 * {@code (Ljava/util/Map;Lorg/apache/kafka/clients/consumer/OffsetCommitCallback;)V}).
 * Neither matches the rule's no-callback descriptor.
 */
public final class GoodConsumerCommitAsyncNoCallback {

    @FunctionalInterface
    interface CommitTrigger {
        void fire();
    }

    private static final OffsetCommitCallback LOG_FAILURE = (offsets, exception) -> {
        if (exception != null) {
            System.err.println("commit failed for offsets " + offsets + ": " + exception);
        }
    };

    public void consumerLoopBatchCommitBounded(KafkaConsumer<String, String> consumer) {
        consumer.commitAsync(LOG_FAILURE);
    }

    public void heartbeatTickCommitBounded(KafkaConsumer<String, String> consumer) {
        consumer.commitAsync(LOG_FAILURE);
    }

    public void acmlessHelperInterfaceCommitBounded(Consumer<String, String> consumer) {
        consumer.commitAsync(LOG_FAILURE);
    }

    public CommitTrigger buildTriggerBounded(KafkaConsumer<String, String> consumer) {
        return () -> consumer.commitAsync(LOG_FAILURE);
    }

    public CommitTrigger buildTriggerInterfaceBounded(Consumer<String, String> consumer) {
        return () -> consumer.commitAsync(LOG_FAILURE);
    }

    public Runnable buildRunnableBounded(KafkaConsumer<String, String> consumer) {
        return () -> consumer.commitAsync(LOG_FAILURE);
    }

    public ScheduledFuture<?> scheduleCommitFixedRateBounded(
            ScheduledExecutorService scheduler,
            KafkaConsumer<String, String> consumer) {
        return scheduler.scheduleAtFixedRate(
                () -> consumer.commitAsync(LOG_FAILURE), 0, 5, TimeUnit.SECONDS);
    }

    public Future<?> submitCommitBounded(
            ExecutorService executor,
            Consumer<String, String> consumer,
            Map<TopicPartition, OffsetAndMetadata> offsets) {
        Runnable trigger = () -> consumer.commitAsync(offsets, LOG_FAILURE);
        return executor.submit(trigger);
    }

    public static void main(String[] args) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(new Properties())) {
            new GoodConsumerCommitAsyncNoCallback().consumerLoopBatchCommitBounded(c);
        }
    }
}
