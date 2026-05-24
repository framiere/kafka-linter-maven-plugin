package sample;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * RULE: CONSUMER_USED_AFTER_CLOSE.
 *
 * <p>Fires when a lifecycle method (poll / commitSync / commitAsync /
 * subscribe / unsubscribe / assign / seek / seekToBeginning / seekToEnd /
 * position / committed / beginningOffsets / endOffsets / offsetsForTimes /
 * pause / resume / paused / partitionsFor / listTopics / enforceRebalance /
 * metrics / groupMetadata / assignment / subscription / currentLag) is
 * invoked on a local-slot {@code KafkaConsumer} AFTER {@code close()} has
 * been called on the same slot earlier in the same method.
 *
 * <p>Why use-after-close on a consumer is a serious bug — what actually
 * happens at runtime:
 * <ol>
 *   <li>{@code KafkaConsumer.close()} is a ONE-WAY transition. It
 *       issues an explicit {@code LeaveGroup} to the group coordinator,
 *       releases the partition assignment, flushes any pending offset
 *       commits (best-effort, bounded by the timeout), shuts the
 *       heartbeat thread, closes the broker sockets, and sets an
 *       internal {@code closed} flag. The closed state is sticky —
 *       there is no "reopen()".</li>
 *   <li>Every subsequent consumer-facing call (poll, commitSync,
 *       commitAsync, subscribe, unsubscribe, assign, seek, position,
 *       committed, beginningOffsets, endOffsets, offsetsForTimes,
 *       pause, resume, paused, partitionsFor, listTopics,
 *       enforceRebalance, metrics, groupMetadata, assignment,
 *       subscription, currentLag) checks that flag first and throws
 *       {@code IllegalStateException: This consumer has already been
 *       closed}. The only method explicitly safe after close is
 *       {@code wakeup()}, which becomes a documented no-op.</li>
 *   <li>The synchronous IllegalStateException is acceptable when it
 *       propagates to a caller that catches and surfaces it. In
 *       practice the consumer is often used via an async path: a
 *       shutdown hook that races the poll loop, a worker pool that
 *       commits offsets while the main thread is in the middle of
 *       close(), a {@code CompletableFuture.thenApply(...)} that
 *       fires after the try-with-resources block has already exited.
 *       In those flows, the exception is swallowed by the framework
 *       (or logged at WARN and forgotten) and the LAST batch of
 *       offsets is SILENTLY DROPPED — the next consumer-group restart
 *       reprocesses every record since the previous successful commit,
 *       producing duplicates in any non-idempotent downstream system.</li>
 *   <li>The typical real-world shape: a finally-block that closes too
 *       eagerly, then a return path that still calls {@code commitSync()}
 *       or a shutdown hook that calls {@code poll()} once more "to drain
 *       the queue." The author thinks "commit then close" but writes
 *       "close then commit" by mistake. The bug does not show up in
 *       unit tests because the local IllegalStateException is loud —
 *       but it shows up in production where the call sits behind an
 *       executor or a Future.</li>
 * </ol>
 *
 * <p>What the rule catches (per-METHOD scan, local-slot tracking):
 * <ol>
 *   <li>Track every {@code new KafkaConsumer(...) + ASTORE N} — record
 *       slot N.</li>
 *   <li>For every method call whose receiver resolves to a tracked slot
 *       (via {@code AsmUtil.resolveReceiverSlot} — backwards stack
 *       simulation that handles nested arg expressions like
 *       {@code seek(new TopicPartition(...), 0L)}):
 *       <ul>
 *         <li>If the method name starts with {@code "close"}, add the
 *             slot to {@code closedSlots}.</li>
 *         <li>Otherwise, if the name is in the lifecycle set AND the
 *             slot is already in {@code closedSlots}, fire.</li>
 *       </ul></li>
 *   <li>Fire site is the lifecycle call (NOT the close call) — the
 *       operator wants to know where the offending use happens.</li>
 * </ol>
 *
 * <p>This Bad class triggers FIVE fires — five lifecycle operations each
 * performed after close() on the same slot.
 */
public final class BadConsumerUsedAfterClose {

    /** Anti-pattern: close() then poll(). IllegalStateException at runtime — but often swallowed in async paths. */
    public void closeThenPoll() {
        Properties props = baseConsumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.close();
        consumer.poll(Duration.ofSeconds(1)); // reported
    }

    /** Anti-pattern: close() then commitSync(). Last-batch offsets silently lost. */
    public void closeThenCommitSync() {
        Properties props = baseConsumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.close();
        consumer.commitSync(); // reported
    }

    /** Anti-pattern: close() then commitAsync(). Same outcome. */
    public void closeThenCommitAsync() {
        Properties props = baseConsumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.close();
        consumer.commitAsync(); // reported
    }

    /** Anti-pattern: close() then subscribe(). The "shutdown-then-re-subscribe" lifecycle misread. */
    public void closeThenSubscribe() {
        Properties props = baseConsumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.close();
        consumer.subscribe(List.of("orders")); // reported
    }

    /** Anti-pattern: close() then seek(new TopicPartition(...), 0L). Nested arg expression — exercises receiver-resolution. */
    public void closeThenSeekWithNestedArg() {
        Properties props = baseConsumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.close();
        consumer.seek(new TopicPartition("orders", 0), 0L); // reported
    }

    private static Properties baseConsumerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("group.id", "bad-consumer-used-after-close");
        p.put("client.id", "bad-consumer-used-after-close");
        p.put("auto.offset.reset", "earliest");
        p.put("enable.auto.commit", "false");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return p;
    }
}
