package sample;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

/**
 * RULE: CONSUMER_COMMIT_ASYNC_NO_CALLBACK.
 *
 * <p>Fires when {@code consumer.commitAsync()} — the zero-argument
 * overload — is invoked. There is no callback, so the only signal
 * the application ever receives about a failed commit is... nothing.
 * Failures are silently swallowed.
 *
 * <p>How async commits fail in practice (any of these can drop your
 * last offset):
 * <ol>
 *   <li><b>Rebalance in progress.</b> If the group is rebalancing when
 *       the OffsetCommitRequest reaches the coordinator, the commit
 *       fails with {@code RebalanceInProgressException}. The consumer
 *       must re-commit after the rebalance settles — but with no
 *       callback, the application doesn't know it has to.</li>
 *   <li><b>Coordinator unreachable.</b> Transient broker outages,
 *       network partitions, or DNS issues cause the OffsetCommitRequest
 *       to time out. Without a callback, the timeout is invisible.</li>
 *   <li><b>Group authorization revoked.</b> If the consumer's ACLs are
 *       revoked mid-session (operations changes, key rotation, etc.),
 *       commits start failing with {@code GroupAuthorizationException}.
 *       Without a callback, the consumer keeps "processing" records
 *       that never get committed. On restart it reprocesses everything
 *       since the last successful commit.</li>
 *   <li><b>Offset out of range.</b> Edge case: if a topic-partition
 *       has been compacted past the committed offset, the commit can
 *       fail with {@code OffsetOutOfRangeException}. Without a
 *       callback, the consumer never logs this.</li>
 * </ol>
 *
 * <p>In every case, the no-callback shape means the application has
 * exactly zero signals to act on. Metrics, alerts, logs — all silent.
 * The bug manifests later as "consumer keeps re-processing the same
 * window of records after every restart" and the root cause is
 * essentially un-debuggable without enabling the missing callback.
 *
 * <p>What the rule catches: the rule is precise on bytecode. It only
 * fires for the {@code commitAsync()V} descriptor — i.e. the
 * truly-no-argument overload. The 1-arg overload
 * {@code commitAsync(OffsetCommitCallback)} and the 2-arg overload
 * {@code commitAsync(Map<TopicPartition, OffsetAndMetadata>, OffsetCommitCallback)}
 * both pass the rule's descriptor check and do NOT fire — those shapes
 * already have an observable failure mode through the callback.
 *
 * <p>Correct minimal pattern — just pass a callback that at least logs:
 * <pre>{@code
 *   consumer.commitAsync((offsets, exception) -> {
 *       if (exception != null) {
 *           log.error("commit failed for offsets {}", offsets, exception);
 *       }
 *   });
 * }</pre>
 *
 * <p>Note: this rule pairs naturally with COMMIT_ASYNC_NO_FINAL_SYNC.
 * A class with only no-callback commitAsync AND no final commitSync
 * is essentially running with NO commit reliability at all — failures
 * are invisible during the loop AND on shutdown.
 */
public final class BadConsumerCommitAsyncNoCallback {

    /** Anti-pattern: no-arg commitAsync() on Consumer interface — FIRES (descriptor ()V). */
    public void asyncCommitInterface(Consumer<String, String> consumer) {
        consumer.commitAsync(); // FIRES — failures go to /dev/null
    }

    /** Anti-pattern: no-arg commitAsync() on KafkaConsumer concrete — FIRES. */
    public void asyncCommitConcrete(KafkaConsumer<String, String> consumer) {
        consumer.commitAsync(); // FIRES — same bytecode shape via INVOKEVIRTUAL
    }

    /** Control: 1-arg commitAsync(OffsetCommitCallback) with a lambda callback — must NOT fire. */
    public void asyncCommitWithLambdaCallback(Consumer<String, String> consumer) {
        consumer.commitAsync((offsets, exception) -> {
            if (exception != null) {
                System.err.println("commit failed: " + exception.getMessage());
            }
        });
    }

    /** Control: 1-arg commitAsync with a Callback reference variable — must NOT fire. */
    public void asyncCommitWithCallbackVar(Consumer<String, String> consumer) {
        OffsetCommitCallback cb = (offsets, exception) -> { /* observed */ };
        consumer.commitAsync(cb);
    }

    /**
     * Control: 2-arg commitAsync(Map&lt;TopicPartition, OffsetAndMetadata&gt;, OffsetCommitCallback) —
     * must NOT fire (different overload, descriptor doesn't match ()V).
     */
    public void asyncCommitWithOffsetsAndCallback(Consumer<String, String> consumer,
                                                  TopicPartition partition,
                                                  long offset) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(partition, new OffsetAndMetadata(offset));
        consumer.commitAsync(offsets, (committedOffsets, exception) -> {
            if (exception != null) {
                System.err.println("targeted commit failed: " + exception.getMessage());
            }
        });
    }
}
