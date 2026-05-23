package sample;

import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/**
 * RULE: CONSUMER_SEEK_BEFORE_POLL.
 *
 * <p>Fires when {@code Consumer.seek(TopicPartition, long)} appears,
 * in linear bytecode order, after {@code Consumer.subscribe(...)} and
 * BEFORE any {@code Consumer.poll(...)} in the same method. At that
 * point the consumer's {@code assignment()} set is empty and the
 * seek call throws {@code IllegalStateException("No current
 * assignment for partition ...")}.
 *
 * <p>Why this happens (the lifecycle, in detail):
 * <ol>
 *   <li>{@code subscribe(topics)} only RECORDS the consumer's interest
 *       in a set of topics on the client side. It does NOT contact the
 *       broker, does NOT join the group, and does NOT populate
 *       {@code assignment()}. It's a purely local state change.</li>
 *   <li>The group join, partition assignment, and population of
 *       {@code assignment()} happen during the FIRST {@code poll()}.
 *       The first poll triggers the JoinGroup / SyncGroup round-trip
 *       with the group coordinator, then the broker tells this
 *       consumer which partitions it owns, and only THEN does
 *       {@code assignment()} become non-empty.</li>
 *   <li>{@code seek(topicPartition, offset)} requires that the given
 *       partition is currently in {@code assignment()}. The first
 *       thing the method does is
 *       {@code if (!subscriptions.isAssigned(partition)) throw new
 *       IllegalStateException(...)}. Between subscribe() and the
 *       first poll(), that check ALWAYS fails.</li>
 * </ol>
 *
 * <p>The fix that experienced Kafka users reach for: register a
 * {@code ConsumerRebalanceListener} when subscribing, and put the
 * {@code seek()} calls inside its {@code onPartitionsAssigned}
 * callback — that callback runs DURING poll(), AFTER assignment()
 * has been populated, so seek() works.
 *
 * <p>Alternative: switch to manual partition assignment with
 * {@code assign(...)} instead of {@code subscribe(...)}.
 * {@code assign()} synchronously populates {@code assignment()},
 * so a subsequent {@code seek()} in the same method is safe.
 *
 * <p>Why this anti-pattern is so common in practice:
 * <ol>
 *   <li>Developer wants "start from a specific offset" or "skip the
 *       backlog" semantics.</li>
 *   <li>The intuitive code reads like prose:
 *       "subscribe to the topic, then seek to offset 0, then poll."
 *       This LOOKS correct — the operations are in a sensible order.</li>
 *   <li>Local tests often pass because the test harness creates a
 *       fresh consumer and the IllegalStateException is masked by a
 *       try/catch, or the test only verifies the consumer was
 *       constructed.</li>
 *   <li>Production fails on first start-up with a confusing
 *       IllegalStateException that doesn't mention "you forgot to
 *       poll first."</li>
 * </ol>
 *
 * <p>What the rule catches: per-method state machine over MethodInsnNode
 * sites whose owner is a Consumer type. States: INITIAL → SUBSCRIBED
 * (on subscribe()) → POLLED (on poll()); MANUAL_ASSIGN (on assign()).
 * A {@code seek()} call only fires the rule when state == SUBSCRIBED
 * — i.e. subscribe has been seen but no poll has happened yet, and
 * the partition assignment did NOT come from an explicit assign().
 *
 * <p>State transitions in this rule:
 * <ul>
 *   <li>INITIAL + subscribe → SUBSCRIBED</li>
 *   <li>SUBSCRIBED + poll → POLLED</li>
 *   <li>POLLED + seek → silent (assignment is populated)</li>
 *   <li>any + assign → MANUAL_ASSIGN</li>
 *   <li>MANUAL_ASSIGN + seek → silent (assign() populated assignment)</li>
 *   <li>INITIAL + seek → silent (no subscribe was seen — could be
 *       a fragment of a larger lifecycle, scope is per-method)</li>
 * </ul>
 *
 * <p>The rule does NOT change state on seek itself — so multiple
 * consecutive seeks in the same broken method each fire their own
 * violation. That is intentional: each line is a distinct fix site
 * the developer needs to address.
 */
public final class BadConsumerSeekBeforePoll {

    /** Anti-pattern: subscribe then seek with no intervening poll. Throws at runtime. */
    public void startFromOffsetZero(Consumer<String, String> consumer,
                                    TopicPartition partition) {
        consumer.subscribe(List.of("orders"));
        consumer.seek(partition, 0L); // reported: assignment() is still empty
    }

    /** Anti-pattern: subscribe then seek to multiple partitions before any poll. */
    public void rewindMultiplePartitions(Consumer<String, String> consumer,
                                         TopicPartition p0,
                                         TopicPartition p1) {
        consumer.subscribe(List.of("orders", "payments"));
        consumer.seek(p0, 100L); // reported
        consumer.seek(p1, 200L); // reported (rule fires for each seek while state == SUBSCRIBED)
    }

    /** Anti-pattern: subscribe, do unrelated work, then seek — still no poll between. */
    public void seekAfterUnrelatedWork(Consumer<String, String> consumer,
                                       TopicPartition partition,
                                       long startOffset) {
        consumer.subscribe(List.of("audit-log"));
        long adjusted = Math.max(0L, startOffset - 1);     // unrelated computation
        String topic = partition.topic().toLowerCase();    // more unrelated work
        if (topic.isEmpty()) {
            return;
        }
        consumer.seek(partition, adjusted); // reported: unrelated code between subscribe and seek does not insert a poll
    }

    /** Helper that does NOT count as a poll for the state machine — Duration.ofMillis is not Consumer.poll. */
    private Duration backoff() {
        return Duration.ofMillis(500);
    }

    /** Anti-pattern: subscribe → unrelated Duration helper call → seek. The helper is not a Consumer.poll. */
    public void seekAfterNonPollCall(Consumer<String, String> consumer,
                                     TopicPartition partition) {
        consumer.subscribe(List.of("metrics"));
        Duration ignored = backoff();        // not a poll on a Consumer — state stays SUBSCRIBED
        consumer.seek(partition, ignored.toMillis()); // reported
    }
}
