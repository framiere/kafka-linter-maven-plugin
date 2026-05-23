package sample;

import java.time.Duration;
import java.util.Collection;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

/**
 * RULE: POLL_IN_REBALANCE_CALLBACK.
 *
 * <p>Fires when a class implementing {@code ConsumerRebalanceListener}
 * calls {@code Consumer.poll(...)} inside any of the three callback
 * methods ({@code onPartitionsRevoked}, {@code onPartitionsAssigned},
 * {@code onPartitionsLost}). Calling {@code poll()} from inside one
 * of these callbacks re-enters the consumer's group state machine
 * while it is in the middle of a rebalance — which is a classic
 * "do not modify the collection you are iterating" mistake at the
 * consumer-protocol layer.
 *
 * <p>What's actually going on inside the consumer:
 * <ol>
 *   <li>{@code poll()} is the consumer's single-threaded event
 *       loop. Every interaction with the group coordinator
 *       (heartbeats, JoinGroup, SyncGroup, fetches, offset commits)
 *       happens on the poll thread, dispatched by poll() itself.</li>
 *   <li>When the group coordinator instructs this consumer to
 *       rebalance, the {@code ConsumerCoordinator} invokes the
 *       configured {@code ConsumerRebalanceListener}'s callback
 *       <em>synchronously from within</em> poll(). So the callstack
 *       at callback entry is, literally:
 *       {@code poll() → updateAssignmentMetadataIfNeeded()
 *       → maybeAutoCommitOffsetsSync() / invokePartitionsAssigned()
 *       → listener.onPartitionsAssigned(...)}.</li>
 *   <li>Inside that callback, the consumer's internal state is
 *       "REBALANCING". A reentrant call to {@code poll()} from
 *       this stack frame hits a guard that detects "you are
 *       already polling" and throws
 *       {@code IllegalStateException("Consumer is not subscribed
 *       to any topics or assigned any partitions")} or the more
 *       descriptive {@code KafkaException("This method is not
 *       reentrant")} depending on the kafka-clients version.</li>
 * </ol>
 *
 * <p>What IS legal inside the callback:
 * <ul>
 *   <li>{@code commitSync(...)} — explicitly designed for this
 *       hook; in onPartitionsRevoked it's the canonical place
 *       to flush in-progress offsets before the partition leaves
 *       you.</li>
 *   <li>{@code seek(...)} — assignment is fully populated at the
 *       start of onPartitionsAssigned, so seeking to a specific
 *       offset (e.g. an externally-stored offset from a database)
 *       is the canonical use of this callback.</li>
 *   <li>{@code position(...)}, {@code committed(...)} — read-only
 *       state queries; safe.</li>
 *   <li>{@code partitionsFor(...)}, {@code metrics()}, etc. —
 *       read-only metadata; safe.</li>
 * </ul>
 *
 * <p>Why the rule only flags poll():
 * <ol>
 *   <li>The other consumer methods either don't re-enter the
 *       coordinator loop (commitSync uses a separate request),
 *       or they're explicitly designed to be called from this
 *       callback (seek).</li>
 *   <li>poll() is the only method that is GUARANTEED to throw at
 *       runtime when called reentrantly. Everything else is
 *       either safe or a smaller bug we'd want to flag with a
 *       different rule.</li>
 *   <li>Confidence: HIGH — the bytecode signature is unambiguous:
 *       class implements ConsumerRebalanceListener (a single,
 *       well-known interface), the callback methods have a fixed
 *       name + descriptor {@code (Ljava/util/Collection;)V}, and
 *       the call inside is a MethodInsnNode whose owner is in
 *       PRODUCER_OWNERS and whose name is "poll". No false
 *       positives are physically possible.</li>
 * </ol>
 *
 * <p>Why this anti-pattern is so common:
 * <ol>
 *   <li>Developer wants to "drain in-flight records before the
 *       partition is taken away" in onPartitionsRevoked, and the
 *       most obvious code to express that is "poll a few more
 *       times to drain". It LOOKS right.</li>
 *   <li>Developer wants to "fetch some context" (e.g. read
 *       __consumer_offsets values, look up a sentinel record)
 *       inside onPartitionsAssigned and again writes
 *       {@code consumer.poll(Duration.ofMillis(100))} — the
 *       intuitive way to read from Kafka.</li>
 *   <li>The fix is unintuitive: do not poll, instead store any
 *       work that needs more records on a queue and let the
 *       OUTER poll loop handle it after the rebalance completes.</li>
 * </ol>
 *
 * <p>What the rule catches (per-class scan):
 * <ol>
 *   <li>Guard 1: {@code classNode.interfaces} must contain
 *       {@code "org/apache/kafka/clients/consumer/ConsumerRebalanceListener"}.
 *       If the class does not implement the listener, the entire
 *       class is skipped.</li>
 *   <li>Guard 2: only methods named {@code onPartitionsRevoked},
 *       {@code onPartitionsAssigned}, or {@code onPartitionsLost}
 *       with descriptor {@code (Ljava/util/Collection;)V} are
 *       scanned. Any helper methods on the listener class are
 *       skipped — even if they call poll() — because they aren't
 *       part of the callback contract that runs on the poll
 *       thread.</li>
 *   <li>Inside each callback, every {@code MethodInsnNode} whose
 *       owner is in {@code CONSUMER_OWNERS} and whose name is
 *       {@code "poll"} fires a violation at its source line.</li>
 * </ol>
 *
 * <p>This Bad class triggers exactly THREE fires — one per callback
 * method. The Good class file in the same fixture proves that other
 * consumer methods inside the same callbacks are silent.
 */
public final class BadPollInRebalanceCallback implements ConsumerRebalanceListener {

    private final Consumer<String, String> consumer;

    public BadPollInRebalanceCallback(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    /** Anti-pattern: poll() from onPartitionsRevoked to "drain" in-flight records. Throws at rebalance time. */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        consumer.poll(Duration.ofMillis(100)); // reported
    }

    /** Anti-pattern: poll() from onPartitionsAssigned to "look up context" before starting. Throws at rebalance time. */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        consumer.poll(Duration.ofMillis(50)); // reported
    }

    /** Anti-pattern: poll() from onPartitionsLost — same reentrance bug, also throws. */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        consumer.poll(Duration.ofMillis(10)); // reported
    }
}
