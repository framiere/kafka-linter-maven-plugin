package sample;

import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/**
 * RULE: CONSUMER_ASSIGN_AND_SUBSCRIBE.
 *
 * <p>Fires when a class contains BOTH {@code consumer.assign(...)}
 * AND {@code consumer.subscribe(...)} calls. These two methods
 * configure the consumer's partition-acquisition mode, and they are
 * MUTUALLY EXCLUSIVE — the KafkaConsumer enforces this at runtime
 * by throwing {@code IllegalStateException} when the second of the
 * two is called on the same consumer instance.
 *
 * <p>Why mixing the two doesn't work:
 * <ol>
 *   <li>{@code subscribe(topics)} is the GROUP-MANAGED mode. The
 *       consumer joins a group via {@code group.id}, the broker's
 *       group coordinator assigns partitions, and rebalances happen
 *       automatically when group membership changes. Offsets are
 *       committed (by default) to {@code __consumer_offsets} on the
 *       coordinator and tracked per-group.</li>
 *   <li>{@code assign(partitions)} is the SELF-MANAGED mode. The
 *       application picks specific topic-partitions and owns them
 *       directly. There is no group coordinator interaction (the
 *       consumer doesn't even need a group.id), no automatic
 *       rebalancing, and no automatic partition revocation.</li>
 *   <li>The internal state machine that tracks subscription type
 *       has exactly three states: UNSET, GROUP-MANAGED, USER-MANAGED.
 *       Transitioning UNSET → GROUP-MANAGED is permitted (subscribe()
 *       on a fresh consumer); UNSET → USER-MANAGED is permitted
 *       (assign() on a fresh consumer). Any other transition —
 *       including GROUP-MANAGED → USER-MANAGED or vice versa —
 *       fails with {@code IllegalStateException("Subscription to
 *       topics, partitions and pattern are mutually exclusive")}.</li>
 * </ol>
 *
 * <p>What this looks like in production:
 * <ol>
 *   <li>A consumer was originally configured for group-managed
 *       consumption with {@code subscribe()}. Months later, a new
 *       feature needs "read from specific partitions for backfill"
 *       — and someone adds {@code consumer.assign(...)} thinking
 *       it overrides the subscription.</li>
 *   <li>Local tests don't exercise the new path through the same
 *       consumer instance (they typically create a fresh consumer
 *       per test).</li>
 *   <li>Production hits the IllegalStateException on first call to
 *       the new feature. Stack trace is unambiguous, but the bug
 *       is wired in at compile time — that's exactly what this rule
 *       catches.</li>
 * </ol>
 *
 * <p>What the rule catches: class-scope detection. It walks every
 * method body and records whether the class contains at least one
 * call to {@code assign} on a Consumer owner and at least one call
 * to {@code subscribe} on a Consumer owner. If BOTH are present in
 * the same class, it reports the {@code subscribe} site (and includes
 * the {@code assign} site's method+line in the message for context).
 *
 * <p>Confidence note: the rule does NOT track WHICH consumer instance
 * each call targets. A class that uses two distinct consumers — one
 * deliberately in assign-mode (e.g. an admin-style backfill consumer)
 * and one in subscribe-mode (the main poll loop) — would generate a
 * false positive. That shape is rare enough in real codebases that
 * the rule is marked MEDIUM confidence; the detection is intentionally
 * coarse-grained because tracking ownership across local variables
 * and fields would add significant complexity for marginal precision
 * gain.
 *
 * <p>This file contains THREE classes — one BAD (both assign and
 * subscribe), one CONTROL (subscribe only — the typical group-managed
 * shape), one CONTROL (assign only — the typical self-managed shape).
 * Each compiles to its own ClassNode, and the class-scope rule is
 * applied independently to each.
 */
public final class BadConsumerAssignAndSubscribe {

    /** Anti-pattern: class calls subscribe() and assign() on the same consumer. */
    public void subscribeForGroupConsumption(Consumer<String, String> consumer) {
        consumer.subscribe(List.of("orders", "payments")); // reported site (rule reports subscribe)
    }

    /** Anti-pattern: same class also calls assign() — triggers the cross-class fire. */
    public void overrideWithSpecificPartitions(Consumer<String, String> consumer,
                                               TopicPartition partition) {
        consumer.assign(List.of(partition)); // referenced in the rule's message
    }
}
