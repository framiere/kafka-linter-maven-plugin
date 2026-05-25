package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;

/**
 * RULE: ADMIN_LIST_CONSUMER_GROUP_OFFSETS_TOPIC_PARTITIONS_DEPRECATED —
 * must NOT fire on any of the four call sites below.
 *
 * <p>Each method exercises the KIP-709 migration target: rather than
 * setting the per-{@code ListConsumerGroupOffsetsOptions} TP filter
 * (which the batched API silently ignores on dispatch), the per-group
 * filter is carried on the
 * {@code ListConsumerGroupOffsetsSpec.topicPartitions(Collection)}
 * setter — a different owner class entirely, and the value the batched
 * dispatch actually reads.
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code ListConsumerGroupOffsetsSpec.topicPartitions(Collection)}.
 *       The owner at the call site is
 *       {@code org/apache/kafka/clients/admin/ListConsumerGroupOffsetsSpec}
 *       (NOT {@code ListConsumerGroupOffsetsOptions}), so the rule's
 *       owner filter rejects this site.</li>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code ListConsumerGroupOffsetsSpec.topicPartitions()} (zero-arg
 *       getter). Same owner-filter rejection as above; reading the
 *       per-Spec filter is the value the batched API actually uses on
 *       dispatch, so it is the operationally meaningful introspection.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code spec::topicPartitions} bound to a Function whose
 *       bsm-args contain a {@code REF_invokeVirtual} handle whose owner
 *       is {@code ListConsumerGroupOffsetsSpec} — rejected by the same
 *       owner filter.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code ListConsumerGroupOffsetsSpec::topicPartitions} unbound;
 *       again the bsm-args handle's owner is the Spec class, not the
 *       Options class.</li>
 * </ol>
 *
 * <p>The KIP-709 migration target closes the five incident classes that
 * justified the deprecation: the per-Spec filter is honoured on
 * dispatch (no silent over-fetch), the batched form fans out
 * coordinator RPCs in parallel (no N × RTT sequential cost), reading
 * the per-Spec filter is operationally meaningful (so introspection is
 * not lying), method-ref captures bind to the Spec class (so a
 * fluent-builder DSL that captures {@code spec::topicPartitions}
 * resolves to the post-KIP-709 owner), and the batched call site
 * {@code admin.listConsumerGroupOffsets(Map<String,
 * ListConsumerGroupOffsetsSpec>)} replaces the legacy single-group form
 * one-for-one.
 */
public final class GoodListConsumerGroupOffsetsTopicPartitions {

    public ListConsumerGroupOffsetsSpec directSetter(Collection<TopicPartition> tps) {
        // DOES NOT FIRE — direct INVOKEVIRTUAL on
        // ListConsumerGroupOffsetsSpec.topicPartitions(Collection). The
        // owner at the call site is
        // org/apache/kafka/clients/admin/ListConsumerGroupOffsetsSpec
        // (NOT ListConsumerGroupOffsetsOptions), so the rule's owner
        // filter rejects.
        ListConsumerGroupOffsetsSpec spec = new ListConsumerGroupOffsetsSpec();
        return spec.topicPartitions(tps);
    }

    public Collection<TopicPartition> directGetter() {
        // DOES NOT FIRE — direct INVOKEVIRTUAL on
        // ListConsumerGroupOffsetsSpec.topicPartitions() (zero-arg
        // getter). Same owner-filter rejection — reading the per-Spec
        // filter is the value the batched API actually reads on
        // dispatch, so this introspection has operational meaning.
        ListConsumerGroupOffsetsSpec spec = new ListConsumerGroupOffsetsSpec();
        return spec.topicPartitions();
    }

    public java.util.function.Function<Collection<TopicPartition>, ListConsumerGroupOffsetsSpec> capturedSetter() {
        // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture
        // `spec::topicPartitions` bound to a Function whose bsm-args
        // contain a REF_invokeVirtual handle whose owner is the Spec
        // class.
        ListConsumerGroupOffsetsSpec spec = new ListConsumerGroupOffsetsSpec();
        return spec::topicPartitions;
    }

    public java.util.function.Function<ListConsumerGroupOffsetsSpec, Collection<TopicPartition>> capturedGetter() {
        // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture
        // `ListConsumerGroupOffsetsSpec::topicPartitions` unbound; the
        // bsm-args handle's owner is the Spec class, not the Options
        // class.
        return ListConsumerGroupOffsetsSpec::topicPartitions;
    }

    /**
     * Exercises the batched migration target end-to-end:
     * {@code admin.listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec>)}.
     * The per-Spec TP filter is honoured on dispatch and the batched
     * form fans out coordinator RPCs in parallel.
     */
    public ListConsumerGroupOffsetsResult batchedMigrationTarget(
            Admin admin, String groupId, Collection<TopicPartition> tps) {
        return admin.listConsumerGroupOffsets(
                Map.of(groupId, new ListConsumerGroupOffsetsSpec().topicPartitions(tps)));
    }
}
