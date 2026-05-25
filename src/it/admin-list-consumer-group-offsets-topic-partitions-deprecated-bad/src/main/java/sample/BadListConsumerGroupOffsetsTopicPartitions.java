package sample;

import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.function.Function;

/**
 * RULE: ADMIN_LIST_CONSUMER_GROUP_OFFSETS_TOPIC_PARTITIONS_DEPRECATED —
 * must fire on the four call sites below.
 *
 * <p>The four methods exercise the four distinct bytecode shapes the
 * rule is required to catch — direct {@code INVOKEVIRTUAL} on the
 * setter and the getter, plus {@code INVOKEDYNAMIC} method-ref captures
 * targeting each of those two methods:
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code ListConsumerGroupOffsetsOptions.topicPartitions(List)}.
 *       The descriptor at the call site is
 *       {@code (Ljava/util/List;)Lorg/apache/kafka/clients/admin/ListConsumerGroupOffsetsOptions;}
 *       — the legacy setter.</li>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code ListConsumerGroupOffsetsOptions.topicPartitions()}.
 *       The descriptor at the call site is
 *       {@code ()Ljava/util/List;} — the zero-arg getter; same
 *       deprecation timer as the setter.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code options::topicPartitions} bound to a
 *       {@code Function<List<TopicPartition>, ListConsumerGroupOffsetsOptions>}.
 *       The user-class bytecode at this site contains ZERO direct
 *       {@code INVOKEVIRTUAL} on the legacy setter — only the
 *       {@code INVOKEDYNAMIC} + {@code LambdaMetafactory} bridge whose
 *       bsm-args contain a {@code REF_invokeVirtual} handle pointing at
 *       the legacy setter, with desc
 *       {@code (Ljava/util/List;)Lorg/apache/kafka/clients/admin/ListConsumerGroupOffsetsOptions;}.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code ListConsumerGroupOffsetsOptions::topicPartitions} bound
 *       to a
 *       {@code Function<ListConsumerGroupOffsetsOptions, List<TopicPartition>>}.
 *       The bsm-args contain a {@code REF_invokeVirtual} handle pointing
 *       at the legacy getter, with desc
 *       {@code ()Ljava/util/List;}.</li>
 * </ol>
 *
 * <h2>Why these methods are deprecated (KIP-709 summary)</h2>
 *
 * <p>KIP-709 (Kafka 3.3, October 2022) split the
 * {@code Admin.listConsumerGroupOffsets} API into a batched form that
 * accepts a {@code Map<String, ListConsumerGroupOffsetsSpec>} where each
 * {@code ListConsumerGroupOffsetsSpec} carries its own per-group
 * topic-partition filter. The new batched overload silently IGNORES the
 * per-{@code ListConsumerGroupOffsetsOptions} TP filter — the filter set
 * via {@code options.topicPartitions(tps)} on the Options object is
 * never read by the batched code path; only the Spec's per-entry filter
 * matters. Five concrete incident classes the deprecation tracks:
 *
 * <ul>
 *   <li><b>Silent over-fetch.</b> A caller migrates from the legacy
 *       single-group form to the batched form but keeps calling
 *       {@code options.topicPartitions(tps)} as a defensive-coding
 *       habit. The batched call returns offsets for ALL partitions
 *       assigned to the group — not just the filter set — because the
 *       Options TP filter is silently dropped on dispatch. The caller's
 *       downstream per-partition lag computation iterates over
 *       partitions it did not request and either crashes on a missing
 *       entry in a partition-to-lag map or double-counts lag for
 *       partitions outside the intended scope.</li>
 *   <li><b>N × broker-RTT sequential fan-out before KIP-709.</b> The
 *       legacy single-group API serialised one OFFSET_FETCH RPC per
 *       group to the group's coordinator. Listing offsets for N groups
 *       took N × RTT in the worst case. The batched form fans out the N
 *       RPCs to all coordinators in parallel — typically reducing
 *       wall-clock time from "N seconds" to "one round-trip" for a
 *       cluster monitoring 100+ groups.</li>
 *   <li><b>Getter is part of the same deprecation.</b>
 *       {@code topicPartitions()} (the zero-arg getter) is also
 *       deprecated because reading the per-Options filter is meaningless
 *       once the call path is the batched form: the read value reflects
 *       whatever the caller previously stored, but the batched API
 *       ignores it on dispatch.</li>
 *   <li><b>INVOKEDYNAMIC capture path.</b> A code base that uses Options
 *       configuration via a fluent-builder DSL can capture the setter as
 *       {@code options::topicPartitions} bound to a Function-style SAM.
 *       The user-class bytecode contains zero direct
 *       {@code INVOKEVIRTUAL} on the legacy method — only the
 *       {@code INVOKEDYNAMIC} + {@code LambdaMetafactory} bridge whose
 *       bsm-args contain a {@code REF_invokeVirtual} handle pointing at
 *       the legacy method. A name-only MethodInsnNode walk misses this
 *       case entirely.</li>
 *   <li><b>Trivial migration with parallel RPC payoff.</b>
 *       {@code admin.listConsumerGroupOffsets(Map.of(groupId, new ListConsumerGroupOffsetsSpec().topicPartitions(tps)))}
 *       replaces the legacy form one-for-one and immediately benefits
 *       from the parallel coordinator fan-out.</li>
 * </ul>
 */
public final class BadListConsumerGroupOffsetsTopicPartitions {

    @SuppressWarnings("deprecation")
    public ListConsumerGroupOffsetsOptions directSetter(List<TopicPartition> tps) {
        // MUST FIRE — direct INVOKEVIRTUAL on
        // ListConsumerGroupOffsetsOptions.topicPartitions(List). The
        // descriptor at the call site is
        // (Ljava/util/List;)Lorg/apache/kafka/clients/admin/ListConsumerGroupOffsetsOptions;
        // — the legacy setter.
        ListConsumerGroupOffsetsOptions options = new ListConsumerGroupOffsetsOptions();
        return options.topicPartitions(tps);
    }

    @SuppressWarnings("deprecation")
    public List<TopicPartition> directGetter() {
        // MUST FIRE — direct INVOKEVIRTUAL on
        // ListConsumerGroupOffsetsOptions.topicPartitions(). The
        // descriptor at the call site is ()Ljava/util/List; — the
        // legacy getter; same deprecation as the setter because reading
        // the per-Options filter is meaningless under the batched
        // dispatch.
        ListConsumerGroupOffsetsOptions options = new ListConsumerGroupOffsetsOptions();
        return options.topicPartitions();
    }

    @SuppressWarnings("deprecation")
    public Function<List<TopicPartition>, ListConsumerGroupOffsetsOptions> capturedSetter() {
        // MUST FIRE — INVOKEDYNAMIC method-ref capture
        // `options::topicPartitions` bound to a
        // Function<List<TopicPartition>, ListConsumerGroupOffsetsOptions>.
        // The receiver `options` is captured; the SAM apply signature
        // (List -> ListConsumerGroupOffsetsOptions) erases to match the
        // legacy setter. The user-class bytecode contains ZERO direct
        // INVOKEVIRTUAL on the legacy method — only the INVOKEDYNAMIC
        // bridge whose bsm-args hold a REF_invokeVirtual handle whose
        // desc is
        // (Ljava/util/List;)Lorg/apache/kafka/clients/admin/ListConsumerGroupOffsetsOptions;.
        ListConsumerGroupOffsetsOptions options = new ListConsumerGroupOffsetsOptions();
        return options::topicPartitions;
    }

    @SuppressWarnings("deprecation")
    public Function<ListConsumerGroupOffsetsOptions, List<TopicPartition>> capturedGetter() {
        // MUST FIRE — INVOKEDYNAMIC method-ref capture
        // `ListConsumerGroupOffsetsOptions::topicPartitions` bound to a
        // Function<ListConsumerGroupOffsetsOptions, List<TopicPartition>>.
        // Unbound — the receiver is the SAM's first (and only)
        // argument. The bsm-args hold a REF_invokeVirtual handle whose
        // desc is ()Ljava/util/List; — the legacy getter.
        return ListConsumerGroupOffsetsOptions::topicPartitions;
    }
}
