package sample;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.ThreadMetadata;

import java.util.Set;
import java.util.function.Function;

/**
 * RULE: STREAMS_LOCAL_THREADS_METADATA_DEPRECATED — must fire on both
 * methods.
 *
 * <p>The two methods below exercise the two distinct bytecode shapes
 * the rule is required to catch for the deprecated zero-arg
 * {@code KafkaStreams.localThreadsMetadata()} instance method:
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on the legacy method — the
 *       classic call site, where the user-class bytecode contains an
 *       explicit {@code INVOKEVIRTUAL
 *       org/apache/kafka/streams/KafkaStreams.localThreadsMetadata()Ljava/util/Set;}.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code streams::localThreadsMetadata} bound to a
 *       {@code Function<KafkaStreams, Set<ThreadMetadata>>} SAM —
 *       javac resolves the method-ref by matching the SAM's argument
 *       (the receiver, since this is an unbound method-ref) and
 *       return type against the available zero-arg instance methods,
 *       picking the legacy localThreadsMetadata(). javac emits an
 *       {@code INVOKEDYNAMIC} site whose bsm-args contain a
 *       {@code REF_invokeVirtual} handle pointing at
 *       {@code KafkaStreams.localThreadsMetadata()Ljava/util/Set;}.
 *       The user-class bytecode at this site contains ZERO direct
 *       {@code INVOKEVIRTUAL} on the legacy method — only the
 *       {@code INVOKEDYNAMIC} + {@code LambdaMetafactory} bridge. A
 *       name-only MethodInsnNode walk misses this case entirely; the
 *       rule's bsm-arg walk via {@code AsmUtil.indyTargetHandle}
 *       catches it.</li>
 * </ol>
 *
 * <h2>Why this method is deprecated</h2>
 *
 * <p>{@code KafkaStreams.localThreadsMetadata()} returns
 * {@code Set<org.apache.kafka.streams.processor.ThreadMetadata>} —
 * the processor-package legacy shape with a fixed field set
 * (threadName, threadState, activeTasks, standbyTasks,
 * consumerClientId). KIP-740 (Kafka Streams 3.0) migrated the
 * metadata API to a new top-level type
 * {@code org.apache.kafka.streams.ThreadMetadata} that adds
 * adminClientId, restoreConsumerClientId, producerClientIds(),
 * threadProducerClientId() and reshapes TaskMetadata to publish
 * topic-partition timestamps. The replacement method
 * {@code metadataForLocalThreads()} returns the modern shape.
 */
public final class BadLocalThreadsMetadata {

    @SuppressWarnings("deprecation")
    public Set<ThreadMetadata> snapshotDirect(KafkaStreams streams) {
        // MUST FIRE — direct INVOKEVIRTUAL on the deprecated method
        // KafkaStreams.localThreadsMetadata(). javac emits an
        // INVOKEVIRTUAL with descriptor ()Ljava/util/Set;.
        return streams.localThreadsMetadata();
    }

    @SuppressWarnings("deprecation")
    public Function<KafkaStreams, Set<ThreadMetadata>> capturedSnapshot() {
        // MUST FIRE — INVOKEDYNAMIC unbound method-ref capture
        // targeting the deprecated method. javac resolves
        // `KafkaStreams::localThreadsMetadata` by matching the SAM's
        // receiver arg (KafkaStreams) and return-type erasure
        // (Set) against the available zero-arg instance methods,
        // picking the legacy localThreadsMetadata(). javac emits an
        // INVOKEDYNAMIC site whose bsm-args contain a
        // REF_invokeVirtual handle on ()Ljava/util/Set;. The
        // user-class bytecode here contains ZERO direct
        // INVOKEVIRTUAL on the legacy method — only the
        // INVOKEDYNAMIC + LambdaMetafactory bridge. A name-only
        // MethodInsnNode walk misses this entirely; the rule's
        // bsm-arg walk catches it.
        return KafkaStreams::localThreadsMetadata;
    }
}
