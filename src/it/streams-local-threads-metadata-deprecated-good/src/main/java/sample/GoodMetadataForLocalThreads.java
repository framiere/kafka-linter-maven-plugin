package sample;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.ThreadMetadata;

import java.util.Set;
import java.util.function.Function;

/**
 * RULE: STREAMS_LOCAL_THREADS_METADATA_DEPRECATED — must NOT fire.
 *
 * <p>The two methods below exercise the supported migration targets:
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code KafkaStreams.metadataForLocalThreads()} — the
 *       grace-free replacement that returns
 *       {@code Set<org.apache.kafka.streams.ThreadMetadata>} with the
 *       full KIP-740 field set. Distinct method name from the legacy
 *       {@code localThreadsMetadata}, so the rule's name filter
 *       rejects the site.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code KafkaStreams::metadataForLocalThreads} bound to a
 *       {@code Function<KafkaStreams, Set<ThreadMetadata>>} — the
 *       bsm-arg handle's name is {@code metadataForLocalThreads},
 *       not {@code localThreadsMetadata}, so the rule's name filter
 *       rejects the site.</li>
 * </ol>
 *
 * <h2>Why this is the right migration</h2>
 *
 * <p>KIP-740 (Kafka Streams 3.0) introduced a new top-level
 * {@code org.apache.kafka.streams.ThreadMetadata} that adds
 * adminClientId, restoreConsumerClientId, producerClientIds(),
 * threadProducerClientId(), and reshapes TaskMetadata to publish
 * topic-partition timestamps — the fields ops teams need for
 * meaningful rebalance debugging and per-task latency reporting. The
 * import statement on this file references the new
 * {@code org.apache.kafka.streams.ThreadMetadata} (top-level), not
 * the legacy {@code org.apache.kafka.streams.processor.ThreadMetadata}
 * (deprecated processor-package), confirming a clean migration.
 *
 * <p>The replacement method has a distinct name from the legacy
 * {@code localThreadsMetadata}, so the rule's name filter
 * discriminates — no descriptor magic is required to keep the
 * supported migration target safe.
 */
public final class GoodMetadataForLocalThreads {

    public Set<ThreadMetadata> snapshotDirect(KafkaStreams streams) {
        // DOES NOT FIRE — metadataForLocalThreads is a distinct method
        // name from the legacy localThreadsMetadata, so the name
        // filter rejects the site. Returns the modern top-level
        // ThreadMetadata (note the import — org.apache.kafka.streams,
        // not org.apache.kafka.streams.processor).
        return streams.metadataForLocalThreads();
    }

    public Function<KafkaStreams, Set<ThreadMetadata>> capturedSnapshot() {
        // DOES NOT FIRE — INVOKEDYNAMIC unbound method-ref capture
        // resolving to metadataForLocalThreads. The bsm-arg handle's
        // name is metadataForLocalThreads, not localThreadsMetadata,
        // so the rule's name filter rejects the site.
        return KafkaStreams::metadataForLocalThreads;
    }
}
