package sample;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsMetadata;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * RULE: STREAMS_ALL_METADATA_FOR_STORE_DEPRECATED — must NOT fire on
 * any method below.
 *
 * <p>The four methods below reach the supported KIP-744 replacement
 * pair {@code metadataForAllStreamsClients()} and
 * {@code streamsMetadataForStore(String)}:
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code KafkaStreams.metadataForAllStreamsClients()} —
 *       distinct method name from {@code allMetadata}, so the rule's
 *       name filter rejects the site.</li>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code KafkaStreams.streamsMetadataForStore(String)} —
 *       distinct method name from {@code allMetadataForStore}.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code KafkaStreams::metadataForAllStreamsClients} — bsm-arg
 *       handle name is {@code metadataForAllStreamsClients}, not
 *       {@code allMetadata}.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code KafkaStreams::streamsMetadataForStore} — bsm-arg
 *       handle name is {@code streamsMetadataForStore}, not
 *       {@code allMetadataForStore}.</li>
 * </ol>
 *
 * <p>The new methods return collections of the new
 * {@code org.apache.kafka.streams.StreamsMetadata} type, which exposes
 * {@code standbyStateStoreNames()} and
 * {@code standbyTopicPartitions()}. Note the import: the new type is
 * in {@code org.apache.kafka.streams}, not in
 * {@code org.apache.kafka.streams.state}, and both types share the
 * simple name {@code StreamsMetadata}.
 */
public final class GoodAllMetadataForStore {

    public Collection<StreamsMetadata> directNewAllMetadata(KafkaStreams streams) {
        // DOES NOT FIRE — metadataForAllStreamsClients() is the
        // supported KIP-744 replacement for allMetadata(). Distinct
        // method name, so the rule's name filter rejects this site.
        return streams.metadataForAllStreamsClients();
    }

    public Collection<StreamsMetadata> directNewStreamsMetadataForStore(KafkaStreams streams) {
        // DOES NOT FIRE — streamsMetadataForStore(String) is the
        // supported replacement for allMetadataForStore(String).
        return streams.streamsMetadataForStore("my-store");
    }

    public Function<KafkaStreams, Collection<StreamsMetadata>> capturedNewAllMetadata() {
        // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture resolving
        // to KafkaStreams::metadataForAllStreamsClients. The bsm-arg
        // handle's name is `metadataForAllStreamsClients`, not
        // `allMetadata`.
        return KafkaStreams::metadataForAllStreamsClients;
    }

    public BiFunction<KafkaStreams, String, Collection<StreamsMetadata>> capturedNewStreamsMetadataForStore() {
        // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture resolving
        // to KafkaStreams::streamsMetadataForStore. The bsm-arg
        // handle's name is `streamsMetadataForStore`, not
        // `allMetadataForStore`.
        return KafkaStreams::streamsMetadataForStore;
    }
}
