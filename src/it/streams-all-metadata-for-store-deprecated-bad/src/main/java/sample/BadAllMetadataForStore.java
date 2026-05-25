package sample;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.StreamsMetadata;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * RULE: STREAMS_ALL_METADATA_FOR_STORE_DEPRECATED — must fire on all
 * four methods.
 *
 * <p>The four methods below exercise the four distinct bytecode shapes
 * the rule is required to catch — two direct calls for the two
 * deprecated method names plus two {@code INVOKEDYNAMIC} method-ref
 * captures (one per name):
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code KafkaStreams.allMetadata()} — zero-arg overload.</li>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code KafkaStreams.allMetadataForStore(String)} —
 *       single-String-arg overload.</li>
 *   <li>{@code INVOKEDYNAMIC} unbound method-ref capture
 *       {@code KafkaStreams::allMetadata} bound to a
 *       {@code Function<KafkaStreams, Collection<StreamsMetadata>>}.
 *       javac resolves the method-ref to the zero-arg legacy overload
 *       by matching the SAM's (receiver, return-erasure) triple — no
 *       arg position because the SAM takes only the receiver. The
 *       user-class bytecode at this site contains ZERO direct
 *       INVOKEVIRTUAL on the legacy method — only the INVOKEDYNAMIC
 *       + LambdaMetafactory bridge.</li>
 *   <li>{@code INVOKEDYNAMIC} unbound method-ref capture
 *       {@code KafkaStreams::allMetadataForStore} bound to a
 *       {@code BiFunction<KafkaStreams, String, Collection<StreamsMetadata>>}.
 *       Same indy mechanism as (3) but for the single-String-arg
 *       overload.</li>
 * </ol>
 *
 * <h2>Why these methods are deprecated</h2>
 *
 * <p>KIP-744 (Kafka Streams 3.0, September 2021) deprecated both
 * methods along with the {@code org.apache.kafka.streams.state.StreamsMetadata}
 * type they return. The old type predates standby-replica support
 * being first-class in Interactive Queries: it exposes only the
 * active host's stateStoreNames() and topicPartitions() with no
 * standby fields. Without standby awareness, IQ routers must wait for
 * the active to finish state-store restoration before serving queries
 * — minutes to hours of 503s on the IQ surface during pod restarts,
 * disk replacements, or node migrations. The new methods
 * {@code metadataForAllStreamsClients()} and
 * {@code streamsMetadataForStore(String)} return collections of
 * {@code org.apache.kafka.streams.StreamsMetadata} (the new type)
 * with {@code standbyStateStoreNames()} populated, letting the
 * router fail over to a warm standby host within seconds.
 */
public final class BadAllMetadataForStore {

    @SuppressWarnings("deprecation")
    public Collection<StreamsMetadata> directAllMetadata(KafkaStreams streams) {
        // MUST FIRE — allMetadata().
        return streams.allMetadata();
    }

    @SuppressWarnings("deprecation")
    public Collection<StreamsMetadata> directAllMetadataForStore(KafkaStreams streams) {
        // MUST FIRE — allMetadataForStore(String).
        return streams.allMetadataForStore("my-store");
    }

    @SuppressWarnings("deprecation")
    public Function<KafkaStreams, Collection<StreamsMetadata>> capturedAllMetadata() {
        // MUST FIRE — INVOKEDYNAMIC unbound method-ref capture
        // targeting the deprecated allMetadata() overload. javac
        // emits an INVOKEDYNAMIC site whose bsm-args contain a
        // REF_invokeVirtual handle on ()Ljava/util/Collection;. The
        // user-class bytecode here contains ZERO direct INVOKEVIRTUAL
        // on the legacy method.
        return KafkaStreams::allMetadata;
    }

    @SuppressWarnings("deprecation")
    public BiFunction<KafkaStreams, String, Collection<StreamsMetadata>> capturedAllMetadataForStore() {
        // MUST FIRE — INVOKEDYNAMIC unbound method-ref capture
        // targeting the deprecated allMetadataForStore(String)
        // overload. javac emits an INVOKEDYNAMIC site whose bsm-args
        // contain a REF_invokeVirtual handle on
        // (Ljava/lang/String;)Ljava/util/Collection;.
        return KafkaStreams::allMetadataForStore;
    }
}
