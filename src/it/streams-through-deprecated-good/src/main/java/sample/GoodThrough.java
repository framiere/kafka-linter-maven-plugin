package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Repartitioned;

import java.util.function.Function;

/**
 * RULE: STREAMS_THROUGH_DEPRECATED — must NOT fire.
 *
 * <p>The three methods below exercise the supported migration target
 * {@code KStream.repartition([Repartitioned])}:
 *
 * <ol>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code KStream.repartition()} — distinct method name from
 *       {@code through}, so the rule's name filter rejects the
 *       site.</li>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code KStream.repartition(Repartitioned)} — distinct method
 *       name.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code KStream::repartition} — bsm-arg handle name is
 *       {@code repartition}, not {@code through}.</li>
 * </ol>
 *
 * <h2>Why this is the right migration</h2>
 *
 * <p>KIP-221 (Kafka Streams 2.6, August 2020) introduced
 * {@code repartition()} as the framework-owned replacement for
 * {@code through(topic)}. The internal topic is auto-created with the
 * matching upstream partition count, named under the application ID so
 * {@code kafka-streams-application-reset} cleans it up transactionally,
 * and serdes are inferred from the upstream node type (or supplied
 * per-call via {@code Repartitioned.with(keySerde, valueSerde)}). The
 * three legacy footguns disappear: no manual partition-count
 * provisioning, no orphan topics, no application-wide serde leakage.
 */
public final class GoodThrough {

    public KStream<String, String> repartitionDefault(KStream<String, String> stream) {
        // DOES NOT FIRE — KStream.repartition() is the supported
        // KIP-221 replacement for through(String). Distinct method
        // name, so the rule's name filter rejects this site.
        return stream.repartition();
    }

    public KStream<String, String> repartitionConfigured(KStream<String, String> stream) {
        // DOES NOT FIRE — KStream.repartition(Repartitioned) is the
        // supported replacement for through(String, Produced). The
        // Repartitioned builder takes per-call serdes, an optional
        // partition count, and an optional stream-partitioner — all
        // of which through()/Produced could not express.
        return stream.repartition(
                Repartitioned.<String, String>as("by-customer")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
                        .withNumberOfPartitions(64));
    }

    public Function<KStream<String, String>, KStream<String, String>> capturedRepartition() {
        // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture resolving
        // to KStream::repartition. The bsm-arg handle's name is
        // `repartition`, not `through`.
        return KStream::repartition;
    }
}
