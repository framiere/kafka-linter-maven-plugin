package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.function.BiFunction;

/**
 * RULE: STREAMS_THROUGH_DEPRECATED — must fire on all three methods.
 *
 * <p>The three methods below exercise the three distinct bytecode
 * shapes the rule is required to catch — two direct calls for the
 * two deprecated overloads plus one {@code INVOKEDYNAMIC} method-ref
 * capture:
 *
 * <ol>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code KStream.through(String)}.</li>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code KStream.through(String, Produced)}.</li>
 *   <li>{@code INVOKEDYNAMIC} unbound method-ref capture
 *       {@code KStream::through} bound to a
 *       {@code BiFunction<KStream<K, V>, String, KStream<K, V>>}
 *       whose erased signature is
 *       {@code (Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;}.
 *       javac resolves the method-ref to the unnamed legacy through
 *       overload by matching the SAM's (receiver, arg-erasures,
 *       return-erasure) triple. The user-class bytecode at this site
 *       contains ZERO direct INVOKEINTERFACE on the legacy method —
 *       only the INVOKEDYNAMIC + LambdaMetafactory bridge.</li>
 * </ol>
 *
 * <h2>Why this method is deprecated</h2>
 *
 * <p>KIP-221 (Kafka Streams 2.6, August 2020) deprecated both
 * {@code through} overloads in favour of
 * {@code KStream.repartition([Repartitioned])}. The motivating
 * incident classes are: wrong-partition-count silently breaks
 * key-based downstream operators because the intermediate topic is
 * operator-provisioned rather than framework-provisioned; orphaned
 * topics accumulate when a topology is refactored to no longer use a
 * given intermediate; the single-arg overload wires serdes via
 * application-wide {@code StreamsConfig} defaults so a schema change
 * upstream of the {@code through} silently writes and reads with
 * mismatched serializers.
 */
public final class BadThrough {

    @SuppressWarnings("deprecation")
    public KStream<String, String> directThroughString(KStream<String, String> stream) {
        // MUST FIRE — through(String).
        return stream.through("intermediate-topic");
    }

    @SuppressWarnings("deprecation")
    public KStream<String, String> directThroughProduced(KStream<String, String> stream) {
        // MUST FIRE — through(String, Produced).
        return stream.through("intermediate-topic",
                Produced.with(Serdes.String(), Serdes.String()));
    }

    @SuppressWarnings("deprecation")
    public BiFunction<KStream<String, String>, String, KStream<String, String>> capturedThrough() {
        // MUST FIRE — INVOKEDYNAMIC unbound method-ref capture
        // targeting the deprecated through(String) overload. javac
        // emits an INVOKEDYNAMIC site whose bsm-args contain a
        // REF_invokeInterface handle on
        // (Ljava/lang/String;)Lorg/apache/kafka/streams/kstream/KStream;.
        // The user-class bytecode here contains ZERO direct
        // INVOKEINTERFACE on the legacy method.
        return KStream::through;
    }
}
