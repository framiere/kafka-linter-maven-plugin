package sample;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Named;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_SELECT_KEY_NO_NAMED — must NOT fire on ANY method
 * below.
 *
 * <p>Mirror of {@code BadStreamsSelectKeyNoNamed} that uses the
 * {@code selectKey(KeyValueMapper, Named)} safe overload
 * everywhere the BAD class used the 1-arg unsafe overload. The
 * safe-overload descriptor is strictly different from the unsafe
 * descriptor the rule predicate matches against — so neither the
 * direct INVOKEINTERFACE sites nor the {@code stream::selectKey}
 * INVOKEDYNAMIC method-reference captures (which carry the
 * resolved safe-overload descriptor in their bsm-args
 * REF_invokeInterface Handle because the target SAM has the
 * matching parameter slots) match the rule predicate.
 *
 * <p>Every Named argument carries a stable domain-meaningful
 * processor node name so that the SelectKey node, the downstream
 * repartition topic, per-node JMX metrics, and ACL grants all
 * survive arbitrary topology edits.
 */
public final class GoodStreamsSelectKeyNoNamed {

    @FunctionalInterface
    interface SelectKeyWithNamedFactory<K, V, KR> {
        KStream<KR, V> apply(KeyValueMapper<? super K, ? super V, ? extends KR> mapper,
                Named named);
    }

    @FunctionalInterface
    interface StringSelectKeyWithNamedFactory {
        KStream<String, String> apply(KeyValueMapper<? super String, ? super String, String> mapper,
                Named named);
    }

    // ===== Direct INVOKEINTERFACE on the safe overload =====

    public KStream<String, String> rekeyByUser(KStream<String, String> events) {
        return events.selectKey((k, v) -> v, Named.as("rekey-by-user"));
    }

    public KStream<String, String> rekeyAndGroup(KStream<String, String> events) {
        return events.selectKey(
                (KeyValueMapper<String, String, String>) (k, v) -> v + "-suffix",
                Named.as("rekey-and-group"));
    }

    public KStream<String, String> rekeyFiltered(KStream<String, String> events) {
        return events.filter((k, v) -> v != null)
                .selectKey((k, v) -> v.toLowerCase(), Named.as("rekey-filtered"));
    }

    // ===== INVOKEDYNAMIC method-reference captures bound to safe
    //       SAMs — implMethod handle desc is (KeyValueMapper, Named)
    //       KStream — does NOT match rule predicate =====

    public BiFunction<KeyValueMapper<? super String, ? super String, String>, Named,
                      KStream<String, String>>
            buildBiFunctionFactory(KStream<String, String> events) {
        return events::selectKey;
    }

    public SelectKeyWithNamedFactory<String, String, String>
            buildSelectKeyFactory(KStream<String, String> events) {
        return events::selectKey;
    }

    public StringSelectKeyWithNamedFactory buildStringSelectKeyFactory(KStream<String, String> events) {
        return events::selectKey;
    }

    public KStream<String, String> useLocalFactory(KStream<String, String> events,
            KeyValueMapper<? super String, ? super String, String> mapper) {
        BiFunction<KeyValueMapper<? super String, ? super String, String>, Named,
                   KStream<String, String>> factory = events::selectKey;
        return factory.apply(mapper, Named.as("local-rekey"));
    }

    public Stream<KStream<String, String>> rekeyAll(
            KStream<String, String> events,
            List<KeyValueMapper<String, String, String>> mappers) {
        return mappers.stream().map(mapper ->
                events.selectKey(mapper, Named.as("rekey-all-" + mapper.hashCode())));
    }

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        System.out.println(builder);
        new GoodStreamsSelectKeyNoNamed();
    }
}
