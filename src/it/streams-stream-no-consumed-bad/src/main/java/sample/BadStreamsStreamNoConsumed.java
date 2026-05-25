package sample;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_STREAM_NO_CONSUMED — must fire EXACTLY 8 times
 * across this class (one per method below).
 *
 * <p>Exercises all three unsafe overload descriptors:
 *
 * <ul>
 *   <li>{@code stream(String)} — UNSAFE</li>
 *   <li>{@code stream(Collection&lt;String&gt;)} — UNSAFE</li>
 *   <li>{@code stream(Pattern)} — UNSAFE</li>
 * </ul>
 */
public final class BadStreamsStreamNoConsumed {

    /** 1-arg String SAM matching {@code (String)KStream}. */
    @FunctionalInterface
    interface StreamFromTopicFactory<K, V> {
        KStream<K, V> apply(String topic);
    }

    // ===== Direct INVOKEVIRTUAL on the unsafe overloads =====

    /** MUST FIRE — direct INVOKEVIRTUAL on the String overload. */
    public KStream<String, String> streamOrders(StreamsBuilder builder) {
        return builder.stream("orders");
    }

    /** MUST FIRE — direct INVOKEVIRTUAL on the Collection overload. */
    public KStream<String, String> streamMulti(
            StreamsBuilder builder, Collection<String> topics) {
        return builder.stream(topics);
    }

    /** MUST FIRE — direct INVOKEVIRTUAL on the Pattern overload. */
    public KStream<String, String> streamPattern(
            StreamsBuilder builder, Pattern pattern) {
        return builder.stream(pattern);
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code builder::stream} bound to
     *  {@link Function Function&lt;String, KStream&gt;}. Indy
     *  bsm-arg points at {@code stream(String)KStream}. */
    public Function<String, KStream<String, String>>
            buildFromTopicFactory(StreamsBuilder builder) {
        return builder::stream;
    }

    /** MUST FIRE — {@code builder::stream} bound to
     *  {@link Function Function&lt;Pattern, KStream&gt;}. Indy
     *  bsm-arg points at {@code stream(Pattern)KStream}. */
    public Function<Pattern, KStream<String, String>>
            buildFromPatternFactory(StreamsBuilder builder) {
        return builder::stream;
    }

    /** MUST FIRE — {@code builder::stream} bound to a custom
     *  1-arg String SAM. Indy implMethod handle descriptor is the
     *  String unsafe overload exactly. */
    public StreamFromTopicFactory<String, String>
            buildCustomFactory(StreamsBuilder builder) {
        return builder::stream;
    }

    /** MUST FIRE — local SAM bound and applied immediately. The
     *  indy site is in this method's bytecode. */
    public KStream<String, String> useLocalFactory(StreamsBuilder builder) {
        Function<String, KStream<String, String>> factory = builder::stream;
        return factory.apply("local-topic");
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code builder.stream(topic)}. The synthetic lambda
     *  method's bytecode contains a direct INVOKEVIRTUAL on the
     *  String unsafe overload — the rule walks all methods on the
     *  class including synthetic lambda bodies and fires there. */
    public Stream<KStream<String, String>> streamAll(
            StreamsBuilder builder, List<String> topics) {
        return topics.stream().map(t -> builder.stream(t));
    }

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        System.out.println(builder);
        new BadStreamsStreamNoConsumed();
    }
}
