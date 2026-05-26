package sample;

import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_GROUP_BY_NO_GROUPED — must fire EXACTLY 8 times
 * across this class (one per method below).
 *
 * <p>Exercises the unsafe single-argument overload descriptor on
 * {@link KStream} ({@code groupBy(KeyValueMapper)KGroupedStream})
 * via direct INVOKEINTERFACE and via INVOKEDYNAMIC method-
 * reference captures bound to {@link Function} and to custom
 * 1-arg SAMs.
 *
 * <p>The 1-arg {@code groupBy(KeyValueMapper)} call defers
 * naming of the auto-repartition topic, downstream state store,
 * and changelog topic to Streams' internal NodeIdAllocator —
 * triple-artifact graph-index-derived names (e.g. {@code app-
 * id-KSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition}).
 */
public final class BadStreamsGroupByNoGrouped {

    /** 1-arg SAM matching {@code (KeyValueMapper)KGroupedStream}. */
    @FunctionalInterface
    interface GroupByFactory<K, V, KR> {
        KGroupedStream<KR, V> apply(KeyValueMapper<? super K, ? super V, KR> mapper);
    }

    /** A second 1-arg SAM at a distinct site. */
    @FunctionalInterface
    interface AnotherGroupByFactory<K, V, KR> {
        KGroupedStream<KR, V> with(KeyValueMapper<? super K, ? super V, KR> mapper);
    }

    // ===== Direct INVOKEINTERFACE on the unsafe 1-arg overload =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KStream.groupBy(KeyValueMapper). */
    public KGroupedStream<String, String> groupByUserId(KStream<String, String> a) {
        return a.groupBy((k, v) -> v + "-userId");
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KStream.groupBy(KeyValueMapper) at a distinct call site. */
    public KGroupedStream<String, String> groupByOrderId(KStream<String, String> b) {
        return b.groupBy((k, v) -> v + "-orderId");
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KStream.groupBy(KeyValueMapper) at a third distinct
     *  call site. */
    public KGroupedStream<String, String> groupBySessionId(KStream<String, String> c) {
        return c.groupBy((k, v) -> v + "-sessionId");
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code stream::groupBy} bound to {@link
     *  Function}{@code <KeyValueMapper, KGroupedStream>}. Indy
     *  implMethod handle descriptor matches the no-Grouped
     *  overload exactly. */
    public Function<KeyValueMapper<? super String, ? super String, String>, KGroupedStream<String, String>>
            buildFunctionFactory(KStream<String, String> stream) {
        return stream::groupBy;
    }

    /** MUST FIRE — {@code stream::groupBy} bound to a custom
     *  1-arg SAM at a distinct site. */
    public GroupByFactory<String, String, String> buildCustomFactory(KStream<String, String> stream) {
        return stream::groupBy;
    }

    /** MUST FIRE — {@code stream::groupBy} bound to a second
     *  custom 1-arg SAM at a third distinct site. */
    public AnotherGroupByFactory<String, String, String> buildAnotherCustomFactory(KStream<String, String> stream) {
        return stream::groupBy;
    }

    /** MUST FIRE — local SAM binding via method reference,
     *  applied inside the same method. */
    public KGroupedStream<String, String> useLocalFactory(KStream<String, String> stream,
            KeyValueMapper<? super String, ? super String, String> mapper) {
        GroupByFactory<String, String, String> factory = stream::groupBy;
        return factory.apply(mapper);
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code s.groupBy(mapper)}. The synthetic
     *  {@code lambda$groupByAll$0} carries a direct
     *  INVOKEINTERFACE on the no-Grouped overload. */
    public Stream<KGroupedStream<String, String>> groupByAll(
            List<KStream<String, String>> streams,
            KeyValueMapper<? super String, ? super String, String> mapper) {
        return streams.stream().map(s -> s.groupBy(mapper));
    }

    public static void main(String[] args) {
        new BadStreamsGroupByNoGrouped();
    }
}
