package sample;

import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_STREAM_JOIN_NO_NAMED — must fire EXACTLY 8
 * times across this class (one per method below).
 *
 * <p>Exercises the unsafe overload descriptors on KStream
 * ({@code join(KStream, ValueJoiner, JoinWindows)},
 * {@code leftJoin(KStream, ValueJoiner, JoinWindows)},
 * {@code outerJoin(KStream, ValueJoiner, JoinWindows)}) via
 * direct INVOKEINTERFACE and via INVOKEDYNAMIC method-reference
 * captures.
 *
 * <p>KStream-KStream join with no StreamJoined is the worst-
 * case unnamed-node hazard — five broker-side artifacts churn
 * on every topology edit.
 */
public final class BadStreamsStreamJoinNoNamed {

    /** 3-arg SAM matching {@code (KStream, ValueJoiner, JoinWindows)KStream}. */
    @FunctionalInterface
    interface JoinFactory<K, V, VO, VR> {
        KStream<K, VR> apply(KStream<K, VO> other,
                ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                JoinWindows windows);
    }

    private static final JoinWindows W = JoinWindows.of(Duration.ofMinutes(5));

    // ===== Direct INVOKEINTERFACE on the unsafe overloads =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KStream.join(KStream, ValueJoiner, JoinWindows). */
    public KStream<String, String> innerJoin(KStream<String, String> a, KStream<String, String> b) {
        return a.join(b, (l, r) -> l + "+" + r, W);
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KStream.leftJoin(KStream, ValueJoiner, JoinWindows). */
    public KStream<String, String> leftJoin(KStream<String, String> a, KStream<String, String> b) {
        return a.leftJoin(b, (l, r) -> l + "+" + (r == null ? "" : r), W);
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KStream.outerJoin(KStream, ValueJoiner, JoinWindows). */
    public KStream<String, String> outerJoin(KStream<String, String> a, KStream<String, String> b) {
        return a.outerJoin(b, (l, r) -> (l == null ? "" : l) + "+" + (r == null ? "" : r), W);
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code stream::join} bound to a custom
     *  3-arg SAM. Indy implMethod handle descriptor matches the
     *  no-Named overload exactly. */
    public <VR> JoinFactory<String, String, String, VR> buildJoinFactory(KStream<String, String> stream) {
        return stream::join;
    }

    /** MUST FIRE — {@code stream::leftJoin} bound to a custom
     *  3-arg SAM at a distinct site. */
    public <VR> JoinFactory<String, String, String, VR> buildLeftJoinFactory(KStream<String, String> stream) {
        return stream::leftJoin;
    }

    /** MUST FIRE — {@code stream::outerJoin} bound to a custom
     *  3-arg SAM at a distinct site. */
    public <VR> JoinFactory<String, String, String, VR> buildOuterJoinFactory(KStream<String, String> stream) {
        return stream::outerJoin;
    }

    /** MUST FIRE — local SAM binding via method reference,
     *  applied inside the same method. */
    public KStream<String, String> useLocalFactory(KStream<String, String> stream,
            KStream<String, String> other,
            ValueJoiner<String, String, String> joiner) {
        JoinFactory<String, String, String, String> factory = stream::join;
        return factory.apply(other, joiner, W);
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code s.join(other, joiner, W)}. */
    public Stream<KStream<String, String>> joinAll(
            List<KStream<String, String>> streams,
            KStream<String, String> other,
            ValueJoiner<String, String, String> joiner) {
        return streams.stream().map(s -> s.join(other, joiner, W));
    }

    public static void main(String[] args) {
        new BadStreamsStreamJoinNoNamed();
    }
}
