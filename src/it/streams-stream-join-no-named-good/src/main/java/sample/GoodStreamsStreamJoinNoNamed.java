package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link StreamJoined} pinning the join state-stores AND the
 * repartition topic names (and the changelog topic names).
 */
public final class GoodStreamsStreamJoinNoNamed {

    /** 4-arg SAM matching {@code (KStream, ValueJoiner, JoinWindows, StreamJoined)KStream}. */
    @FunctionalInterface
    interface JoinFactory<K, V, VO, VR> {
        KStream<K, VR> apply(KStream<K, VO> other,
                ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                JoinWindows windows,
                StreamJoined<K, V, VO> streamJoined);
    }

    private static final JoinWindows W = JoinWindows.of(Duration.ofMinutes(5));
    private static final StreamJoined<String, String, String> SJ =
            StreamJoined.<String, String, String>with(Serdes.String(), Serdes.String(), Serdes.String())
                    .withName("join-store");

    public KStream<String, String> innerJoin(KStream<String, String> a, KStream<String, String> b) {
        return a.join(b, (l, r) -> l + "+" + r, W,
                StreamJoined.<String, String, String>as("inner-join-store"));
    }

    public KStream<String, String> leftJoin(KStream<String, String> a, KStream<String, String> b) {
        return a.leftJoin(b, (l, r) -> l + "+" + (r == null ? "" : r), W,
                StreamJoined.<String, String, String>as("left-join-store"));
    }

    public KStream<String, String> outerJoin(KStream<String, String> a, KStream<String, String> b) {
        return a.outerJoin(b, (l, r) -> (l == null ? "" : l) + "+" + (r == null ? "" : r), W,
                StreamJoined.<String, String, String>as("outer-join-store"));
    }

    public <VR> JoinFactory<String, String, String, VR> buildJoinFactory(KStream<String, String> stream) {
        return stream::join;
    }

    public <VR> JoinFactory<String, String, String, VR> buildLeftJoinFactory(KStream<String, String> stream) {
        return stream::leftJoin;
    }

    public <VR> JoinFactory<String, String, String, VR> buildOuterJoinFactory(KStream<String, String> stream) {
        return stream::outerJoin;
    }

    public KStream<String, String> useLocalFactory(KStream<String, String> stream,
            KStream<String, String> other,
            ValueJoiner<String, String, String> joiner) {
        JoinFactory<String, String, String, String> factory = stream::join;
        return factory.apply(other, joiner, W, SJ);
    }

    public Stream<KStream<String, String>> joinAll(
            List<KStream<String, String>> streams,
            KStream<String, String> other,
            ValueJoiner<String, String, String> joiner) {
        return streams.stream().map(s -> s.join(other, joiner, W, SJ));
    }

    public static void main(String[] args) {
        new GoodStreamsStreamJoinNoNamed();
    }
}
