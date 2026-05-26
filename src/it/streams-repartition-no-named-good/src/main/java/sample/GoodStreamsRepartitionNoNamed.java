package sample;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Repartitioned;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link Repartitioned} pinning the repartition topic name (and
 * therefore the processor-node ids, MBean keys, and runbook
 * targets).
 */
public final class GoodStreamsRepartitionNoNamed {

    /** 1-arg SAM matching {@code (Repartitioned)KStream}. */
    @FunctionalInterface
    interface RepartitionFactory<K, V> {
        KStream<K, V> apply(Repartitioned<K, V> r);
    }

    /** A second 1-arg SAM at a distinct site. */
    @FunctionalInterface
    interface AnotherRepartitionFactory<K, V> {
        KStream<K, V> with(Repartitioned<K, V> r);
    }

    private static final Repartitioned<String, String> R =
            Repartitioned.as("shared-repartition");

    public KStream<String, String> repartitionA(KStream<String, String> a) {
        return a.repartition(Repartitioned.as("repartition-a"));
    }

    public KStream<String, String> repartitionB(KStream<String, String> b) {
        return b.repartition(Repartitioned.as("repartition-b"));
    }

    public KStream<String, String> repartitionC(KStream<String, String> c) {
        return c.repartition(Repartitioned.as("repartition-c"));
    }

    public Function<Repartitioned<String, String>, KStream<String, String>> buildFunctionFactory(KStream<String, String> stream) {
        return stream::repartition;
    }

    public RepartitionFactory<String, String> buildCustomFactory(KStream<String, String> stream) {
        return stream::repartition;
    }

    public AnotherRepartitionFactory<String, String> buildAnotherCustomFactory(KStream<String, String> stream) {
        return stream::repartition;
    }

    public KStream<String, String> useLocalFactory(KStream<String, String> stream) {
        RepartitionFactory<String, String> factory = stream::repartition;
        return factory.apply(R);
    }

    public Stream<KStream<String, String>> repartitionAll(List<KStream<String, String>> streams) {
        return streams.stream().map(s -> s.repartition(R));
    }

    public static void main(String[] args) {
        new GoodStreamsRepartitionNoNamed();
    }
}
