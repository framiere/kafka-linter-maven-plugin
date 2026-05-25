package sample;

import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link Named}, so the split-parent node carries a stable
 * user-chosen name AND every downstream branch map key inherits
 * the stable prefix.
 */
public final class GoodStreamsSplitNoNamed {

    /** 1-arg Named SAM matching {@code (Named)BranchedKStream}. */
    @FunctionalInterface
    interface SplitWithNamedFactory<K, V> {
        BranchedKStream<K, V> apply(Named named);
    }

    private static Named name(String s) {
        return Named.as(s);
    }

    public BranchedKStream<String, String> splitOrders(KStream<String, String> stream) {
        return stream.split(name("orders-split"));
    }

    public BranchedKStream<String, String> splitAfterFilter(KStream<String, String> stream) {
        return stream.filter((k, v) -> v != null).split(name("filter-split"));
    }

    public BranchedKStream<String, String> splitAfterMapValues(KStream<String, String> stream) {
        return stream.mapValues(v -> v.toUpperCase()).split(name("upper-split"));
    }

    public Function<Named, BranchedKStream<String, String>>
            buildFunctionFactory(KStream<String, String> stream) {
        return stream::split;
    }

    public SplitWithNamedFactory<String, String>
            buildCustomFactory(KStream<String, String> stream) {
        return stream::split;
    }

    public Function<Named, BranchedKStream<String, String>>
            buildAnotherFunction(KStream<String, String> stream) {
        return stream::split;
    }

    public BranchedKStream<String, String> useLocalFactory(KStream<String, String> stream) {
        SplitWithNamedFactory<String, String> factory = stream::split;
        return factory.apply(name("local-split"));
    }

    public Stream<BranchedKStream<String, String>> splitAll(
            KStream<String, String> stream, List<String> tags) {
        return tags.stream().map(t -> stream.split(name(t + "-split")));
    }

    public static void main(String[] args) {
        new GoodStreamsSplitNoNamed();
    }
}
