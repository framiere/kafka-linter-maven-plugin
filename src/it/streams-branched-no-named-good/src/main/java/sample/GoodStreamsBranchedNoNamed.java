package sample;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site uses the
 * {@code branch(Predicate, Branched)} overload, so each branch
 * carries a stable, user-chosen name across topology edits.
 */
public final class GoodStreamsBranchedNoNamed {

    /** 2-arg branch SAM matching {@code (Predicate, Branched)BranchedKStream}. */
    @FunctionalInterface
    interface BranchWithBranchedFactory<K, V> {
        BranchedKStream<K, V> apply(
                Predicate<? super K, ? super V> predicate,
                Branched<K, V> branched);
    }

    private static Predicate<String, String> nonNull() {
        return (k, v) -> v != null;
    }

    public BranchedKStream<String, String> branchOnce(KStream<String, String> stream) {
        return stream.split().branch(nonNull(), Branched.as("non-null"));
    }

    public BranchedKStream<String, String> branchAfterFilter(KStream<String, String> stream) {
        return stream.filter((k, v) -> v != null).split()
                .branch(nonNull(), Branched.as("filtered-non-null"));
    }

    public BranchedKStream<String, String> branchViaLocal(KStream<String, String> stream) {
        BranchedKStream<String, String> branched = stream.split();
        return branched.branch(nonNull(), Branched.as("via-local"));
    }

    public BiFunction<Predicate<String, String>, Branched<String, String>,
                      BranchedKStream<String, String>>
            buildFunctionFactory(KStream<String, String> stream) {
        BranchedKStream<String, String> branched = stream.split();
        return branched::branch;
    }

    public BranchWithBranchedFactory<String, String>
            buildCustomFactory(KStream<String, String> stream) {
        BranchedKStream<String, String> branched = stream.split();
        return branched::branch;
    }

    public BranchWithBranchedFactory<String, String>
            buildAnotherCustomFactory(KStream<String, String> stream) {
        BranchedKStream<String, String> branched = stream.split();
        BranchWithBranchedFactory<String, String> factory = branched::branch;
        return factory;
    }

    public BranchedKStream<String, String> useLocalFactory(KStream<String, String> stream) {
        BranchedKStream<String, String> branched = stream.split();
        BranchWithBranchedFactory<String, String> factory = branched::branch;
        return factory.apply(nonNull(), Branched.as("local-branch"));
    }

    public Stream<BranchedKStream<String, String>> branchAll(
            List<BranchedKStream<String, String>> bs) {
        return bs.stream().map(b -> b.branch(nonNull(), Branched.as("each-branch")));
    }

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        System.out.println(builder);
        new GoodStreamsBranchedNoNamed();
    }
}
