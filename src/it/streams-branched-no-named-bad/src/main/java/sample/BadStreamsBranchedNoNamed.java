package sample;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_BRANCHED_NO_NAMED — must fire EXACTLY 8 times
 * across this class (one per method below).
 *
 * <p>Single unsafe overload descriptor:
 * {@code (Lorg/apache/kafka/streams/kstream/Predicate;)
 *        Lorg/apache/kafka/streams/kstream/BranchedKStream;}.
 */
public final class BadStreamsBranchedNoNamed {

    /** 1-arg branch SAM matching {@code (Predicate)BranchedKStream}. */
    @FunctionalInterface
    interface BranchFactory<K, V> {
        BranchedKStream<K, V> apply(Predicate<? super K, ? super V> predicate);
    }

    private static Predicate<String, String> nonNull() {
        return (k, v) -> v != null;
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overload =====

    /** MUST FIRE — direct INVOKEINTERFACE single branch. */
    public BranchedKStream<String, String> branchOnce(KStream<String, String> stream) {
        return stream.split().branch(nonNull());
    }

    /** MUST FIRE — direct INVOKEINTERFACE first branch in a chain.
     *  Only the first branch is direct; the second is on the result
     *  of the first which is also a BranchedKStream — that second
     *  call ALSO fires. We isolate to one call here. */
    public BranchedKStream<String, String> branchAfterFilter(KStream<String, String> stream) {
        return stream.filter((k, v) -> v != null).split().branch(nonNull());
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the result of split()
     *  via a temporary variable. */
    public BranchedKStream<String, String> branchViaLocal(KStream<String, String> stream) {
        BranchedKStream<String, String> branched = stream.split();
        return branched.branch(nonNull());
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code branched::branch} bound to
     *  {@link Function Function&lt;Predicate, BranchedKStream&gt;}.
     *  INVOKEDYNAMIC bsm-args contain a REF_invokeInterface Handle
     *  pointing at {@code BranchedKStream.branch(Predicate)
     *  BranchedKStream}. */
    public Function<Predicate<String, String>, BranchedKStream<String, String>>
            buildFunctionFactory(KStream<String, String> stream) {
        BranchedKStream<String, String> branched = stream.split();
        return branched::branch;
    }

    /** MUST FIRE — {@code branched::branch} bound to a custom 1-arg
     *  SAM. Indy implMethod handle descriptor is the unsafe overload
     *  exactly. */
    public BranchFactory<String, String>
            buildCustomFactory(KStream<String, String> stream) {
        BranchedKStream<String, String> branched = stream.split();
        return branched::branch;
    }

    /** MUST FIRE — second indy site in the same class. */
    public BranchFactory<String, String>
            buildAnotherCustomFactory(KStream<String, String> stream) {
        BranchedKStream<String, String> branched = stream.split();
        BranchFactory<String, String> factory = branched::branch;
        return factory;
    }

    /** MUST FIRE — local SAM binding applied immediately. */
    public BranchedKStream<String, String> useLocalFactory(KStream<String, String> stream) {
        BranchedKStream<String, String> branched = stream.split();
        BranchFactory<String, String> factory = branched::branch;
        return factory.apply(nonNull());
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code b.branch(predicate)}. Synthetic lambda body. */
    public Stream<BranchedKStream<String, String>> branchAll(
            List<BranchedKStream<String, String>> bs) {
        return bs.stream().map(b -> b.branch(nonNull()));
    }

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        System.out.println(builder);
        new BadStreamsBranchedNoNamed();
    }
}
