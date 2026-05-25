package sample;

import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.KStream;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_SPLIT_NO_NAMED — must fire EXACTLY 8 times
 * across this class (one per method below).
 *
 * <p>Exercises the single unsafe overload descriptor:
 *
 * <ul>
 *   <li>{@code split()} — UNSAFE (nullary)</li>
 * </ul>
 */
public final class BadStreamsSplitNoNamed {

    /** 0-arg SAM matching {@code ()BranchedKStream}. */
    @FunctionalInterface
    interface SplitFactory<K, V> {
        BranchedKStream<K, V> apply();
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overload =====

    /** MUST FIRE — direct INVOKEINTERFACE on split(). */
    public BranchedKStream<String, String> splitOrders(KStream<String, String> stream) {
        return stream.split();
    }

    /** MUST FIRE — direct INVOKEINTERFACE on split() after filter. */
    public BranchedKStream<String, String> splitAfterFilter(KStream<String, String> stream) {
        return stream.filter((k, v) -> v != null).split();
    }

    /** MUST FIRE — direct INVOKEINTERFACE on split() after a mapValues. */
    public BranchedKStream<String, String> splitAfterMapValues(KStream<String, String> stream) {
        return stream.mapValues(v -> v.toUpperCase()).split();
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code stream::split} bound to
     *  {@link Supplier Supplier&lt;BranchedKStream&gt;}. Indy
     *  bsm-arg points at {@code split()BranchedKStream}. */
    public Supplier<BranchedKStream<String, String>>
            buildSupplierFactory(KStream<String, String> stream) {
        return stream::split;
    }

    /** MUST FIRE — {@code stream::split} bound to a custom 0-arg
     *  SAM. Indy implMethod handle descriptor is the nullary
     *  unsafe overload exactly. */
    public SplitFactory<String, String>
            buildCustomFactory(KStream<String, String> stream) {
        return stream::split;
    }

    /** MUST FIRE — {@code stream::split} bound to a second
     *  Supplier instance. The indy site is in this method's
     *  bytecode. */
    public Supplier<BranchedKStream<String, String>>
            buildAnotherSupplier(KStream<String, String> stream) {
        return stream::split;
    }

    /** MUST FIRE — local SAM bound and applied immediately. The
     *  indy site is in this method's bytecode. */
    public BranchedKStream<String, String> useLocalFactory(KStream<String, String> stream) {
        Supplier<BranchedKStream<String, String>> factory = stream::split;
        return factory.get();
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code ks.split()}. The synthetic lambda method's
     *  bytecode contains a direct INVOKEINTERFACE on the nullary
     *  unsafe overload — the rule walks all methods on the class
     *  including synthetic lambda bodies and fires there. */
    public Stream<BranchedKStream<String, String>> splitAll(
            KStream<String, String> stream, List<String> tags) {
        return tags.stream().map(t -> stream.split());
    }

    public static void main(String[] args) {
        new BadStreamsSplitNoNamed();
    }
}
