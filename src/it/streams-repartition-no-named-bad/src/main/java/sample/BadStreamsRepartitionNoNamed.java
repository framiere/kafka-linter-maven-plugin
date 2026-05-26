package sample;

import org.apache.kafka.streams.kstream.KStream;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_REPARTITION_NO_NAMED — must fire EXACTLY 8
 * times across this class (one per method below).
 *
 * <p>Exercises the unsafe zero-argument overload descriptor on
 * {@link KStream} ({@code repartition()KStream}) via direct
 * INVOKEINTERFACE and via INVOKEDYNAMIC method-reference
 * captures bound to {@link Supplier} and to custom 0-arg SAMs.
 *
 * <p>The 0-arg {@code repartition()} call defers naming to
 * Streams' internal NodeIdAllocator (graph-index-derived topic
 * name like {@code app-id-KSTREAM-REPARTITION-0000000005-
 * repartition}), so any topology edit upstream orphans the old
 * repartition topic and cold-starts the downstream operator
 * chain.
 */
public final class BadStreamsRepartitionNoNamed {

    /** 0-arg SAM matching {@code ()KStream}. */
    @FunctionalInterface
    interface RepartitionFactory<K, V> {
        KStream<K, V> apply();
    }

    /** A second 0-arg SAM at a distinct site. */
    @FunctionalInterface
    interface AnotherRepartitionFactory<K, V> {
        KStream<K, V> get();
    }

    // ===== Direct INVOKEINTERFACE on the unsafe 0-arg overload =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KStream.repartition(). */
    public KStream<String, String> repartitionA(KStream<String, String> a) {
        return a.repartition();
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KStream.repartition() at a distinct call site. */
    public KStream<String, String> repartitionB(KStream<String, String> b) {
        return b.repartition();
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KStream.repartition() at a third distinct call site. */
    public KStream<String, String> repartitionC(KStream<String, String> c) {
        return c.repartition();
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code stream::repartition} bound to {@link
     *  Supplier}{@code <KStream>}. Indy implMethod handle
     *  descriptor matches the no-Repartitioned overload exactly. */
    public Supplier<KStream<String, String>> buildSupplierFactory(KStream<String, String> stream) {
        return stream::repartition;
    }

    /** MUST FIRE — {@code stream::repartition} bound to a
     *  custom 0-arg SAM at a distinct site. */
    public RepartitionFactory<String, String> buildCustomFactory(KStream<String, String> stream) {
        return stream::repartition;
    }

    /** MUST FIRE — {@code stream::repartition} bound to a second
     *  custom 0-arg SAM at a third distinct site. */
    public AnotherRepartitionFactory<String, String> buildAnotherCustomFactory(KStream<String, String> stream) {
        return stream::repartition;
    }

    /** MUST FIRE — local SAM binding via method reference,
     *  applied inside the same method. */
    public KStream<String, String> useLocalFactory(KStream<String, String> stream) {
        RepartitionFactory<String, String> factory = stream::repartition;
        return factory.apply();
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code s.repartition()}. The synthetic
     *  {@code lambda$repartitionAll$0} carries a direct
     *  INVOKEINTERFACE on the 0-arg overload. */
    public Stream<KStream<String, String>> repartitionAll(List<KStream<String, String>> streams) {
        return streams.stream().map(s -> s.repartition());
    }

    public static void main(String[] args) {
        new BadStreamsRepartitionNoNamed();
    }
}
