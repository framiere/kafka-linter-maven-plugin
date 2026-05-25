package sample;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_KSTREAM_FILTER_NO_NAMED — must fire EXACTLY 8
 * times across this class (one per method below).
 *
 * <p>Exercises both unsafe overload descriptors on KStream
 * ({@code filter(Predicate)}, {@code filterNot(Predicate)})
 * via direct INVOKEINTERFACE and via INVOKEDYNAMIC method-
 * reference captures.
 */
public final class BadStreamsKStreamFilterNoNamed {

    /** 1-arg SAM matching {@code (Predicate)KStream}. */
    @FunctionalInterface
    interface FilterFactory<K, V> {
        KStream<K, V> apply(Predicate<? super K, ? super V> predicate);
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overloads =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KStream.filter(Predicate). */
    public KStream<String, String> rejectFraud(KStream<String, String> txns) {
        return txns.filter((k, v) -> v != null && !v.startsWith("FRAUD-"));
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KStream.filterNot(Predicate). */
    public KStream<String, String> dropEmpty(KStream<String, String> stream) {
        return stream.filterNot((k, v) -> v == null || v.isBlank());
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the unsafe overload
     *  in a chain. */
    public KStream<String, String> filteredAndChained(KStream<String, String> stream) {
        return stream.filter((k, v) -> v != null && v.length() > 4);
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code stream::filter} bound to
     *  {@link Function Function&lt;Predicate, KStream&gt;}.
     *  Indy bsm-arg points at {@code filter(Predicate)KStream}. */
    public Function<Predicate<String, String>, KStream<String, String>>
            buildFilterFactory(KStream<String, String> stream) {
        return stream::filter;
    }

    /** MUST FIRE — {@code stream::filterNot} bound to
     *  {@link Function Function&lt;Predicate, KStream&gt;}.
     *  Indy bsm-arg points at {@code filterNot(Predicate)KStream}. */
    public Function<Predicate<String, String>, KStream<String, String>>
            buildFilterNotFactory(KStream<String, String> stream) {
        return stream::filterNot;
    }

    /** MUST FIRE — {@code stream::filter} bound to a custom
     *  1-arg generic SAM. Indy implMethod handle descriptor is
     *  the no-Named overload exactly. */
    public <K, V> FilterFactory<K, V> buildCustomFactory(KStream<K, V> stream) {
        return stream::filter;
    }

    /** MUST FIRE — local Function binding via method reference,
     *  applied to a predicate inside the same method. */
    public KStream<String, String> useLocalFactory(KStream<String, String> stream,
            Predicate<String, String> p) {
        Function<Predicate<String, String>, KStream<String, String>> factory = stream::filter;
        return factory.apply(p);
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code s.filter(predicate)}. The synthetic lambda
     *  method's bytecode contains a direct INVOKEINTERFACE on
     *  the no-Named overload. */
    public Stream<KStream<String, String>> filterAll(
            List<KStream<String, String>> streams,
            Predicate<String, String> p) {
        return streams.stream().map(s -> s.filter(p));
    }

    public static void main(String[] args) {
        new BadStreamsKStreamFilterNoNamed();
    }
}
