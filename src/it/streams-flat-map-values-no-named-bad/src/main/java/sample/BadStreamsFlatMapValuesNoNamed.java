package sample;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_FLAT_MAP_VALUES_NO_NAMED — must fire EXACTLY 8
 * times across this class (one per method below).
 *
 * <p>Exercises the unsafe overload descriptor on KStream
 * ({@code flatMapValues(ValueMapper)}) via direct
 * INVOKEINTERFACE and via INVOKEDYNAMIC method-reference
 * captures.
 *
 * <p>KStream.flatMapValues is fan-out but NOT key-changing —
 * every fire is a latent observability-churn hazard (no
 * broker-disk component).
 */
public final class BadStreamsFlatMapValuesNoNamed {

    /** 1-arg SAM matching {@code (ValueMapper)KStream}. */
    @FunctionalInterface
    interface FlatMapValuesFactory<V, V2> {
        KStream<String, V2> apply(ValueMapper<? super V, ? extends Iterable<? extends V2>> mapper);
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overload =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KStream.flatMapValues(ValueMapper). */
    public KStream<String, String> explode(KStream<String, String> stream) {
        return stream.flatMapValues((ValueMapper<String, Iterable<String>>) v ->
                v == null ? Collections.<String>emptyList() : Collections.singletonList(v));
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the unsafe
     *  overload in a chain. */
    public KStream<String, Integer> explodeToLengths(KStream<String, String> stream) {
        return stream.flatMapValues((ValueMapper<String, Iterable<Integer>>) v ->
                v == null ? Collections.<Integer>emptyList() : Collections.singletonList(v.length()));
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the unsafe
     *  overload, distinct call site. */
    public KStream<String, String> identityExplode(KStream<String, String> stream) {
        return stream.flatMapValues((ValueMapper<String, Iterable<String>>) v ->
                Collections.singletonList(v));
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code stream::flatMapValues} bound to
     *  {@link Function Function&lt;ValueMapper, KStream&gt;}. */
    public Function<ValueMapper<String, Iterable<String>>, KStream<String, String>>
            buildExpandFactory(KStream<String, String> stream) {
        return stream::flatMapValues;
    }

    /** MUST FIRE — {@code stream::flatMapValues} bound to a
     *  generic Function at a second site. */
    public Function<ValueMapper<String, Iterable<Integer>>, KStream<String, Integer>>
            buildLengthExpandFactory(KStream<String, String> stream) {
        return stream::flatMapValues;
    }

    /** MUST FIRE — {@code stream::flatMapValues} bound to a
     *  custom 1-arg generic SAM. */
    public <V2> FlatMapValuesFactory<String, V2> buildCustomFactory(KStream<String, String> stream) {
        return stream::flatMapValues;
    }

    /** MUST FIRE — local Function binding via method
     *  reference, applied inside the same method. */
    public KStream<String, String> useLocalFactory(KStream<String, String> stream,
            ValueMapper<String, Iterable<String>> mapper) {
        Function<ValueMapper<String, Iterable<String>>, KStream<String, String>> factory =
                stream::flatMapValues;
        return factory.apply(mapper);
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code s.flatMapValues(mapper)}. */
    public Stream<KStream<String, String>> explodeAll(
            List<KStream<String, String>> streams,
            ValueMapper<String, Iterable<String>> mapper) {
        return streams.stream().map(s -> s.flatMapValues(mapper));
    }

    public static void main(String[] args) {
        new BadStreamsFlatMapValuesNoNamed();
    }
}
