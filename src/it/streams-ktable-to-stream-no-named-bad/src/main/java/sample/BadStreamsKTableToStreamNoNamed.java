package sample;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_KTABLE_TO_STREAM_NO_NAMED — must fire EXACTLY
 * 8 times across this class (one per method below).
 *
 * <p>Exercises both unsafe overload descriptors on KTable
 * ({@code toStream()},
 * {@code toStream(KeyValueMapper)}) via direct INVOKEINTERFACE
 * and via INVOKEDYNAMIC method-reference captures.
 */
public final class BadStreamsKTableToStreamNoNamed {

    /** 0-arg SAM matching {@code ()KStream}. */
    @FunctionalInterface
    interface ToStreamFactory<K, V> {
        KStream<K, V> toStream();
    }

    /** 1-arg SAM matching {@code (KeyValueMapper)KStream}. */
    @FunctionalInterface
    interface RekeyFactory<K, V, KR> {
        KStream<KR, V> apply(KeyValueMapper<? super K, ? super V, ? extends KR> mapper);
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overloads =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KTable.toStream(). */
    public KStream<String, String> ordersToStream(KTable<String, String> orders) {
        return orders.toStream();
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KTable.toStream() at a second site. */
    public KStream<String, String> usersToStream(KTable<String, String> users) {
        return users.toStream();
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the key-changing
     *  KTable.toStream(KeyValueMapper) overload. */
    public KStream<String, String> ordersByCustomer(KTable<String, String> orders) {
        return orders.toStream((k, v) -> v == null ? "anon" : v.substring(0, 1));
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code table::toStream} bound to
     *  {@link Supplier Supplier&lt;KStream&gt;}.
     *  Indy bsm-arg points at {@code toStream()KStream}. */
    public Supplier<KStream<String, String>>
            buildSupplierFactory(KTable<String, String> table) {
        return table::toStream;
    }

    /** MUST FIRE — {@code table::toStream} bound to
     *  {@link Function Function&lt;KeyValueMapper, KStream&gt;}.
     *  Indy bsm-arg points at {@code toStream(KeyValueMapper)KStream}. */
    public Function<KeyValueMapper<String, String, String>, KStream<String, String>>
            buildRekeyFactory(KTable<String, String> table) {
        return table::toStream;
    }

    /** MUST FIRE — {@code table::toStream} bound to a custom
     *  0-arg generic SAM. Indy implMethod handle descriptor is
     *  the no-Named overload exactly. */
    public <K, V> ToStreamFactory<K, V> buildCustomFactory(KTable<K, V> table) {
        return table::toStream;
    }

    /** MUST FIRE — local Supplier binding via method reference,
     *  invoked inside the same method. The indy site is in this
     *  method's bytecode. */
    public KStream<String, String> useLocalFactory(KTable<String, String> orders) {
        Supplier<KStream<String, String>> factory = orders::toStream;
        return factory.get();
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code t.toStream()}. The synthetic lambda method's
     *  bytecode contains a direct INVOKEINTERFACE on the
     *  no-Named overload. */
    public Stream<KStream<String, String>> streamAll(
            List<KTable<String, String>> tables) {
        return tables.stream().map(t -> t.toStream());
    }

    public static void main(String[] args) {
        new BadStreamsKTableToStreamNoNamed();
    }
}
