package sample;

import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_KTABLE_FILTER_NO_NAMED — must fire EXACTLY 8
 * times across this class (one per method below).
 *
 * <p>Exercises both unsafe overload descriptors on KTable
 * ({@code filter(Predicate)}, {@code filterNot(Predicate)})
 * via direct INVOKEINTERFACE and via INVOKEDYNAMIC method-
 * reference captures.
 */
public final class BadStreamsKTableFilterNoNamed {

    /** 1-arg SAM matching {@code (Predicate)KTable}. */
    @FunctionalInterface
    interface FilterFactory<K, V> {
        KTable<K, V> apply(Predicate<? super K, ? super V> predicate);
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overloads =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KTable.filter(Predicate). */
    public KTable<String, String> filterHighValue(KTable<String, String> orders) {
        return orders.filter((k, v) -> v != null && v.length() > 10);
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KTable.filterNot(Predicate). */
    public KTable<String, String> rejectNulls(KTable<String, String> orders) {
        return orders.filterNot((k, v) -> v == null);
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the unsafe overload
     *  inside a chain that ends in another KTable operator. The
     *  filter node is still anonymous; the downstream call does
     *  not paper over it. */
    public KTable<String, String> filteredAndChained(KTable<String, String> orders) {
        return orders.filter((k, v) -> v.startsWith("hv-"));
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code table::filter} bound to
     *  {@link Function Function&lt;Predicate, KTable&gt;}.
     *  Indy bsm-arg points at {@code filter(Predicate)KTable}. */
    public Function<Predicate<String, String>, KTable<String, String>>
            buildFilterFactory(KTable<String, String> table) {
        return table::filter;
    }

    /** MUST FIRE — {@code table::filterNot} bound to
     *  {@link Function Function&lt;Predicate, KTable&gt;}.
     *  Indy bsm-arg points at {@code filterNot(Predicate)KTable}. */
    public Function<Predicate<String, String>, KTable<String, String>>
            buildFilterNotFactory(KTable<String, String> table) {
        return table::filterNot;
    }

    /** MUST FIRE — {@code table::filter} bound to a custom 1-arg
     *  generic SAM. Indy implMethod handle descriptor is the
     *  no-Named overload exactly. */
    public <K, V> FilterFactory<K, V> buildCustomFactory(KTable<K, V> table) {
        return table::filter;
    }

    /** MUST FIRE — local Function binding via method reference,
     *  applied to a predicate inside the same method. The indy
     *  site is in this method's bytecode. */
    public KTable<String, String> useLocalFactory(KTable<String, String> orders,
            Predicate<String, String> p) {
        Function<Predicate<String, String>, KTable<String, String>> factory = orders::filter;
        return factory.apply(p);
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code t.filter(predicate)}. The synthetic lambda
     *  method's bytecode contains a direct INVOKEINTERFACE on
     *  the no-Named overload. */
    public Stream<KTable<String, String>> filterAll(
            List<KTable<String, String>> tables,
            Predicate<String, String> p) {
        return tables.stream().map(t -> t.filter(p));
    }

    public static void main(String[] args) {
        new BadStreamsKTableFilterNoNamed();
    }
}
