package sample;

import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_KTABLE_MAP_VALUES_NO_NAMED — must fire EXACTLY
 * 8 times across this class (one per method below).
 *
 * <p>Exercises the unsafe overload descriptors on KTable
 * ({@code mapValues(ValueMapper)},
 * {@code mapValues(ValueMapperWithKey)}) via direct
 * INVOKEINTERFACE and via INVOKEDYNAMIC method-reference
 * captures.
 */
public final class BadStreamsKTableMapValuesNoNamed {

    /** 1-arg SAM matching {@code (ValueMapper)KTable}. */
    @FunctionalInterface
    interface MapValuesFactory<V, V2> {
        KTable<String, V2> apply(ValueMapper<? super V, ? extends V2> mapper);
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overloads =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KTable.mapValues(ValueMapper). */
    public KTable<String, Integer> projectLength(KTable<String, String> orders) {
        return orders.mapValues((ValueMapper<String, Integer>) v -> v == null ? 0 : v.length());
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the unsafe overload
     *  in a chain that ends in another KTable operator. */
    public KTable<String, String> projectAndChain(KTable<String, String> orders) {
        return orders.mapValues((ValueMapper<String, String>) v -> v == null ? "" : v.toUpperCase());
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the unsafe overload
     *  again, distinct call site. */
    public KTable<String, String> truncateValue(KTable<String, String> orders) {
        return orders.mapValues((ValueMapper<String, String>) v -> v == null ? "" : v.substring(0, Math.min(8, v.length())));
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code table::mapValues} bound to
     *  {@link Function Function&lt;ValueMapper, KTable&gt;}.
     *  Indy bsm-arg points at {@code mapValues(ValueMapper)KTable}. */
    public Function<ValueMapper<String, Integer>, KTable<String, Integer>>
            buildProjectionFactory(KTable<String, String> table) {
        return table::mapValues;
    }

    /** MUST FIRE — {@code table::mapValues} bound to a generic
     *  {@code Function<ValueMapper, KTable>} at a second site. */
    public Function<ValueMapper<String, String>, KTable<String, String>>
            buildIdentityFactory(KTable<String, String> table) {
        return table::mapValues;
    }

    /** MUST FIRE — {@code table::mapValues} bound to a custom
     *  1-arg generic SAM. Indy implMethod handle descriptor is
     *  the no-Named overload exactly. */
    public <V2> MapValuesFactory<String, V2> buildCustomFactory(KTable<String, String> table) {
        return table::mapValues;
    }

    /** MUST FIRE — local Function binding via method reference,
     *  applied to a mapper inside the same method. The indy
     *  site is in this method's bytecode. */
    public KTable<String, Integer> useLocalFactory(KTable<String, String> orders,
            ValueMapper<String, Integer> mapper) {
        Function<ValueMapper<String, Integer>, KTable<String, Integer>> factory = orders::mapValues;
        return factory.apply(mapper);
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code t.mapValues(mapper)}. The synthetic lambda
     *  method's bytecode contains a direct INVOKEINTERFACE on
     *  the no-Named overload. */
    public Stream<KTable<String, Integer>> projectAll(
            List<KTable<String, String>> tables,
            ValueMapper<String, Integer> mapper) {
        return tables.stream().map(t -> t.mapValues(mapper));
    }

    public static void main(String[] args) {
        new BadStreamsKTableMapValuesNoNamed();
    }
}
