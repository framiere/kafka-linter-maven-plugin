package sample;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_FOREIGN_KEY_JOIN_NO_MATERIALIZED — must fire
 * EXACTLY 8 times across this class (one per method below).
 *
 * <p>Exercises every unsafe FK-join descriptor shape:
 *
 * <ul>
 *   <li>3-arg {@code (KTable, Function, ValueJoiner)} on
 *       {@code join};</li>
 *   <li>3-arg {@code (KTable, Function, ValueJoiner)} on
 *       {@code leftJoin};</li>
 *   <li>4-arg {@code (KTable, Function, ValueJoiner, Named)} on
 *       {@code join};</li>
 *   <li>4-arg {@code (KTable, Function, ValueJoiner, TableJoined)}
 *       on {@code leftJoin};</li>
 * </ul>
 *
 * <p>And the matching INVOKEDYNAMIC shapes:
 *
 * <ul>
 *   <li>{@code orders::join} bound to a 3-arg custom SAM;</li>
 *   <li>{@code orders::leftJoin} bound to a 3-arg custom SAM;</li>
 *   <li>Local 3-arg SAM binding applied immediately;</li>
 *   <li>Synthetic lambda body containing a direct
 *       INVOKEINTERFACE.</li>
 * </ul>
 */
public final class BadStreamsForeignKeyJoinNoMaterialized {

    /** 3-arg FK-join SAM — implMethod desc matches unsafe 3-arg. */
    @FunctionalInterface
    interface FkJoinFactory<K, V, KO, VO, VR> {
        KTable<K, VR> apply(KTable<KO, VO> other,
                Function<V, KO> foreignKeyExtractor,
                ValueJoiner<V, VO, VR> joiner);
    }

    /** Non-generic 3-arg FK-join SAM. */
    @FunctionalInterface
    interface OrderCustomerJoinFactory {
        KTable<String, String> apply(KTable<String, String> customers,
                Function<String, String> foreignKeyExtractor,
                ValueJoiner<String, String, String> joiner);
    }

    private static String extractCustomerId(String orderJson) {
        return orderJson;
    }

    private static String enrich(String order, String customer) {
        return order + "|" + customer;
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overloads =====

    /**
     * MUST FIRE — 3-arg {@code KTable.join(KTable, Function,
     * ValueJoiner)}. All four FK-join artifacts (subscription
     * store, subscription topic, response store, response topic)
     * are auto-named from the topology graph index.
     */
    public KTable<String, String> ordersEnrichedByCustomer(
            KTable<String, String> orders, KTable<String, String> customers) {
        return orders.join(customers,
                BadStreamsForeignKeyJoinNoMaterialized::extractCustomerId,
                (ValueJoiner<String, String, String>)
                        BadStreamsForeignKeyJoinNoMaterialized::enrich);
    }

    /**
     * MUST FIRE — 3-arg {@code KTable.leftJoin(KTable, Function,
     * ValueJoiner)}. Same auto-naming hazard as the inner join.
     */
    public KTable<String, String> ordersLeftEnrichedByCustomer(
            KTable<String, String> orders, KTable<String, String> customers) {
        return orders.leftJoin(customers,
                BadStreamsForeignKeyJoinNoMaterialized::extractCustomerId,
                (ValueJoiner<String, String, String>)
                        BadStreamsForeignKeyJoinNoMaterialized::enrich);
    }

    /**
     * MUST FIRE — 4-arg {@code KTable.join(KTable, Function,
     * ValueJoiner, Named)}. Named labels the processor node but
     * does NOT materialize the result store — all four
     * FK-internal artifacts are still auto-named.
     */
    public KTable<String, String> ordersEnrichedNamed(
            KTable<String, String> orders, KTable<String, String> customers) {
        return orders.join(customers,
                BadStreamsForeignKeyJoinNoMaterialized::extractCustomerId,
                (ValueJoiner<String, String, String>)
                        BadStreamsForeignKeyJoinNoMaterialized::enrich,
                Named.as("orders-enriched-named"));
    }

    /**
     * MUST FIRE — 4-arg {@code KTable.leftJoin(KTable, Function,
     * ValueJoiner, TableJoined)}. TableJoined names the
     * subscription / response repartition topics but does NOT
     * materialize the result store, and the two state stores
     * remain auto-named.
     */
    public KTable<String, String> ordersLeftEnrichedTableJoined(
            KTable<String, String> orders, KTable<String, String> customers) {
        return orders.leftJoin(customers,
                BadStreamsForeignKeyJoinNoMaterialized::extractCustomerId,
                (ValueJoiner<String, String, String>)
                        BadStreamsForeignKeyJoinNoMaterialized::enrich,
                TableJoined.as("orders-left-enriched-tj"));
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /**
     * MUST FIRE — {@code orders::join} bound to a custom 3-arg
     * FK-join SAM. INVOKEDYNAMIC bsm-args contain a
     * REF_invokeInterface Handle pointing at the 3-arg unsafe
     * overload (descriptor contains Function, no Materialized).
     */
    public FkJoinFactory<String, String, String, String, String>
            buildJoinFactory(KTable<String, String> orders) {
        return orders::join;
    }

    /**
     * MUST FIRE — {@code orders::leftJoin} bound to a custom
     * 3-arg FK-join SAM. INVOKEDYNAMIC bsm-args contain a
     * REF_invokeInterface Handle pointing at the 3-arg unsafe
     * leftJoin overload.
     */
    public FkJoinFactory<String, String, String, String, String>
            buildLeftJoinFactory(KTable<String, String> orders) {
        return orders::leftJoin;
    }

    /**
     * MUST FIRE — local non-generic 3-arg SAM bound to
     * {@code orders::join} and applied inside the same method.
     * The indy site is in this method's bytecode; the synthetic
     * lambda body that calls {@code factory.apply(...)} goes
     * through the SAM handle, not directly through join.
     */
    public KTable<String, String> useLocalFactory(
            KTable<String, String> orders, KTable<String, String> customers) {
        OrderCustomerJoinFactory factory = orders::join;
        return factory.apply(customers,
                BadStreamsForeignKeyJoinNoMaterialized::extractCustomerId,
                (ValueJoiner<String, String, String>)
                        BadStreamsForeignKeyJoinNoMaterialized::enrich);
    }

    /**
     * MUST FIRE — explicit lambda body that invokes the 3-arg
     * FK-join directly. The synthetic lambda method's bytecode
     * contains a direct INVOKEINTERFACE on the 3-arg unsafe
     * overload — the rule walks all methods on the class
     * including synthetic lambda bodies and fires there.
     */
    public Stream<KTable<String, String>> enrichAll(
            KTable<String, String> orders, KTable<String, String> customers,
            List<Function<String, String>> extractors) {
        ValueJoiner<String, String, String> joiner =
                BadStreamsForeignKeyJoinNoMaterialized::enrich;
        return extractors.stream().map(extractor ->
                orders.join(customers, extractor, joiner));
    }

    public static void main(String[] args) {
        // Reference StreamsBuilder so the import isn't pruned by a future tool.
        StreamsBuilder builder = new StreamsBuilder();
        System.out.println(builder);
        new BadStreamsForeignKeyJoinNoMaterialized();
    }
}
