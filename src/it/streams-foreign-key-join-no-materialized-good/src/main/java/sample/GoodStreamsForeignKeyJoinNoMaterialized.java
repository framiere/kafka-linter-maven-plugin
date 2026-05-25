package sample;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_FOREIGN_KEY_JOIN_NO_MATERIALIZED — must NOT fire on
 * ANY method below.
 *
 * <p>Mirror of {@code BadStreamsForeignKeyJoinNoMaterialized} that
 * uses the {@code Materialized}-bearing overloads everywhere the
 * BAD class used the unsafe no-Materialized overloads. The
 * safe-overload descriptors are strictly different from the
 * predicate "contains Function, no Materialized" — so neither the
 * direct {@code INVOKEINTERFACE} sites nor the
 * {@code orders::join} {@code INVOKEDYNAMIC} method-reference
 * captures (which carry the resolved safe-overload descriptor in
 * their bsm-args {@code REF_invokeInterface} Handle because the
 * target SAM has the matching parameter slots) match the rule
 * predicate.
 *
 * <p>Every {@link Materialized} argument carries a stable
 * domain-meaningful result-store name so that the four FK-join
 * internal artifacts (subscription store + subscription topic +
 * response topic + response store) all derive from it and survive
 * topology edits.
 */
public final class GoodStreamsForeignKeyJoinNoMaterialized {

    @FunctionalInterface
    interface FkJoinWithMaterializedFactory<K, V, KO, VO, VR> {
        KTable<K, VR> apply(KTable<KO, VO> other,
                Function<V, KO> foreignKeyExtractor,
                ValueJoiner<V, VO, VR> joiner,
                Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);
    }

    @FunctionalInterface
    interface OrderCustomerJoinWithMaterializedFactory {
        KTable<String, String> apply(KTable<String, String> customers,
                Function<String, String> foreignKeyExtractor,
                ValueJoiner<String, String, String> joiner,
                Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized);
    }

    private static String extractCustomerId(String orderJson) {
        return orderJson;
    }

    private static String enrich(String order, String customer) {
        return order + "|" + customer;
    }

    // ===== Direct INVOKEINTERFACE on the safe overloads =====

    public KTable<String, String> ordersEnrichedByCustomer(
            KTable<String, String> orders, KTable<String, String> customers) {
        return orders.join(customers,
                GoodStreamsForeignKeyJoinNoMaterialized::extractCustomerId,
                (ValueJoiner<String, String, String>)
                        GoodStreamsForeignKeyJoinNoMaterialized::enrich,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(
                        "orders-by-customer-enriched"));
    }

    public KTable<String, String> ordersLeftEnrichedByCustomer(
            KTable<String, String> orders, KTable<String, String> customers) {
        return orders.leftJoin(customers,
                GoodStreamsForeignKeyJoinNoMaterialized::extractCustomerId,
                (ValueJoiner<String, String, String>)
                        GoodStreamsForeignKeyJoinNoMaterialized::enrich,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(
                        "orders-by-customer-left-enriched"));
    }

    public KTable<String, String> ordersEnrichedTableJoined(
            KTable<String, String> orders, KTable<String, String> customers) {
        return orders.join(customers,
                GoodStreamsForeignKeyJoinNoMaterialized::extractCustomerId,
                (ValueJoiner<String, String, String>)
                        GoodStreamsForeignKeyJoinNoMaterialized::enrich,
                TableJoined.as("orders-enriched-tj"),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(
                        "orders-by-customer-enriched-tj"));
    }

    public KTable<String, String> ordersLeftEnrichedTableJoined(
            KTable<String, String> orders, KTable<String, String> customers) {
        return orders.leftJoin(customers,
                GoodStreamsForeignKeyJoinNoMaterialized::extractCustomerId,
                (ValueJoiner<String, String, String>)
                        GoodStreamsForeignKeyJoinNoMaterialized::enrich,
                TableJoined.as("orders-left-enriched-tj"),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(
                        "orders-by-customer-left-enriched-tj"));
    }

    // ===== INVOKEDYNAMIC method-reference captures bound to safe
    //       SAMs — implMethod handle desc contains Materialized —
    //       does NOT match rule predicate =====

    public FkJoinWithMaterializedFactory<String, String, String, String, String>
            buildJoinFactory(KTable<String, String> orders) {
        return orders::join;
    }

    public FkJoinWithMaterializedFactory<String, String, String, String, String>
            buildLeftJoinFactory(KTable<String, String> orders) {
        return orders::leftJoin;
    }

    public KTable<String, String> useLocalFactory(
            KTable<String, String> orders, KTable<String, String> customers) {
        OrderCustomerJoinWithMaterializedFactory factory = orders::join;
        return factory.apply(customers,
                GoodStreamsForeignKeyJoinNoMaterialized::extractCustomerId,
                (ValueJoiner<String, String, String>)
                        GoodStreamsForeignKeyJoinNoMaterialized::enrich,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(
                        "local-store"));
    }

    public Stream<KTable<String, String>> enrichAll(
            KTable<String, String> orders, KTable<String, String> customers,
            List<Function<String, String>> extractors) {
        ValueJoiner<String, String, String> joiner =
                GoodStreamsForeignKeyJoinNoMaterialized::enrich;
        return extractors.stream().map(extractor ->
                orders.join(customers, extractor, joiner,
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(
                                "enrich-all-" + extractor.hashCode())));
    }

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        System.out.println(builder);
        new GoodStreamsForeignKeyJoinNoMaterialized();
    }
}
