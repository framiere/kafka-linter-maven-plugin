package sample;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_KTABLE_GROUP_BY_NO_GROUPED — must NOT fire on ANY
 * method below.
 *
 * <p>Mirror of {@code BadKTableGroupByNoGrouped} that uses the 2-arg
 * {@code groupBy(KeyValueMapper, Grouped)} overload everywhere the
 * BAD class used the unsafe 1-arg {@code groupBy(KeyValueMapper)}.
 * The 2-arg overload's descriptor is
 * {@code (Lorg/apache/kafka/streams/kstream/KeyValueMapper;Lorg/
 * apache/kafka/streams/kstream/Grouped;)Lorg/apache/kafka/streams/
 * kstream/KGroupedTable;} — strictly different from the 1-arg
 * unsafe descriptor — so neither the direct {@code INVOKEINTERFACE}
 * sites nor the {@code someKTable::groupBy} {@code INVOKEDYNAMIC}
 * method-reference captures (which carry the resolved 2-arg
 * descriptor in their bsm-args {@code REF_invokeInterface} Handle
 * because the target SAM has two parameter slots) match the rule
 * predicate.
 *
 * <p>Every {@link Grouped} argument carries a domain-meaningful
 * name so that the repartition topic and the downstream
 * aggregation's state store + changelog topic survive arbitrary
 * topology edits.
 */
public final class GoodKTableGroupByNoGrouped {

    @FunctionalInterface
    interface KTableNamedRegrouper<K1, V1> {
        KGroupedTable<K1, V1> regroup(
                KeyValueMapper<String, String, KeyValue<K1, V1>> mapper,
                Grouped<K1, V1> grouped);
    }

    @FunctionalInterface
    interface OrderNamedRegrouper {
        KGroupedTable<String, String> regroup(
                KeyValueMapper<String, String, KeyValue<String, String>> mapper,
                Grouped<String, String> grouped);
    }

    // ===== Direct INVOKEINTERFACE on the safe 2-arg overload =====

    public KGroupedTable<String, String> ordersByCustomer(KTable<String, String> orders) {
        return orders.groupBy(
                (k, v) -> KeyValue.pair(extractCustomerId(v), v),
                Grouped.as("orders-by-customer"));
    }

    public KTable<String, Long> paymentsByAccountCount(KTable<String, String> payments) {
        return payments
                .groupBy(
                        (k, v) -> KeyValue.pair(extractAccountId(v), v),
                        Grouped.as("payments-by-account"))
                .count(Materialized.as("payments-by-account-count"));
    }

    public void buildTopology(StreamsBuilder builder) {
        KTable<String, String> users = builder.table("users",
                Materialized.as("users-store"));
        users.groupBy(
                        (k, v) -> KeyValue.pair(extractTenantId(v), v),
                        Grouped.as("users-by-tenant"))
                .count(Materialized.as("users-by-tenant-count"))
                .toStream();
    }

    // ===== INVOKEDYNAMIC method-reference captures bound to 2-arg
    //       SAMs — implMethod handle desc is (KeyValueMapper, Grouped)
    //       KGroupedTable — does NOT match rule predicate =====

    public BiFunction<KeyValueMapper<String, String, KeyValue<String, String>>,
                      Grouped<String, String>,
                      KGroupedTable<String, String>>
            buildBiFunctionFactory(KTable<String, String> table) {
        return table::groupBy;
    }

    public <K1, V1> KTableNamedRegrouper<K1, V1> buildNamedRegrouperFactory(
            KTable<String, String> table) {
        return table::groupBy;
    }

    public OrderNamedRegrouper buildOrderNamedRegrouper(KTable<String, String> table) {
        return table::groupBy;
    }

    public KGroupedTable<String, String> useLocalFactory(KTable<String, String> orders,
            KeyValueMapper<String, String, KeyValue<String, String>> mapper) {
        BiFunction<KeyValueMapper<String, String, KeyValue<String, String>>,
                Grouped<String, String>,
                KGroupedTable<String, String>> factory = orders::groupBy;
        return factory.apply(mapper, Grouped.as("orders-local"));
    }

    public Stream<KGroupedTable<String, String>> regroupAll(
            List<KTable<String, String>> tables,
            KeyValueMapper<String, String, KeyValue<String, String>> mapper) {
        return tables.stream().map(t -> t.groupBy(mapper, Grouped.as("regroup-all")));
    }

    private static String extractCustomerId(String orderBody) {
        return orderBody.isEmpty() ? "anon" : orderBody.substring(0, 1);
    }

    private static String extractAccountId(String eventBody) {
        return eventBody.isEmpty() ? "anon" : eventBody.substring(0, 1);
    }

    private static String extractTenantId(String userBody) {
        return userBody.isEmpty() ? "anon" : userBody.substring(0, 1);
    }

    public static void main(String[] args) {
        StreamsBuilder b = new StreamsBuilder();
        new GoodKTableGroupByNoGrouped().buildTopology(b);
    }
}
