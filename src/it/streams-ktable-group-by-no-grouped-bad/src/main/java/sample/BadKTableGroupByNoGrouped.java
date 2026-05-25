package sample;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_KTABLE_GROUP_BY_NO_GROUPED — must fire EXACTLY 8
 * times across this class (one per method below).
 *
 * <p>Each method exercises one of the bytecode shapes the rule is
 * required to catch:
 *
 * <ol>
 *   <li>direct {@code INVOKEINTERFACE} on
 *       {@code KTable.groupBy(Lorg/apache/kafka/streams/kstream/
 *       KeyValueMapper;)Lorg/apache/kafka/streams/kstream/
 *       KGroupedTable;};</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code someKTable::groupBy} bound to a generic
 *       {@link Function Function&lt;KeyValueMapper, KGroupedTable&gt;};</li>
 *   <li>{@code INVOKEDYNAMIC} capture bound to a custom 1-arg SAM
 *       ({@code KTableRegrouper}, {@code OrderRegrouper});</li>
 *   <li>synthetic lambda body containing direct {@code
 *       INVOKEINTERFACE} —
 *       {@code .map(t -> t.groupBy(mapper))} — the rule walks all
 *       methods on the class including synthetic lambda bodies and
 *       fires there.</li>
 * </ol>
 */
public final class BadKTableGroupByNoGrouped {

    @FunctionalInterface
    interface KTableRegrouper<K1, V1> {
        KGroupedTable<K1, V1> regroup(
                KeyValueMapper<String, String, KeyValue<K1, V1>> mapper);
    }

    @FunctionalInterface
    interface OrderRegrouper {
        KGroupedTable<String, String> regroup(
                KeyValueMapper<String, String, KeyValue<String, String>> mapper);
    }

    // ===== Direct INVOKEINTERFACE on the 1-arg overload =====

    /**
     * MUST FIRE — direct INVOKEINTERFACE on the 1-arg overload. The
     * repartition topic that redistributes orders by customerId is
     * auto-named from the topology graph index; any upstream edit
     * renames it.
     */
    public KGroupedTable<String, String> ordersByCustomer(KTable<String, String> orders) {
        return orders.groupBy((k, v) -> KeyValue.pair(extractCustomerId(v), v));
    }

    /**
     * MUST FIRE — direct INVOKEINTERFACE on the 1-arg overload in a
     * chain that ends in count(Materialized). The auto-named
     * repartition topic is the load-bearing artifact here; count's
     * Materialized only names its OWN store, not the upstream
     * repartition.
     */
    public KTable<String, Long> paymentsByAccountCount(KTable<String, String> payments) {
        return payments
                .groupBy((k, v) -> KeyValue.pair(extractAccountId(v), v))
                .count(Materialized.as("payments-by-account-count"));
    }

    /**
     * MUST FIRE — direct INVOKEINTERFACE on the 1-arg overload in a
     * void topology-builder method. Users-by-tenant aggregation
     * restarts from zero on any upstream topology shift.
     */
    public void buildTopology(StreamsBuilder builder) {
        KTable<String, String> users = builder.table("users",
                Materialized.as("users-store"));
        users.groupBy((k, v) -> KeyValue.pair(extractTenantId(v), v))
                .count(Materialized.as("users-by-tenant-count"))
                .toStream();
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /**
     * MUST FIRE — {@code someKTable::groupBy} bound to a generic
     * {@link Function}. INVOKEDYNAMIC bsm-args contain a
     * REF_invokeInterface Handle pointing at
     * {@code KTable.groupBy(KeyValueMapper)KGroupedTable;}.
     */
    public Function<KeyValueMapper<String, String, KeyValue<String, String>>,
                    KGroupedTable<String, String>>
            buildFunctionFactory(KTable<String, String> table) {
        return table::groupBy;
    }

    /**
     * MUST FIRE — {@code someKTable::groupBy} bound to a custom
     * generic 1-arg SAM whose erased implMethod descriptor matches
     * the 1-arg overload.
     */
    public <K1, V1> KTableRegrouper<K1, V1> buildRegrouperFactory(KTable<String, String> table) {
        return table::groupBy;
    }

    /**
     * MUST FIRE — {@code someKTable::groupBy} bound to a custom
     * non-generic 1-arg SAM. Indy implMethod handle descriptor is
     * exactly {@code (Lorg/apache/kafka/streams/kstream/
     * KeyValueMapper;)Lorg/apache/kafka/streams/kstream/
     * KGroupedTable;}.
     */
    public OrderRegrouper buildOrderRegrouper(KTable<String, String> table) {
        return table::groupBy;
    }

    /**
     * MUST FIRE — local Function binding via method reference,
     * applied to a mapper inside the same method. The indy site is
     * in this method's bytecode; the synthetic lambda body that
     * calls {@code factory.apply(mapper)} does NOT contain a direct
     * INVOKEINTERFACE on groupBy (it goes through the Function
     * handle).
     */
    public KGroupedTable<String, String> useLocalFactory(KTable<String, String> orders,
            KeyValueMapper<String, String, KeyValue<String, String>> mapper) {
        Function<KeyValueMapper<String, String, KeyValue<String, String>>,
                KGroupedTable<String, String>> factory = orders::groupBy;
        return factory.apply(mapper);
    }

    /**
     * MUST FIRE — explicit lambda body that invokes the 1-arg
     * {@code t.groupBy(mapper)} overload. The synthetic lambda
     * method's bytecode contains a direct INVOKEINTERFACE on the
     * 1-arg overload — the rule walks all methods on the class
     * including synthetic lambda bodies and fires there.
     */
    public Stream<KGroupedTable<String, String>> regroupAll(
            List<KTable<String, String>> tables,
            KeyValueMapper<String, String, KeyValue<String, String>> mapper) {
        return tables.stream().map(t -> t.groupBy(mapper));
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
        new BadKTableGroupByNoGrouped().buildTopology(b);
    }
}
