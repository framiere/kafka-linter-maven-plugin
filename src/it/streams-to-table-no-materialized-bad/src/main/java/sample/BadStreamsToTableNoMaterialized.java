package sample;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_TO_TABLE_NO_MATERIALIZED — must fire EXACTLY 8
 * times across this class (one per method below).
 *
 * <p>Exercises both unsafe overload descriptors:
 *
 * <ol>
 *   <li>{@code ()Lorg/apache/kafka/streams/kstream/KTable;} —
 *       {@code toTable()};</li>
 *   <li>{@code (Lorg/apache/kafka/streams/kstream/Named;)Lorg/
 *       apache/kafka/streams/kstream/KTable;} —
 *       {@code toTable(Named)}.</li>
 * </ol>
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls and
 * {@code INVOKEDYNAMIC} method-reference captures bound to a
 * SAM whose erased implMethod descriptor matches one of the
 * two unsafe overloads.
 */
public final class BadStreamsToTableNoMaterialized {

    /** 0-arg toTable SAM matching {@code ()KTable}. */
    @FunctionalInterface
    interface ToTableNullaryFactory<K, V> {
        KTable<K, V> apply();
    }

    /** 1-arg Named toTable SAM matching {@code (Named)KTable}. */
    @FunctionalInterface
    interface ToTableWithNamedFactory<K, V> {
        KTable<K, V> apply(Named named);
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overloads =====

    /** MUST FIRE — direct INVOKEINTERFACE on the 0-arg overload. */
    public KTable<String, String> eventsTable(KStream<String, String> events) {
        return events.toTable();
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the 1-arg
     *  {@code (Named)} unsafe overload. Named labels the source
     *  node but the backing state store is still auto-named. */
    public KTable<String, String> ordersTable(KStream<String, String> orders) {
        return orders.toTable(Named.as("orders-promotion"));
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the 0-arg overload
     *  in a chain after a filter; the filter is the upstream edit
     *  the team will eventually add. */
    public KTable<String, String> filteredTable(KStream<String, String> stream) {
        return stream.filter((k, v) -> v != null).toTable();
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code events::toTable} bound to
     *  {@link Supplier Supplier&lt;KTable&gt;}. INVOKEDYNAMIC
     *  bsm-args contain a REF_invokeInterface Handle pointing at
     *  {@code KStream.toTable()KTable}. */
    public Supplier<KTable<String, String>>
            buildSupplierFactory(KStream<String, String> events) {
        return events::toTable;
    }

    /** MUST FIRE — {@code events::toTable} bound to
     *  {@link Function Function&lt;Named, KTable&gt;}.
     *  INVOKEDYNAMIC bsm-args contain a REF_invokeInterface Handle
     *  pointing at the 1-arg {@code (Named)} unsafe overload. */
    public Function<Named, KTable<String, String>>
            buildFunctionFactory(KStream<String, String> events) {
        return events::toTable;
    }

    /** MUST FIRE — {@code events::toTable} bound to a custom 0-arg
     *  generic SAM. Indy implMethod handle descriptor is the
     *  0-arg unsafe overload exactly. */
    public ToTableNullaryFactory<String, String>
            buildNullaryFactory(KStream<String, String> events) {
        return events::toTable;
    }

    /** MUST FIRE — local Supplier binding applied immediately.
     *  The indy site is in this method's bytecode. */
    public KTable<String, String> useLocalFactory(KStream<String, String> events) {
        Supplier<KTable<String, String>> factory = events::toTable;
        return factory.get();
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code stream.toTable(Named.as(...))}. The synthetic lambda
     *  method's bytecode contains a direct INVOKEINTERFACE on the
     *  1-arg unsafe overload — the rule walks all methods on the
     *  class including synthetic lambda bodies and fires there. */
    public Stream<KTable<String, String>> tableAll(
            KStream<String, String> stream, List<String> names) {
        return names.stream().map(name -> stream.toTable(Named.as(name)));
    }

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        System.out.println(builder);
        new BadStreamsToTableNoMaterialized();
    }
}
