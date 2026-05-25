package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_TABLE_NO_MATERIALIZED — must fire EXACTLY 8 times
 * across this class (one per method below).
 *
 * <p>Each method exercises one of the bytecode shapes the rule is
 * required to catch — two unsafe overload descriptors:
 *
 * <ol>
 *   <li>{@code (Ljava/lang/String;)Lorg/apache/kafka/streams/
 *       kstream/KTable;} — {@code table(topic)};</li>
 *   <li>{@code (Ljava/lang/String;Lorg/apache/kafka/streams/
 *       kstream/Consumed;)Lorg/apache/kafka/streams/kstream/
 *       KTable;} — {@code table(topic, Consumed)}.</li>
 * </ol>
 *
 * <p>Catches both direct {@code INVOKEVIRTUAL} calls and {@code
 * INVOKEDYNAMIC} method-reference captures bound to a SAM whose
 * erased implMethod descriptor matches one of the two unsafe
 * overloads.
 */
public final class BadStreamsTableNoMaterialized {

    @FunctionalInterface
    interface TableFactory<K, V> {
        KTable<K, V> create(String topic);
    }

    @FunctionalInterface
    interface UsersTableFactory {
        KTable<String, String> create(String topic);
    }

    // ===== Direct INVOKEVIRTUAL on the two unsafe overloads =====

    /**
     * MUST FIRE — direct INVOKEVIRTUAL on the 1-arg overload.
     * Backing state store and changelog topic are auto-named from
     * the topology graph index.
     */
    public KTable<String, String> usersTable(StreamsBuilder builder) {
        return builder.table("users");
    }

    /**
     * MUST FIRE — direct INVOKEVIRTUAL on the 2-arg
     * {@code (String, Consumed)} unsafe overload. Consumed names
     * the source-node serdes; it does NOT materialize the
     * downstream store — that's Materialized's job.
     */
    public KTable<String, String> customersTable(StreamsBuilder builder) {
        return builder.table("customers",
                Consumed.with(Serdes.String(), Serdes.String()));
    }

    /**
     * MUST FIRE — direct INVOKEVIRTUAL on the 1-arg overload in a
     * void topology-builder method. Inventory KTable backs a
     * downstream join; auto-named store invalidates on any
     * upstream topology shift.
     */
    public void buildTopology(StreamsBuilder builder) {
        KTable<String, String> inventory = builder.table("inventory");
        inventory.toStream().to("inventory-out");
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /**
     * MUST FIRE — {@code builder::table} bound to
     * {@link Function Function&lt;String, KTable&lt;String,
     * String&gt;&gt;}. INVOKEDYNAMIC bsm-args contain a
     * REF_invokeVirtual Handle pointing at
     * {@code StreamsBuilder.table(Ljava/lang/String;)Lorg/apache/
     * kafka/streams/kstream/KTable;}.
     */
    public Function<String, KTable<String, String>> buildFunctionFactory(StreamsBuilder builder) {
        return builder::table;
    }

    /**
     * MUST FIRE — {@code builder::table} bound to
     * {@link BiFunction BiFunction&lt;String, Consumed, KTable&lt;
     * String, String&gt;&gt;}. INVOKEDYNAMIC bsm-args contain a
     * REF_invokeVirtual Handle pointing at the 2-arg
     * {@code (String, Consumed)} unsafe overload.
     */
    public BiFunction<String, Consumed<String, String>, KTable<String, String>>
            buildConsumedFactory(StreamsBuilder builder) {
        return builder::table;
    }

    /**
     * MUST FIRE — {@code builder::table} bound to a custom
     * non-generic 1-arg SAM. Indy implMethod handle descriptor is
     * exactly {@code (Ljava/lang/String;)Lorg/apache/kafka/streams/
     * kstream/KTable;}.
     */
    public UsersTableFactory buildUsersTableFactory(StreamsBuilder builder) {
        return builder::table;
    }

    /**
     * MUST FIRE — local Function binding via method reference,
     * applied to a topic name inside the same method. The indy
     * site is in this method's bytecode; the synthetic lambda
     * body that calls {@code factory.apply(topic)} does NOT
     * contain a direct INVOKEVIRTUAL on table (it goes through
     * the Function handle).
     */
    public KTable<String, String> useLocalFactory(StreamsBuilder builder, String topic) {
        Function<String, KTable<String, String>> factory = builder::table;
        return factory.apply(topic);
    }

    /**
     * MUST FIRE — explicit lambda body that invokes the 1-arg
     * {@code builder.table(name)} overload. The synthetic lambda
     * method's bytecode contains a direct INVOKEVIRTUAL on the
     * 1-arg unsafe overload — the rule walks all methods on the
     * class including synthetic lambda bodies and fires there.
     */
    public Stream<KTable<String, String>> tableAll(StreamsBuilder builder, List<String> topics) {
        return topics.stream().map(name -> builder.table(name));
    }

    public static void main(String[] args) {
        StreamsBuilder b = new StreamsBuilder();
        new BadStreamsTableNoMaterialized().buildTopology(b);
    }
}
