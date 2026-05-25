package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_GLOBAL_TABLE_NO_MATERIALIZED — must fire EXACTLY 8
 * times across this class (one per method below).
 *
 * <p>A {@link GlobalKTable} is loaded from a Kafka topic onto EVERY
 * Streams instance in the cluster, in full, before processing
 * begins. Unlike a regular KTable (partitioned, local-partitions
 * only), a GlobalKTable consumes the entire topic on every instance.
 * Without an explicit {@code Materialized} argument naming the
 * global state store, the store is auto-named from the topology
 * graph index; any upstream topology edit shifts the sequence
 * number and renames the store; on deploy, every Streams instance
 * throws away the existing global store and restores from scratch
 * (cluster-wide cost), and stream-globalTable joins synchronously
 * return null during the restore window.
 *
 * <p>Each method exercises one of the bytecode shapes the rule is
 * required to catch — two unsafe overload descriptors:
 *
 * <ol>
 *   <li>{@code (Ljava/lang/String;)Lorg/apache/kafka/streams/
 *       kstream/GlobalKTable;} — {@code globalTable(topic)};</li>
 *   <li>{@code (Ljava/lang/String;Lorg/apache/kafka/streams/
 *       kstream/Consumed;)Lorg/apache/kafka/streams/kstream/
 *       GlobalKTable;} — {@code globalTable(topic, Consumed)}.</li>
 * </ol>
 *
 * <p>Catches both direct {@code INVOKEVIRTUAL} calls and {@code
 * INVOKEDYNAMIC} method-reference captures bound to a SAM whose
 * erased implMethod descriptor matches one of the two unsafe
 * overloads.
 */
public final class BadStreamsGlobalTableNoMaterialized {

    @FunctionalInterface
    interface GlobalTableFactory<K, V> {
        GlobalKTable<K, V> create(String topic);
    }

    @FunctionalInterface
    interface ProductsGlobalTableFactory {
        GlobalKTable<String, String> create(String topic);
    }

    // ===== Direct INVOKEVIRTUAL on the two unsafe overloads =====

    /**
     * MUST FIRE — direct INVOKEVIRTUAL on the 1-arg overload.
     * Global state store and cluster-wide cold-restart cost both
     * derive from the topology graph index.
     */
    public GlobalKTable<String, String> productsGlobalTable(StreamsBuilder builder) {
        return builder.globalTable("products");
    }

    /**
     * MUST FIRE — direct INVOKEVIRTUAL on the 2-arg
     * {@code (String, Consumed)} unsafe overload. Consumed names
     * the source-node serdes; it does NOT materialize the
     * downstream global store — that's Materialized's job.
     */
    public GlobalKTable<String, String> customersGlobalTable(StreamsBuilder builder) {
        return builder.globalTable("customers",
                Consumed.with(Serdes.String(), Serdes.String()));
    }

    /**
     * MUST FIRE — direct INVOKEVIRTUAL on the 1-arg overload in a
     * void topology-builder method. Reference data GlobalKTable
     * backs a downstream enrichment join; auto-named global store
     * invalidates on any upstream topology shift, forcing a
     * cluster-wide cold-restart.
     */
    public void buildTopology(StreamsBuilder builder) {
        GlobalKTable<String, String> reference = builder.globalTable("reference");
        // touch the value so javac keeps the local
        System.out.println(reference);
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /**
     * MUST FIRE — {@code builder::globalTable} bound to
     * {@link Function Function&lt;String, GlobalKTable&lt;String,
     * String&gt;&gt;}. INVOKEDYNAMIC bsm-args contain a
     * REF_invokeVirtual Handle pointing at
     * {@code StreamsBuilder.globalTable(Ljava/lang/String;)Lorg/
     * apache/kafka/streams/kstream/GlobalKTable;}.
     */
    public Function<String, GlobalKTable<String, String>> buildFunctionFactory(StreamsBuilder builder) {
        return builder::globalTable;
    }

    /**
     * MUST FIRE — {@code builder::globalTable} bound to
     * {@link BiFunction BiFunction&lt;String, Consumed, GlobalKTable
     * &lt;String, String&gt;&gt;}. INVOKEDYNAMIC bsm-args contain
     * a REF_invokeVirtual Handle pointing at the 2-arg
     * {@code (String, Consumed)} unsafe overload.
     */
    public BiFunction<String, Consumed<String, String>, GlobalKTable<String, String>>
            buildConsumedFactory(StreamsBuilder builder) {
        return builder::globalTable;
    }

    /**
     * MUST FIRE — {@code builder::globalTable} bound to a custom
     * non-generic 1-arg SAM. Indy implMethod handle descriptor is
     * exactly {@code (Ljava/lang/String;)Lorg/apache/kafka/streams/
     * kstream/GlobalKTable;}.
     */
    public ProductsGlobalTableFactory buildProductsGlobalTableFactory(StreamsBuilder builder) {
        return builder::globalTable;
    }

    /**
     * MUST FIRE — local Function binding via method reference,
     * applied to a topic name inside the same method. The indy
     * site is in this method's bytecode; the synthetic lambda
     * body that calls {@code factory.apply(topic)} does NOT
     * contain a direct INVOKEVIRTUAL on globalTable (it goes
     * through the Function handle).
     */
    public GlobalKTable<String, String> useLocalFactory(StreamsBuilder builder, String topic) {
        Function<String, GlobalKTable<String, String>> factory = builder::globalTable;
        return factory.apply(topic);
    }

    /**
     * MUST FIRE — explicit lambda body that invokes the 1-arg
     * {@code builder.globalTable(name)} overload. The synthetic
     * lambda method's bytecode contains a direct INVOKEVIRTUAL on
     * the 1-arg unsafe overload — the rule walks all methods on
     * the class including synthetic lambda bodies and fires there.
     */
    public Stream<GlobalKTable<String, String>> globalTableAll(StreamsBuilder builder, List<String> topics) {
        return topics.stream().map(name -> builder.globalTable(name));
    }

    public static void main(String[] args) {
        StreamsBuilder b = new StreamsBuilder();
        new BadStreamsGlobalTableNoMaterialized().buildTopology(b);
    }
}
