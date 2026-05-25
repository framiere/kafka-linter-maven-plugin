package sample;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Named;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site passes an explicit
 * {@link Named} pinning the KTable→KStream transition node
 * name. For the KeyValueMapper overload, this also pins the
 * downstream auto-repartition topic name (it is derived from
 * the rekey node name, not the graph index).
 */
public final class GoodStreamsKTableToStreamNoNamed {

    /** 1-arg SAM matching {@code (Named)KStream}. */
    @FunctionalInterface
    interface NamedToStreamFactory<K, V> {
        KStream<K, V> apply(Named named);
    }

    /** 2-arg SAM matching {@code (KeyValueMapper, Named)KStream}. */
    @FunctionalInterface
    interface RekeyWithNamedFactory<K, V, KR> {
        KStream<KR, V> apply(KeyValueMapper<? super K, ? super V, ? extends KR> mapper,
                             Named named);
    }

    public KStream<String, String> ordersToStream(KTable<String, String> orders) {
        return orders.toStream(Named.as("orders-to-stream"));
    }

    public KStream<String, String> usersToStream(KTable<String, String> users) {
        return users.toStream(Named.as("users-to-stream"));
    }

    public KStream<String, String> ordersByCustomer(KTable<String, String> orders) {
        return orders.toStream((k, v) -> v == null ? "anon" : v.substring(0, 1),
                Named.as("orders-by-customer-rekey"));
    }

    public Function<Named, KStream<String, String>>
            buildSupplierFactory(KTable<String, String> table) {
        return table::toStream;
    }

    public BiFunction<KeyValueMapper<String, String, String>, Named, KStream<String, String>>
            buildRekeyFactory(KTable<String, String> table) {
        return table::toStream;
    }

    public <K, V> NamedToStreamFactory<K, V> buildCustomFactory(KTable<K, V> table) {
        return table::toStream;
    }

    public KStream<String, String> useLocalFactory(KTable<String, String> orders) {
        Function<Named, KStream<String, String>> factory = orders::toStream;
        return factory.apply(Named.as("local-factory-to-stream"));
    }

    public Stream<KStream<String, String>> streamAll(
            List<KTable<String, String>> tables) {
        return tables.stream().map(t -> t.toStream(Named.as("stream-all")));
    }

    public static void main(String[] args) {
        new GoodStreamsKTableToStreamNoNamed();
    }
}
