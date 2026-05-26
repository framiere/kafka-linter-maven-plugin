package sample;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every foreign-key join passes a TableJoined,
 * so the descriptor contains org/apache/kafka/streams/kstream/
 * TableJoined and the predicate rejects it.
 */
public final class GoodStreamsForeignKeyJoinNoTableJoined {

    private static final Function<String, String> FK = String::toUpperCase;
    private static final ValueJoiner<String, String, String> JOINER = (l, r) -> l + "/" + r;

    /** 3-arg SAM whose erased descriptor matches
     *  KTable.join(KTable, Function, ValueJoiner,
     *  TableJoined)KTable — TableJoined-bearing overload. */
    @FunctionalInterface
    interface FkTjFactory<V, VO, KO, VR> {
        KTable<String, VR> apply(
                Function<V, KO> fk,
                ValueJoiner<V, VO, VR> joiner,
                TableJoined<String, KO> tj);
    }

    /** 4-arg SAM whose erased descriptor matches
     *  KTable.leftJoin(KTable, Function, ValueJoiner,
     *  TableJoined, Materialized)KTable — full safe overload. */
    @FunctionalInterface
    interface FkTjMatFactory<V, VO, KO, VR> {
        KTable<String, VR> apply(
                Function<V, KO> fk,
                ValueJoiner<V, VO, VR> joiner,
                TableJoined<String, KO> tj,
                Materialized<String, VR, KeyValueStore<Bytes, byte[]>> mat);
    }

    public KTable<String, String> joinA(
            KTable<String, String> left, KTable<String, String> right) {
        return left.join(
                right, FK, JOINER,
                TableJoined.<String, String>as("join-a"));
    }

    public KTable<String, String> joinB(
            KTable<String, String> left, KTable<String, String> right) {
        return left.join(
                right, FK, JOINER,
                TableJoined.<String, String>as("join-b"),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("join-b-store"));
    }

    public KTable<String, String> joinC(
            KTable<String, String> left, KTable<String, String> right) {
        return left.leftJoin(
                right, FK, JOINER,
                TableJoined.<String, String>as("join-c"));
    }

    public KTable<String, String> joinD(
            KTable<String, String> left, KTable<String, String> right) {
        return left.leftJoin(
                right, FK, JOINER,
                TableJoined.<String, String>as("join-d"),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("join-d-store"));
    }

    public FkTjFactory<String, String, String, String> buildTableJoinedFactory(
            final KTable<String, String> left, final KTable<String, String> right) {
        return (fk, joiner, tj) -> left.join(right, fk, joiner, tj);
    }

    public KTable<String, String> useLocalFactory(
            KTable<String, String> left, KTable<String, String> right) {
        FkTjMatFactory<String, String, String, String> factory =
                (fk, joiner, tj, mat) -> left.leftJoin(right, fk, joiner, tj, mat);
        return factory.apply(
                FK, JOINER,
                TableJoined.<String, String>as("join-local"),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("join-local-store"));
    }

    public Stream<KTable<String, String>> joinAll(
            final KTable<String, String> left,
            List<KTable<String, String>> rights) {
        return rights.stream().map(right ->
                left.join(right, FK, JOINER,
                        TableJoined.<String, String>as("join-each")));
    }

    public static void main(String[] args) {
        new GoodStreamsForeignKeyJoinNoTableJoined();
    }
}
