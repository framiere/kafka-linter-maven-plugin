package sample;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_FOREIGN_KEY_JOIN_NO_TABLE_JOINED — must fire
 * EXACTLY 8 times across this class (one per method below).
 *
 * <p>Exercises the unsafe overload descriptors of {@code join}
 * and {@code leftJoin} on {@link KTable} — the foreign-key
 * variants carrying a {@code Function<V, KO>} extractor whose
 * argument list does NOT include a {@link
 * org.apache.kafka.streams.kstream.TableJoined TableJoined}.
 * Each unsafe descriptor flavour is exercised: 3-arg (no extra),
 * Named-only half-fix, Materialized-only half-fix, and
 * Named+Materialized "both half-fixes" still without
 * TableJoined.
 */
public final class BadStreamsForeignKeyJoinNoTableJoined {

    private static final Function<String, String> FK = String::toUpperCase;
    private static final ValueJoiner<String, String, String> JOINER = (l, r) -> l + "/" + r;

    /** 3-arg SAM matching {@code (KTable, Function, ValueJoiner
     *  )KTable} — bound-receiver method ref captures
     *  KTable.join(KTable, Function, ValueJoiner). */
    @FunctionalInterface
    interface FkFactory<V, VO, KO, VR> {
        KTable<String, VR> apply(
                KTable<String, VO> right,
                Function<V, KO> fk,
                ValueJoiner<V, VO, VR> joiner);
    }

    /** 4-arg SAM matching {@code (KTable, Function, ValueJoiner,
     *  Materialized)KTable} — the Materialized-only half-fix
     *  via bound-receiver method ref. */
    @FunctionalInterface
    interface FkMatFactory<V, VO, KO, VR> {
        KTable<String, VR> apply(
                KTable<String, VO> right,
                Function<V, KO> fk,
                ValueJoiner<V, VO, VR> joiner,
                Materialized<String, VR, KeyValueStore<Bytes, byte[]>> mat);
    }

    // ===== Direct INVOKEINTERFACE on the unsafe FK overloads =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KTable.join(KTable, Function, ValueJoiner) (3-arg FK,
     *  no TableJoined, no Named, no Materialized). */
    public KTable<String, String> joinA(
            KTable<String, String> left, KTable<String, String> right) {
        return left.join(right, FK, JOINER);
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KTable.join(KTable, Function, ValueJoiner, Named) — the
     *  Named-only HALF-fix (pins the processor-node id but NOT
     *  the subscription topic names). */
    public KTable<String, String> joinB(
            KTable<String, String> left, KTable<String, String> right) {
        return left.join(right, FK, JOINER, Named.as("join-b"));
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KTable.leftJoin(KTable, Function, ValueJoiner,
     *  Materialized) — the Materialized-only half-fix
     *  (subscription topics still graph-index-derived). */
    public KTable<String, String> joinC(
            KTable<String, String> left, KTable<String, String> right) {
        return left.leftJoin(
                right, FK, JOINER,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("join-c-store"));
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KTable.leftJoin(KTable, Function, ValueJoiner, Named,
     *  Materialized) — BOTH half-fixes together but STILL no
     *  TableJoined, so the subscription topics remain
     *  graph-index-derived. */
    public KTable<String, String> joinD(
            KTable<String, String> left, KTable<String, String> right) {
        return left.leftJoin(
                right, FK, JOINER,
                Named.as("join-d"),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("join-d-store"));
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code left::join} as a bound-receiver
     *  method reference. Indy implMethod handle descriptor
     *  matches KTable.join(KTable, Function, ValueJoiner)KTable
     *  — no-TableJoined overload. */
    public FkFactory<String, String, String, String> buildSupplierFactory(
            KTable<String, String> left) {
        return left::join;
    }

    /** MUST FIRE — {@code left::leftJoin} bound to a 4-arg SAM
     *  whose erased descriptor matches KTable.leftJoin(KTable,
     *  Function, ValueJoiner, Materialized)KTable — still no
     *  TableJoined. */
    public FkMatFactory<String, String, String, String> buildMaterializedFactory(
            KTable<String, String> left) {
        return left::leftJoin;
    }

    /** MUST FIRE — local SAM binding via bound-receiver method
     *  reference, applied inside the same method. */
    public KTable<String, String> useLocalFactory(
            KTable<String, String> left, KTable<String, String> right) {
        FkFactory<String, String, String, String> factory = left::join;
        return factory.apply(right, FK, JOINER);
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code left.join(right, FK, JOINER)} per element. The
     *  synthetic {@code lambda$joinAll$0} carries a direct
     *  INVOKEINTERFACE on KTable.join(KTable, Function,
     *  ValueJoiner). */
    public Stream<KTable<String, String>> joinAll(
            final KTable<String, String> left,
            List<KTable<String, String>> rights) {
        return rights.stream().map(right -> left.join(right, FK, JOINER));
    }

    public static void main(String[] args) {
        new BadStreamsForeignKeyJoinNoTableJoined();
    }
}
