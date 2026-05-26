package sample;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_COUNT_NO_NAMED — must fire EXACTLY 8 times
 * across this class (one per method below).
 *
 * <p>Exercises the unsafe overload descriptors of {@code count}
 * on each of the four grouped-stream-like owner interfaces
 * (KGroupedStream, KGroupedTable, TimeWindowedKStream,
 * SessionWindowedKStream) — including BOTH the no-arg
 * {@code count()} and the {@code count(Materialized)} half-fix
 * variant. (The {@code count(Named)} and {@code count(Named,
 * Materialized)} overloads are safe and must NOT fire.)
 */
public final class BadStreamsCountNoNamed {

    /** 0-arg SAM matching {@code ()KTable}. */
    @FunctionalInterface
    interface CountFactory<K> {
        KTable<K, Long> apply();
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overloads =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KGroupedStream.count() (no Named, no Materialized). */
    public KTable<String, Long> countA(KGroupedStream<String, String> g) {
        return g.count();
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KGroupedStream.count(Materialized) — the SYMMETRIC
     *  half-fix variant (still no Named). */
    public KTable<String, Long> countB(KGroupedStream<String, String> g) {
        return g.count(
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count-b-store"));
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  TimeWindowedKStream.count() — windowed count, still
     *  unnamed processor node. */
    public KTable<Windowed<String>, Long> countC(TimeWindowedKStream<String, String> w) {
        return w.count();
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  SessionWindowedKStream.count(Materialized) — session-
     *  windowed Materialized-only half-fix. */
    public KTable<Windowed<String>, Long> countD(SessionWindowedKStream<String, String> s) {
        return s.count(
                Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("count-d-store"));
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code grouped::count} bound to {@link
     *  Supplier}{@code <KTable>}. Indy implMethod handle
     *  descriptor matches KGroupedStream.count()KTable —
     *  no-Named overload. */
    public Supplier<KTable<String, Long>> buildSupplierFactory(KGroupedStream<String, String> g) {
        return g::count;
    }

    /** MUST FIRE — {@code grouped::count} bound to {@link
     *  Function}{@code <Materialized, KTable>}. Indy implMethod
     *  handle descriptor matches KGroupedStream.count(
     *  Materialized)KTable — still no Named. */
    public Function<Materialized<String, Long, KeyValueStore<Bytes, byte[]>>, KTable<String, Long>>
            buildMaterializedFactory(KGroupedStream<String, String> g) {
        return g::count;
    }

    /** MUST FIRE — local SAM binding via method reference,
     *  applied inside the same method. */
    public KTable<String, Long> useLocalFactory(KGroupedStream<String, String> g) {
        CountFactory<String> factory = g::count;
        return factory.apply();
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code g.count()}. The synthetic
     *  {@code lambda$countAll$0} carries a direct
     *  INVOKEINTERFACE on KGroupedStream.count(). */
    public Stream<KTable<String, Long>> countAll(List<KGroupedStream<String, String>> groups) {
        return groups.stream().map(g -> g.count());
    }

    public static void main(String[] args) {
        new BadStreamsCountNoNamed();
    }
}
