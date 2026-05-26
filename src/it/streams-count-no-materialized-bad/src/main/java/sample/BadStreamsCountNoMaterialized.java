package sample;

import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_COUNT_NO_MATERIALIZED — must fire EXACTLY 8
 * times across this class (one per method below).
 *
 * <p>Exercises the unsafe overload descriptors of {@code count}
 * on each of the four grouped-stream-like owner interfaces
 * (KGroupedStream, KGroupedTable, TimeWindowedKStream,
 * SessionWindowedKStream) — including BOTH the no-arg
 * {@code count()} and the {@code count(Named)} half-fix
 * variant.
 */
public final class BadStreamsCountNoMaterialized {

    /** 0-arg SAM matching {@code ()KTable}. */
    @FunctionalInterface
    interface CountFactory<K> {
        KTable<K, Long> apply();
    }

    /** 1-arg SAM matching {@code (Named)KTable}. */
    @FunctionalInterface
    interface NamedCountFactory<K> {
        KTable<K, Long> apply(Named named);
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overloads =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KGroupedStream.count() (no Materialized, no Named). */
    public KTable<String, Long> countA(KGroupedStream<String, String> g) {
        return g.count();
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  KGroupedStream.count(Named) — the half-fix variant
     *  (still no Materialized). */
    public KTable<String, Long> countB(KGroupedStream<String, String> g) {
        return g.count(Named.as("count-b"));
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  TimeWindowedKStream.count() — windowed count, still
     *  unnamed state store. */
    public KTable<Windowed<String>, Long> countC(TimeWindowedKStream<String, String> w) {
        return w.count();
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  SessionWindowedKStream.count(Named) — session-windowed
     *  half-fix. */
    public KTable<Windowed<String>, Long> countD(SessionWindowedKStream<String, String> s) {
        return s.count(Named.as("count-d"));
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code grouped::count} bound to {@link
     *  Supplier}{@code <KTable>}. Indy implMethod handle
     *  descriptor matches KGroupedStream.count()KTable. */
    public Supplier<KTable<String, Long>> buildSupplierFactory(KGroupedStream<String, String> g) {
        return g::count;
    }

    /** MUST FIRE — {@code grouped::count} bound to {@link
     *  Function}{@code <Named, KTable>}. Indy implMethod handle
     *  descriptor matches KGroupedStream.count(Named)KTable —
     *  still no Materialized. */
    public Function<Named, KTable<String, Long>> buildNamedFactory(KGroupedStream<String, String> g) {
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
        new BadStreamsCountNoMaterialized();
    }
}
