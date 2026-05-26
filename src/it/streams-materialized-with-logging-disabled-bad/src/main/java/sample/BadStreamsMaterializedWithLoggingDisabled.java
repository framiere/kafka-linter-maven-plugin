package sample;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_MATERIALIZED_WITH_LOGGING_DISABLED — must fire
 * EXACTLY 8 times across this class (one per method below).
 *
 * <p>Exercises {@code Materialized.withLoggingDisabled()} in
 * every shape that compiled Java emits: direct INVOKEVIRTUAL on
 * the fluent builder, INVOKEDYNAMIC bound to a captured
 * {@code mat::withLoggingDisabled} as Supplier / UnaryOperator,
 * unbound {@code Materialized::withLoggingDisabled} as Function,
 * and a synthetic lambda body that invokes the method.
 */
public final class BadStreamsMaterializedWithLoggingDisabled {

    /** 0-arg SAM matching {@code ()Materialized}. */
    @FunctionalInterface
    interface MatHardener<K, V> {
        Materialized<K, V, KeyValueStore<Bytes, byte[]>> apply();
    }

    // ===== Direct INVOKEVIRTUAL on the fluent builder =====

    /** MUST FIRE — direct INVOKEVIRTUAL on
     *  Materialized.withLoggingDisabled() at the tail of a
     *  fluent builder chain. */
    public Materialized<String, Long, KeyValueStore<Bytes, byte[]>> matA() {
        return Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("store-a")
                .withLoggingDisabled();
    }

    /** MUST FIRE — direct INVOKEVIRTUAL in the middle of a
     *  fluent builder chain (followed by a further setter). */
    public Materialized<String, Long, KeyValueStore<Bytes, byte[]>> matB() {
        return Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("store-b")
                .withLoggingDisabled()
                .withCachingDisabled();
    }

    /** MUST FIRE — direct INVOKEVIRTUAL on a Materialized
     *  passed in as a parameter. */
    public Materialized<String, Long, KeyValueStore<Bytes, byte[]>> matC(
            Materialized<String, Long, KeyValueStore<Bytes, byte[]>> m) {
        return m.withLoggingDisabled();
    }

    /** MUST FIRE — direct INVOKEVIRTUAL on a Materialized held
     *  by a local variable. */
    public Materialized<String, Long, KeyValueStore<Bytes, byte[]>> matD() {
        Materialized<String, Long, KeyValueStore<Bytes, byte[]>> m =
                Materialized.as("store-d");
        return m.withLoggingDisabled();
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — bound {@code m::withLoggingDisabled} bound to
     *  {@link Supplier}{@code <Materialized>}. Indy implMethod
     *  handle descriptor matches Materialized.withLoggingDisabled
     *  ()Materialized. */
    public Supplier<Materialized<String, Long, KeyValueStore<Bytes, byte[]>>>
            buildSupplierFactory(Materialized<String, Long, KeyValueStore<Bytes, byte[]>> m) {
        return m::withLoggingDisabled;
    }

    /** MUST FIRE — unbound {@code Materialized::withLoggingDisabled}
     *  bound to {@link Function}{@code <Materialized,
     *  Materialized>}. The first parameter is the receiver at
     *  invocation time. */
    @SuppressWarnings("rawtypes")
    public Function<Materialized, Materialized> buildFunctionFactory() {
        return Materialized::withLoggingDisabled;
    }

    /** MUST FIRE — local SAM binding via method reference,
     *  applied inside the same method. */
    public Materialized<String, Long, KeyValueStore<Bytes, byte[]>> useLocalFactory(
            Materialized<String, Long, KeyValueStore<Bytes, byte[]>> m) {
        MatHardener<String, Long> factory = m::withLoggingDisabled;
        return factory.apply();
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code m.withLoggingDisabled()}. The synthetic
     *  {@code lambda$disableAll$0} carries a direct
     *  INVOKEVIRTUAL on Materialized.withLoggingDisabled. */
    public Stream<Materialized<String, Long, KeyValueStore<Bytes, byte[]>>> disableAll(
            List<Materialized<String, Long, KeyValueStore<Bytes, byte[]>>> mats) {
        return mats.stream().map(m -> m.withLoggingDisabled());
    }

    public static void main(String[] args) {
        new BadStreamsMaterializedWithLoggingDisabled();
    }
}
