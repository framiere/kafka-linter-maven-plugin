package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_STOREBUILDER_WITH_LOGGING_DISABLED — must fire
 * EXACTLY 8 times across this class (one per method below).
 *
 * <p>Exercises {@code StoreBuilder.withLoggingDisabled()} in
 * every shape that compiled Java emits: direct INVOKEINTERFACE
 * on the StoreBuilder interface, INVOKEDYNAMIC bound to a
 * captured {@code builder::withLoggingDisabled} as Supplier /
 * UnaryOperator, unbound
 * {@code StoreBuilder::withLoggingDisabled} as Function, and a
 * synthetic lambda body that invokes the method.
 */
public final class BadStreamsStoreBuilderWithLoggingDisabled {

    /** 0-arg SAM matching {@code ()StoreBuilder}. */
    @FunctionalInterface
    interface BuilderHardener<T extends StoreBuilder<?>> {
        T apply();
    }

    private static StoreBuilder<KeyValueStore<String, Long>> fresh(String name) {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(name),
                Serdes.String(),
                Serdes.Long());
    }

    // ===== Direct INVOKEINTERFACE on the StoreBuilder interface =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  StoreBuilder.withLoggingDisabled() at the tail of a
     *  fluent builder chain. */
    public StoreBuilder<KeyValueStore<String, Long>> storeA() {
        return Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("store-a"),
                        Serdes.String(),
                        Serdes.Long())
                .withLoggingDisabled();
    }

    /** MUST FIRE — direct INVOKEINTERFACE in the middle of a
     *  fluent builder chain (followed by a further setter). */
    public StoreBuilder<KeyValueStore<String, Long>> storeB() {
        return Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("store-b"),
                        Serdes.String(),
                        Serdes.Long())
                .withLoggingDisabled()
                .withCachingDisabled();
    }

    /** MUST FIRE — direct INVOKEINTERFACE on a StoreBuilder
     *  passed in as a parameter. */
    public StoreBuilder<KeyValueStore<String, Long>> storeC(
            StoreBuilder<KeyValueStore<String, Long>> b) {
        return b.withLoggingDisabled();
    }

    /** MUST FIRE — direct INVOKEINTERFACE on a StoreBuilder
     *  held by a local variable. */
    public StoreBuilder<KeyValueStore<String, Long>> storeD() {
        StoreBuilder<KeyValueStore<String, Long>> b = fresh("store-d");
        return b.withLoggingDisabled();
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — bound {@code b::withLoggingDisabled} bound
     *  to {@link Supplier}{@code <StoreBuilder>}. Indy
     *  implMethod handle descriptor matches
     *  StoreBuilder.withLoggingDisabled()StoreBuilder. */
    public Supplier<StoreBuilder<KeyValueStore<String, Long>>>
            buildSupplierFactory(StoreBuilder<KeyValueStore<String, Long>> b) {
        return b::withLoggingDisabled;
    }

    /** MUST FIRE — unbound
     *  {@code StoreBuilder::withLoggingDisabled} bound to
     *  {@link Function}{@code <StoreBuilder, StoreBuilder>}.
     *  The first parameter is the receiver at invocation time. */
    @SuppressWarnings("rawtypes")
    public Function<StoreBuilder, StoreBuilder> buildFunctionFactory() {
        return StoreBuilder::withLoggingDisabled;
    }

    /** MUST FIRE — local SAM binding via method reference,
     *  applied inside the same method. */
    public StoreBuilder<KeyValueStore<String, Long>> useLocalFactory(
            StoreBuilder<KeyValueStore<String, Long>> b) {
        BuilderHardener<StoreBuilder<KeyValueStore<String, Long>>> factory =
                b::withLoggingDisabled;
        return factory.apply();
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code b.withLoggingDisabled()}. The synthetic
     *  {@code lambda$disableAll$0} carries a direct
     *  INVOKEINTERFACE on StoreBuilder.withLoggingDisabled. */
    public Stream<StoreBuilder<KeyValueStore<String, Long>>> disableAll(
            List<StoreBuilder<KeyValueStore<String, Long>>> builders) {
        return builders.stream().map(b -> b.withLoggingDisabled());
    }

    public static void main(String[] args) {
        new BadStreamsStoreBuilderWithLoggingDisabled();
    }
}
