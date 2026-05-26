package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every StoreBuilder leaves the changelog topic
 * enabled. Some chains call
 * {@link StoreBuilder#withLoggingEnabled(Map)} to tune the
 * changelog topic config without disabling it; others simply
 * pass the freshly-built StoreBuilder through without touching
 * the logging flag.
 */
public final class GoodStreamsStoreBuilderWithLoggingDisabled {

    private static final Map<String, String> CHANGELOG_CONFIG = Map.of(
            "retention.ms", "604800000",
            "cleanup.policy", "compact");

    private static StoreBuilder<KeyValueStore<String, Long>> fresh(String name) {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(name),
                Serdes.String(),
                Serdes.Long());
    }

    public StoreBuilder<KeyValueStore<String, Long>> storeA() {
        return Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("store-a"),
                        Serdes.String(),
                        Serdes.Long())
                .withLoggingEnabled(CHANGELOG_CONFIG);
    }

    public StoreBuilder<KeyValueStore<String, Long>> storeB() {
        return Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("store-b"),
                        Serdes.String(),
                        Serdes.Long())
                .withLoggingEnabled(CHANGELOG_CONFIG)
                .withCachingDisabled();
    }

    public StoreBuilder<KeyValueStore<String, Long>> storeC(
            StoreBuilder<KeyValueStore<String, Long>> b) {
        return b.withLoggingEnabled(CHANGELOG_CONFIG);
    }

    public StoreBuilder<KeyValueStore<String, Long>> storeD() {
        StoreBuilder<KeyValueStore<String, Long>> b = fresh("store-d");
        return b.withLoggingEnabled(CHANGELOG_CONFIG);
    }

    public Supplier<StoreBuilder<KeyValueStore<String, Long>>>
            buildSupplierFactory(StoreBuilder<KeyValueStore<String, Long>> b) {
        return () -> b.withLoggingEnabled(CHANGELOG_CONFIG);
    }

    public Function<Map<String, String>, StoreBuilder<KeyValueStore<String, Long>>>
            buildFunctionFactory(StoreBuilder<KeyValueStore<String, Long>> b) {
        return b::withLoggingEnabled;
    }

    public StoreBuilder<KeyValueStore<String, Long>> useLocalFactory(
            StoreBuilder<KeyValueStore<String, Long>> b) {
        Function<Map<String, String>, StoreBuilder<KeyValueStore<String, Long>>>
                factory = b::withLoggingEnabled;
        return factory.apply(CHANGELOG_CONFIG);
    }

    public Stream<StoreBuilder<KeyValueStore<String, Long>>> hardenAll(
            List<StoreBuilder<KeyValueStore<String, Long>>> builders) {
        return builders.stream().map(b -> b.withLoggingEnabled(CHANGELOG_CONFIG));
    }

    public static void main(String[] args) {
        new GoodStreamsStoreBuilderWithLoggingDisabled();
    }
}
