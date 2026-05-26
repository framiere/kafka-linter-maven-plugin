package sample;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every fluent chain leaves the changelog
 * topic enabled. Some chains call
 * {@link Materialized#withLoggingEnabled(Map)} to tune the
 * changelog topic config without disabling it; others simply
 * pass {@code Materialized.as(...)} through without touching
 * the logging flag.
 */
public final class GoodStreamsMaterializedWithLoggingDisabled {

    private static final Map<String, String> CHANGELOG_CONFIG = Map.of(
            "retention.ms", "604800000",
            "cleanup.policy", "compact");

    public Materialized<String, Long, KeyValueStore<Bytes, byte[]>> matA() {
        return Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("store-a")
                .withLoggingEnabled(CHANGELOG_CONFIG);
    }

    public Materialized<String, Long, KeyValueStore<Bytes, byte[]>> matB() {
        return Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("store-b")
                .withLoggingEnabled(CHANGELOG_CONFIG)
                .withCachingDisabled();
    }

    public Materialized<String, Long, KeyValueStore<Bytes, byte[]>> matC(
            Materialized<String, Long, KeyValueStore<Bytes, byte[]>> m) {
        return m.withLoggingEnabled(CHANGELOG_CONFIG);
    }

    public Materialized<String, Long, KeyValueStore<Bytes, byte[]>> matD() {
        Materialized<String, Long, KeyValueStore<Bytes, byte[]>> m =
                Materialized.as("store-d");
        return m.withLoggingEnabled(CHANGELOG_CONFIG);
    }

    public Supplier<Materialized<String, Long, KeyValueStore<Bytes, byte[]>>>
            buildSupplierFactory(Materialized<String, Long, KeyValueStore<Bytes, byte[]>> m) {
        return () -> m.withLoggingEnabled(CHANGELOG_CONFIG);
    }

    public Function<Map<String, String>, Materialized<String, Long, KeyValueStore<Bytes, byte[]>>>
            buildFunctionFactory(Materialized<String, Long, KeyValueStore<Bytes, byte[]>> m) {
        return m::withLoggingEnabled;
    }

    public Materialized<String, Long, KeyValueStore<Bytes, byte[]>> useLocalFactory(
            Materialized<String, Long, KeyValueStore<Bytes, byte[]>> m) {
        Function<Map<String, String>, Materialized<String, Long, KeyValueStore<Bytes, byte[]>>>
                factory = m::withLoggingEnabled;
        return factory.apply(CHANGELOG_CONFIG);
    }

    public Stream<Materialized<String, Long, KeyValueStore<Bytes, byte[]>>> hardenAll(
            List<Materialized<String, Long, KeyValueStore<Bytes, byte[]>>> mats) {
        return mats.stream().map(m -> m.withLoggingEnabled(CHANGELOG_CONFIG));
    }

    public static void main(String[] args) {
        new GoodStreamsMaterializedWithLoggingDisabled();
    }
}
