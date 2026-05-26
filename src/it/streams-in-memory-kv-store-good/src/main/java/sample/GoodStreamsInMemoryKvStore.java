package sample;

import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every factory call uses a {@code
 * persistent*} variant whose method name does not match
 * any of {@code inMemoryKeyValueStore},
 * {@code inMemoryWindowStore}, or
 * {@code inMemorySessionStore}.
 */
public final class GoodStreamsInMemoryKvStore {

    @FunctionalInterface
    interface WindowFactory {
        WindowBytesStoreSupplier make(String name, Duration retention,
                                      Duration windowSize, boolean retainDup);
    }

    public KeyValueBytesStoreSupplier createA() {
        return Stores.persistentKeyValueStore("store-a");
    }

    public WindowBytesStoreSupplier createB() {
        return Stores.persistentWindowStore(
                "store-b",
                Duration.ofMinutes(10),
                Duration.ofMinutes(1),
                false);
    }

    public SessionBytesStoreSupplier createC() {
        return Stores.persistentSessionStore("store-c", Duration.ofMinutes(5));
    }

    public static KeyValueBytesStoreSupplier createD() {
        return Stores.persistentKeyValueStore("store-d");
    }

    public Function<String, KeyValueBytesStoreSupplier> kvFactory() {
        return Stores::persistentKeyValueStore;
    }

    public WindowFactory windowFactory() {
        return Stores::persistentWindowStore;
    }

    public KeyValueBytesStoreSupplier useLocalFactory() {
        Function<String, KeyValueBytesStoreSupplier> factory = Stores::persistentKeyValueStore;
        return factory.apply("store-local");
    }

    public Stream<KeyValueBytesStoreSupplier> createAll(List<String> names) {
        return names.stream().map(name -> Stores.persistentKeyValueStore(name));
    }

    public static void main(String[] args) {
        new GoodStreamsInMemoryKvStore();
    }
}
