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
 * RULE: STREAMS_IN_MEMORY_KV_STORE — must fire EXACTLY 8
 * times across this class (one per method below).
 *
 * <p>Exercises the three in-memory factory methods on
 * {@link Stores}: {@code inMemoryKeyValueStore},
 * {@code inMemoryWindowStore}, {@code inMemorySessionStore}.
 * The {@code persistent*} counterparts are the safe
 * production-default form and are intentionally not
 * exercised here.
 *
 * <p>Each method exercises one capture shape — direct
 * {@code INVOKESTATIC} or {@code INVOKEDYNAMIC} method-
 * reference capture. The rule fires on the method
 * containing the unsafe instruction; for explicit lambdas
 * the fire lands on the synthetic {@code lambda$N$0}
 * method.
 */
public final class BadStreamsInMemoryKvStore {

    /** 4-arg SAM whose erased descriptor matches
     *  {@code Stores.inMemoryWindowStore(String, Duration,
     *  Duration, boolean)WindowBytesStoreSupplier}. */
    @FunctionalInterface
    interface WindowFactory {
        WindowBytesStoreSupplier make(String name, Duration retention,
                                      Duration windowSize, boolean retainDup);
    }

    // ===== Direct INVOKESTATIC on the in-memory factories =====

    /** MUST FIRE — direct INVOKESTATIC on
     *  Stores.inMemoryKeyValueStore(String). */
    public KeyValueBytesStoreSupplier createA() {
        return Stores.inMemoryKeyValueStore("store-a");
    }

    /** MUST FIRE — direct INVOKESTATIC on
     *  Stores.inMemoryWindowStore(String, Duration,
     *  Duration, boolean). */
    public WindowBytesStoreSupplier createB() {
        return Stores.inMemoryWindowStore(
                "store-b",
                Duration.ofMinutes(10),
                Duration.ofMinutes(1),
                false);
    }

    /** MUST FIRE — direct INVOKESTATIC on
     *  Stores.inMemorySessionStore(String, Duration). */
    public SessionBytesStoreSupplier createC() {
        return Stores.inMemorySessionStore("store-c", Duration.ofMinutes(5));
    }

    /** MUST FIRE — direct INVOKESTATIC on
     *  Stores.inMemoryKeyValueStore inside a static helper. */
    public static KeyValueBytesStoreSupplier createD() {
        return Stores.inMemoryKeyValueStore("store-d");
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code Stores::inMemoryKeyValueStore} as
     *  a static method reference. Indy implMethod handle
     *  descriptor matches Stores.inMemoryKeyValueStore(
     *  String)KeyValueBytesStoreSupplier. */
    public Function<String, KeyValueBytesStoreSupplier> kvFactory() {
        return Stores::inMemoryKeyValueStore;
    }

    /** MUST FIRE — {@code Stores::inMemoryWindowStore} as a
     *  static method reference bound to a 4-arg SAM whose
     *  erased descriptor matches the window factory. */
    public WindowFactory windowFactory() {
        return Stores::inMemoryWindowStore;
    }

    /** MUST FIRE — local SAM binding via static method
     *  reference, applied inside the same method. The
     *  factory.apply() call is just an invokeinterface on
     *  Function.apply() which is not flagged. */
    public KeyValueBytesStoreSupplier useLocalFactory() {
        Function<String, KeyValueBytesStoreSupplier> factory = Stores::inMemoryKeyValueStore;
        return factory.apply("store-local");
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code Stores.inMemoryKeyValueStore(name)} per
     *  element. The synthetic {@code lambda$createAll$0}
     *  carries a direct INVOKESTATIC on
     *  Stores.inMemoryKeyValueStore. */
    public Stream<KeyValueBytesStoreSupplier> createAll(List<String> names) {
        return names.stream().map(name -> Stores.inMemoryKeyValueStore(name));
    }

    public static void main(String[] args) {
        new BadStreamsInMemoryKvStore();
    }
}
