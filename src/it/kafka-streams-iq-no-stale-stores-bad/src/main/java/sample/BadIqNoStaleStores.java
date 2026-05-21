package sample;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/**
 * RULE: STREAMS_STORE_QUERY_PARAMETERS_NO_STALE_STORES.
 *
 * Each of the three IQ-handler methods below builds {@link StoreQueryParameters}
 * via {@code fromNameAndType(name, type)} but never calls {@code .enableStaleStores()}.
 * With the default {@code staleStores=false}, any IQ hitting this pod during the
 * active task's RESTORING window throws {@code InvalidStateStoreException} — the
 * IQ endpoint serves 503 for the entire restore duration even though a standby
 * replica on a sibling pod is fully populated and could answer.
 *
 * The fourth method shows the correct shape: it chains
 * {@code .enableStaleStores()} onto the builder and must NOT fire.
 */
public final class BadIqNoStaleStores {

    private final KafkaStreams streams;

    public BadIqNoStaleStores(KafkaStreams streams) {
        this.streams = streams;
    }

    public Object lookupOrder(String orderId) {
        ReadOnlyKeyValueStore<String, Object> store = streams.store(
                StoreQueryParameters.fromNameAndType("orders-store", QueryableStoreTypes.keyValueStore()));   // FIRES
        return store.get(orderId);
    }

    public Object lookupUser(String userId) {
        StoreQueryParameters<ReadOnlyKeyValueStore<String, Object>> params =
                StoreQueryParameters.fromNameAndType("users-store", QueryableStoreTypes.keyValueStore());     // FIRES
        ReadOnlyKeyValueStore<String, Object> store = streams.store(params);
        return store.get(userId);
    }

    public Object countOrders() {
        ReadOnlyKeyValueStore<String, Long> store = streams.store(
                StoreQueryParameters.fromNameAndType("orders-count-store", QueryableStoreTypes.keyValueStore())); // FIRES
        return store.approximateNumEntries();
    }

    /** Control: chains .enableStaleStores() — must NOT fire. */
    public Object lookupSession(String sessionId) {
        StoreQueryParameters<ReadOnlyKeyValueStore<String, Object>> params =
                StoreQueryParameters.<ReadOnlyKeyValueStore<String, Object>>fromNameAndType(
                                "sessions-store", QueryableStoreTypes.keyValueStore())
                        .enableStaleStores();
        ReadOnlyKeyValueStore<String, Object> store = streams.store(params);
        return store.get(sessionId);
    }
}
