package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * RULE: TOPOLOGY_ADD_GLOBAL_STORE_LEGACY_SUPPLIER.
 *
 * Fires when {@code Topology.addGlobalStore(..., ProcessorSupplier)} is called with the
 * LEGACY {@code org.apache.kafka.streams.processor.ProcessorSupplier} (vs the KIP-820 typed
 * {@code org.apache.kafka.streams.processor.api.ProcessorSupplier<KIn, VIn, Void, Void>}).
 * The bytecode descriptor of the legacy overloads contains
 * {@code Lorg/apache/kafka/streams/processor/ProcessorSupplier;} (no /api/ between
 * /processor/ and /ProcessorSupplier;); the new overload descriptors contain
 * {@code Lorg/apache/kafka/streams/processor/api/ProcessorSupplier;}.
 */
public final class BadTopologyAddGlobalStoreLegacySupplier {

    private static StoreBuilder<?> storeBuilder() {
        KeyValueBytesStoreSupplier kv = Stores.inMemoryKeyValueStore("global-store");
        return Stores.keyValueStoreBuilder(kv, Serdes.String(), Serdes.String());
    }

    /** Anti-pattern: legacy 7-arg overload (no TimestampExtractor) — FIRES. */
    public Topology buildLegacySevenArg(Topology t) {
        ProcessorSupplier<String, String> legacy = LegacyProcessor::new;
        return t.addGlobalStore(
                storeBuilder(),
                "global-source",
                new StringDeserializer(),
                new StringDeserializer(),
                "global-topic",
                "global-processor",
                legacy); // FIRES — legacy ProcessorSupplier
    }

    /** Anti-pattern: legacy 8-arg overload (with TimestampExtractor) — FIRES. */
    public Topology buildLegacyEightArg(Topology t, TimestampExtractor extractor) {
        ProcessorSupplier<String, String> legacy = LegacyProcessor::new;
        return t.addGlobalStore(
                storeBuilder(),
                "global-source-ts",
                extractor,
                new StringDeserializer(),
                new StringDeserializer(),
                "global-topic-ts",
                "global-processor-ts",
                legacy); // FIRES — legacy ProcessorSupplier
    }

    /** Control: new typed api.ProcessorSupplier — must NOT fire. */
    public Topology buildNewApi(Topology t) {
        org.apache.kafka.streams.processor.api.ProcessorSupplier<String, String, Void, Void> newApi =
                () -> new org.apache.kafka.streams.processor.api.Processor<>() {
                    @Override
                    public void process(Record<String, String> record) {
                        // populate the global store
                    }
                };
        return t.addGlobalStore(
                storeBuilder(),
                "global-source-new",
                new StringDeserializer(),
                new StringDeserializer(),
                "global-topic-new",
                "global-processor-new",
                newApi);
    }

    /** Legacy processor implementation — uses the old AbstractProcessor + process(K, V) signature. */
    static final class LegacyProcessor extends AbstractProcessor<String, String> {
        @Override
        public void process(String key, String value) {
            ProcessorContext ctx = context();
            ctx.forward(key, value);
        }
    }
}
