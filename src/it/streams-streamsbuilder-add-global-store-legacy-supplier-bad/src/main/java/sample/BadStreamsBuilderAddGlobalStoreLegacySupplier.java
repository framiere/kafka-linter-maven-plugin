package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * RULE: STREAMSBUILDER_ADDGLOBALSTORE_LEGACY_SUPPLIER.
 *
 * Fires when {@code StreamsBuilder.addGlobalStore(StoreBuilder, topic, Consumed, ProcessorSupplier)}
 * is called with the LEGACY {@code org.apache.kafka.streams.processor.ProcessorSupplier}
 * (vs the KIP-820 typed {@code org.apache.kafka.streams.processor.api.ProcessorSupplier<KIn, VIn, Void, Void>}).
 * Same descriptor discriminator as TOPOLOGY_ADD_PROCESSOR_LEGACY_SUPPLIER and
 * TOPOLOGY_ADD_GLOBAL_STORE_LEGACY_SUPPLIER: legacy descriptor contains
 * {@code Lorg/apache/kafka/streams/processor/ProcessorSupplier;} (no /api/), new descriptor contains
 * {@code Lorg/apache/kafka/streams/processor/api/ProcessorSupplier;}.
 */
public final class BadStreamsBuilderAddGlobalStoreLegacySupplier {

    private static StoreBuilder<?> storeBuilder(String name) {
        KeyValueBytesStoreSupplier kv = Stores.inMemoryKeyValueStore(name);
        return Stores.keyValueStoreBuilder(kv, Serdes.String(), Serdes.String());
    }

    /** Anti-pattern: legacy ProcessorSupplier via lambda — FIRES. */
    public StreamsBuilder buildLegacyLambda(StreamsBuilder b) {
        ProcessorSupplier<String, String> legacy = LegacyProcessor::new;
        return b.addGlobalStore(
                storeBuilder("global-lambda"),
                "global-topic-lambda",
                Consumed.with(Serdes.String(), Serdes.String()),
                legacy); // FIRES — legacy ProcessorSupplier
    }

    /** Anti-pattern: legacy ProcessorSupplier as anonymous class — FIRES. */
    public StreamsBuilder buildLegacyAnonymous(StreamsBuilder b) {
        return b.addGlobalStore(
                storeBuilder("global-anon"),
                "global-topic-anon",
                Consumed.with(Serdes.String(), Serdes.String()),
                new ProcessorSupplier<String, String>() {
                    @Override
                    public org.apache.kafka.streams.processor.Processor<String, String> get() {
                        return new LegacyProcessor();
                    }
                }); // FIRES — legacy ProcessorSupplier
    }

    /** Control: new typed api.ProcessorSupplier — must NOT fire. */
    public StreamsBuilder buildNewApi(StreamsBuilder b) {
        org.apache.kafka.streams.processor.api.ProcessorSupplier<String, String, Void, Void> newApi =
                () -> new org.apache.kafka.streams.processor.api.Processor<>() {
                    @Override
                    public void process(Record<String, String> record) {
                        // populate the global store
                    }
                };
        return b.addGlobalStore(
                storeBuilder("global-new"),
                "global-topic-new",
                Consumed.with(Serdes.String(), Serdes.String()),
                newApi);
    }

    /** Legacy processor implementation — old AbstractProcessor + process(K, V) signature. */
    static final class LegacyProcessor extends AbstractProcessor<String, String> {
        @Override
        public void process(String key, String value) {
            ProcessorContext ctx = context();
            ctx.forward(key, value);
        }
    }
}
