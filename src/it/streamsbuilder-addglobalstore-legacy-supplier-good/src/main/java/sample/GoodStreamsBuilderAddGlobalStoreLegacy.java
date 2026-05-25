package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * RULE: STREAMSBUILDER_ADDGLOBALSTORE_LEGACY_SUPPLIER — must NOT
 * fire on the call sites below.
 *
 * <p>All shapes target the KIP-820 migration replacement
 * {@code StreamsBuilder.addGlobalStore(StoreBuilder<?>, String,
 * Consumed<KIn, VIn>, api.ProcessorSupplier<KIn, VIn, Void, Void>)}.
 * Same owner ({@code StreamsBuilder}), same method name
 * ({@code addGlobalStore}), but the descriptor differs — the
 * supplier-typed argument is the new {@code api.ProcessorSupplier},
 * so the rule's {@code contains(legacy) && !contains(new)}
 * predicate rejects every site here.
 *
 * <p>The new supplier additionally constrains {@code KOut/VOut} to
 * {@code Void}, so any accidental {@code context.forward(...)} call
 * inside the global processor body fails at compile time. The
 * {@code Consumed<KIn, VIn>} type-parameters on the call site are
 * now constrained to match the supplier's input types as well —
 * javac enforces the cross-argument type relationship the legacy
 * raw supplier could not.
 */
public final class GoodStreamsBuilderAddGlobalStoreLegacy {

    @FunctionalInterface
    public interface AddGlobalStoreFn {
        StreamsBuilder apply(
                StoreBuilder<?> storeBuilder,
                String topic,
                Consumed<String, String> consumed,
                ProcessorSupplier<String, String, Void, Void> supplier);
    }

    private static ProcessorSupplier<String, String, Void, Void> newSupplier() {
        return () -> new Processor<String, String, Void, Void>() {
            @Override
            public void init(ProcessorContext<Void, Void> context) {
            }

            @Override
            public void process(Record<String, String> record) {
            }

            @Override
            public void close() {
            }
        };
    }

    public StreamsBuilder directAddGlobalStore(StreamsBuilder builder, StoreBuilder<?> storeBuilder) {
        return builder.addGlobalStore(
                storeBuilder,
                "topic",
                Consumed.with(Serdes.String(), Serdes.String()),
                newSupplier());
    }

    public AddGlobalStoreFn capturedAddGlobalStore(StreamsBuilder builder) {
        return builder::addGlobalStore;
    }
}
