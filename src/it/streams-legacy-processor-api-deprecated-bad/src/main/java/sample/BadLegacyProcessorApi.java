package sample;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * RULE: STREAMS_LEGACY_PROCESSOR_API_DEPRECATED — must fire on the
 * four call sites below.
 *
 * <p>The four methods exercise the four distinct bytecode shapes the
 * rule is required to catch — three direct calls (one per
 * (owner, name) combination that exists in the Kafka 3.7 API) plus
 * one {@code INVOKEDYNAMIC} method-reference capture targeting the
 * legacy {@code Topology.addProcessor} overload:
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code Topology.addProcessor(String, ProcessorSupplier,
 *       String...)} where {@code ProcessorSupplier} is the legacy
 *       {@code org.apache.kafka.streams.processor.ProcessorSupplier}.
 *       The descriptor contains
 *       {@code Lorg/apache/kafka/streams/processor/ProcessorSupplier;}
 *       (NOT {@code .../processor/api/ProcessorSupplier;}).</li>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code Topology.addGlobalStore(StoreBuilder, String,
 *       Deserializer, Deserializer, String, String,
 *       ProcessorSupplier)} (the 7-arg overload). Legacy supplier
 *       descriptor.</li>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code StreamsBuilder.addGlobalStore(StoreBuilder, String,
 *       Consumed, ProcessorSupplier)}. Legacy supplier descriptor.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code Topology::addProcessor} bound to a custom 4-arg SAM
 *       whose supplier-typed argument erases to the legacy
 *       {@code org.apache.kafka.streams.processor.ProcessorSupplier}.
 *       javac resolves the method-ref to the legacy overload by
 *       matching the SAM's (receiver, arg-erasures, return) tuple.
 *       The user-class bytecode at this site contains ZERO direct
 *       INVOKEVIRTUAL on the legacy addProcessor — only the
 *       INVOKEDYNAMIC + LambdaMetafactory bridge whose bsm-args
 *       contain a REF_invokeVirtual handle pointing at the legacy
 *       method.</li>
 * </ol>
 *
 * <h2>Why these overloads are deprecated (KIP-820 summary)</h2>
 *
 * <p>The legacy
 * {@code org.apache.kafka.streams.processor.ProcessorSupplier} yields
 * a non-generic {@code Processor} whose {@code process(K, V)} is
 * erased to {@code Object/Object} at the bytecode level. The new
 * {@code org.apache.kafka.streams.processor.api.ProcessorSupplier}
 * yields {@code Processor<KIn, VIn, KOut, VOut>} with a strongly-typed
 * {@code Record<KIn, VIn>} input, a {@code ProcessorContext<KOut, VOut>}
 * generic in the downstream types, and an additional
 * {@code FixedKeyProcessor} variant whose {@code FixedKeyRecord}
 * physically cannot mutate the input key. Five concrete incident
 * classes that the legacy shape caused before KIP-820:
 *
 * <ul>
 *   <li>Type-erased {@code context.forward(k, v)} silently accepts a
 *       downstream-type-incompatible value; the bug ships as a runtime
 *       {@code ClassCastException} inside the downstream node, often
 *       on the first non-test record after deploy.</li>
 *   <li>Headers, timestamp, and partition exposed via thread-local
 *       context lookups whose semantics depend on whether the call
 *       comes from a punctuator or a record handler; a punctuator
 *       reading {@code context.timestamp()} gets wall-clock time, not
 *       event time.</li>
 *   <li>Named-child fan-out is positional ({@code To.child(0)}) — the
 *       same array-index coupling that doomed legacy
 *       {@code KStream.branch(Predicate[])}.</li>
 *   <li>No {@code FixedKeyProcessor} variant — co-partitioned joins
 *       that assumed key preservation break silently with
 *       cross-partition writes when an implementor mutates the key
 *       inside {@code process(K, V)}.</li>
 *   <li>{@code INVOKEDYNAMIC Topology::addProcessor} captures
 *       silently bind to the deprecated overload — the user-class
 *       bytecode contains zero direct INVOKEVIRTUAL on the legacy
 *       method and a name-only walk misses it.</li>
 * </ul>
 */
public final class BadLegacyProcessorApi {

    /**
     * Custom 4-arg SAM that matches the legacy
     * {@code Topology.addProcessor(String, ProcessorSupplier,
     * String...)} signature. Used for shape (4) — an INVOKEDYNAMIC
     * method-ref capture of {@code Topology::addProcessor} bound to
     * this SAM. The supplier-typed argument erases to the legacy
     * {@code org.apache.kafka.streams.processor.ProcessorSupplier}, so
     * javac resolves the method-ref to the legacy overload.
     */
    @FunctionalInterface
    public interface AddProcessorFn {
        Topology apply(Topology t, String name, ProcessorSupplier<?, ?> supplier, String[] parents);
    }

    @SuppressWarnings({"deprecation", "rawtypes", "unchecked"})
    public Topology directTopologyAddProcessor(Topology t) {
        // MUST FIRE — direct INVOKEVIRTUAL on
        // Topology.addProcessor(String, ProcessorSupplier, String...)
        // taking the LEGACY ProcessorSupplier. The descriptor at the
        // call site is
        // (Ljava/lang/String;Lorg/apache/kafka/streams/processor/ProcessorSupplier;[Ljava/lang/String;)Lorg/apache/kafka/streams/Topology;
        // — contains the legacy supplier substring and NOT the new
        // api-package substring.
        ProcessorSupplier<String, String> legacy = LegacyProcessor::new;
        return t.addProcessor("legacy-direct", legacy, "source");
    }

    @SuppressWarnings({"deprecation", "rawtypes", "unchecked"})
    public Topology directTopologyAddGlobalStore(Topology t) {
        // MUST FIRE — direct INVOKEVIRTUAL on the 7-arg
        // Topology.addGlobalStore overload taking the LEGACY
        // ProcessorSupplier. The descriptor at the call site includes
        // the legacy supplier substring and NOT the new api-package
        // substring.
        StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("legacy-global-store"),
                Serdes.String(),
                Serdes.String());
        ProcessorSupplier<String, String> legacy = LegacyProcessor::new;
        return t.addGlobalStore(
                storeBuilder,
                "global-source",
                Serdes.String().deserializer(),
                Serdes.String().deserializer(),
                "global-topic",
                "global-processor",
                legacy);
    }

    @SuppressWarnings({"deprecation", "rawtypes", "unchecked"})
    public StreamsBuilder directStreamsBuilderAddGlobalStore(StreamsBuilder b) {
        // MUST FIRE — direct INVOKEVIRTUAL on
        // StreamsBuilder.addGlobalStore(StoreBuilder, String,
        // Consumed, ProcessorSupplier) taking the LEGACY
        // ProcessorSupplier. The descriptor contains the legacy
        // supplier substring and not the new api-package substring.
        StoreBuilder<KeyValueStore<Bytes, byte[]>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("legacy-sb-global-store"),
                Serdes.Bytes(),
                Serdes.ByteArray());
        ProcessorSupplier<String, String> legacy = () -> new LegacyProcessor();
        return b.addGlobalStore(
                storeBuilder,
                "sb-global-topic",
                Consumed.with(Serdes.String(), Serdes.String()),
                legacy);
    }

    @SuppressWarnings({"deprecation"})
    public AddProcessorFn capturedTopologyAddProcessor() {
        // MUST FIRE — INVOKEDYNAMIC method-ref capture
        // `Topology::addProcessor` bound to a custom 4-arg SAM whose
        // supplier-typed argument erases to the LEGACY
        // org.apache.kafka.streams.processor.ProcessorSupplier. javac
        // resolves the method-ref to the legacy overload at this site
        // because the matching api-package overload would require the
        // SAM's supplier-typed argument to erase to
        // .../processor/api/ProcessorSupplier;. The user-class bytecode
        // at this site contains ZERO direct INVOKEVIRTUAL on the legacy
        // addProcessor — only the INVOKEDYNAMIC bridge whose bsm-args
        // hold a REF_invokeVirtual handle whose desc contains the
        // legacy supplier substring.
        return Topology::addProcessor;
    }

    /**
     * Legacy processor implementation — uses the old
     * {@code AbstractProcessor} + {@code process(K, V)} signature.
     * Used as a {@link ProcessorSupplier} target for the three direct
     * call sites above.
     */
    @SuppressWarnings("deprecation")
    static final class LegacyProcessor extends AbstractProcessor<String, String> {
        @Override
        public void process(String key, String value) {
            ProcessorContext ctx = context();
            ctx.forward(key, value == null ? "" : value.toUpperCase());
        }
    }
}
