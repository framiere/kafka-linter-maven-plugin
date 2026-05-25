package sample;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

import java.util.List;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_PROCESS_NO_NAMED — must fire EXACTLY 8 times
 * across this class (one per method below).
 *
 * <p>Exercises every unsafe overload pair:
 *
 * <ul>
 *   <li>{@code process(ProcessorSupplier, String...)} — UNSAFE</li>
 *   <li>{@code processValues(FixedKeyProcessorSupplier, String...)} — UNSAFE</li>
 * </ul>
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls and
 * {@code INVOKEDYNAMIC} method-reference captures bound to a SAM
 * whose erased implMethod descriptor matches one of the unsafe
 * overloads.
 */
public final class BadStreamsProcessNoNamed {

    /** 2-arg SAM matching {@code (ProcessorSupplier, String[])KStream}. */
    @FunctionalInterface
    interface ProcessFactory<K, V> {
        KStream<K, V> apply(
                ProcessorSupplier<? super K, ? super V, K, V> supplier,
                String[] stateStoreNames);
    }

    /** 2-arg SAM matching {@code (FixedKeyProcessorSupplier, String[])KStream}. */
    @FunctionalInterface
    interface ProcessValuesFactory<K, V> {
        KStream<K, V> apply(
                FixedKeyProcessorSupplier<? super K, ? super V, V> supplier,
                String[] stateStoreNames);
    }

    private static <K, V> ProcessorSupplier<K, V, K, V> noop() {
        return () -> new Processor<K, V, K, V>() {
            @Override
            public void process(Record<K, V> record) {
                // no-op
            }
        };
    }

    private static <K, V> FixedKeyProcessorSupplier<K, V, V> noopFixed() {
        return () -> new FixedKeyProcessor<K, V, V>() {
            @Override
            public void process(FixedKeyRecord<K, V> record) {
                // no-op
            }
        };
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overloads =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  {@code process(ProcessorSupplier, String...)} with one store. */
    public KStream<String, String> processOrders(KStream<String, String> orders) {
        return orders.process(BadStreamsProcessNoNamed.<String, String>noop(), "orders-store");
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  {@code processValues(FixedKeyProcessorSupplier, String...)}
     *  with one store. */
    public KStream<String, String> processValuesOrders(KStream<String, String> orders) {
        return orders.processValues(BadStreamsProcessNoNamed.<String, String>noopFixed(), "orders-store");
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  {@code process(ProcessorSupplier, String...)} with no store. */
    public KStream<String, String> processNoStores(KStream<String, String> events) {
        return events.process(BadStreamsProcessNoNamed.<String, String>noop());
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  {@code processValues(FixedKeyProcessorSupplier, String...)}
     *  with no store. */
    public KStream<String, String> processValuesNoStores(KStream<String, String> events) {
        return events.processValues(BadStreamsProcessNoNamed.<String, String>noopFixed());
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code stream::process} bound to a custom 2-arg
     *  SAM whose erased implMethod descriptor is the unsafe
     *  {@code (ProcessorSupplier, String[])KStream} overload exactly. */
    public ProcessFactory<String, String>
            buildProcessFactory(KStream<String, String> stream) {
        return stream::process;
    }

    /** MUST FIRE — {@code stream::processValues} bound to a custom
     *  2-arg SAM whose erased implMethod descriptor is the unsafe
     *  {@code (FixedKeyProcessorSupplier, String[])KStream}
     *  overload exactly. */
    public ProcessValuesFactory<String, String>
            buildProcessValuesFactory(KStream<String, String> stream) {
        return stream::processValues;
    }

    /** MUST FIRE — local 2-arg SAM binding applied immediately. The
     *  indy site is in this method's bytecode. */
    public KStream<String, String> useLocalFactory(KStream<String, String> stream) {
        ProcessFactory<String, String> factory = stream::process;
        return factory.apply(BadStreamsProcessNoNamed.<String, String>noop(), new String[]{"local-store"});
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code stream.process(supplier, name)}. The synthetic lambda
     *  method's bytecode contains a direct INVOKEINTERFACE on the
     *  unsafe overload — the rule walks all methods on the class
     *  including synthetic lambda bodies and fires there. */
    public Stream<KStream<String, String>> processAll(
            KStream<String, String> stream, List<String> storeNames) {
        return storeNames.stream().map(name -> stream.process(
                BadStreamsProcessNoNamed.<String, String>noop(), name));
    }

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        System.out.println(builder);
        new BadStreamsProcessNoNamed();
    }
}
