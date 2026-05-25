package sample;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

import java.util.List;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call site uses a Named overload of
 * {@code process} or {@code processValues} so the processor node
 * carries a stable name across topology edits.
 */
public final class GoodStreamsProcessNoNamed {

    @FunctionalInterface
    interface ProcessFactoryWithNamed<K, V> {
        KStream<K, V> apply(
                ProcessorSupplier<? super K, ? super V, K, V> supplier,
                Named named,
                String[] stateStoreNames);
    }

    @FunctionalInterface
    interface ProcessValuesFactoryWithNamed<K, V> {
        KStream<K, V> apply(
                FixedKeyProcessorSupplier<? super K, ? super V, V> supplier,
                Named named,
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

    public KStream<String, String> processOrders(KStream<String, String> orders) {
        return orders.process(
                GoodStreamsProcessNoNamed.<String, String>noop(),
                Named.as("enrich-orders"),
                "orders-store");
    }

    public KStream<String, String> processValuesOrders(KStream<String, String> orders) {
        return orders.processValues(
                GoodStreamsProcessNoNamed.<String, String>noopFixed(),
                Named.as("enrich-orders-values"),
                "orders-store");
    }

    public KStream<String, String> processNoStores(KStream<String, String> events) {
        return events.process(
                GoodStreamsProcessNoNamed.<String, String>noop(),
                Named.as("enrich-events"));
    }

    public KStream<String, String> processValuesNoStores(KStream<String, String> events) {
        return events.processValues(
                GoodStreamsProcessNoNamed.<String, String>noopFixed(),
                Named.as("enrich-events-values"));
    }

    public ProcessFactoryWithNamed<String, String>
            buildProcessFactory(KStream<String, String> stream) {
        return stream::process;
    }

    public ProcessValuesFactoryWithNamed<String, String>
            buildProcessValuesFactory(KStream<String, String> stream) {
        return stream::processValues;
    }

    public KStream<String, String> useLocalFactory(KStream<String, String> stream) {
        ProcessFactoryWithNamed<String, String> factory = stream::process;
        return factory.apply(
                GoodStreamsProcessNoNamed.<String, String>noop(),
                Named.as("local-process"),
                new String[]{"local-store"});
    }

    public Stream<KStream<String, String>> processAll(
            KStream<String, String> stream, List<String> storeNames) {
        return storeNames.stream().map(name -> stream.process(
                GoodStreamsProcessNoNamed.<String, String>noop(),
                Named.as("process-" + name),
                name));
    }

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        System.out.println(builder);
        new GoodStreamsProcessNoNamed();
    }
}
