package sample;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.util.List;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every {@code build} call uses the
 * Properties-bearing overload {@code build(Properties)} whose
 * erased descriptor is
 * {@code (Ljava/util/Properties;)Lorg/apache/kafka/streams/Topology;}.
 * The descriptor does NOT match the unsafe
 * {@code ()Lorg/apache/kafka/streams/Topology;} so the
 * predicate rejects it.
 */
public final class GoodStreamsBuilderBuildNoProperties {

    private static Properties props() {
        Properties p = new Properties();
        p.setProperty("topology.optimization", "all");
        return p;
    }

    /** 1-arg SAM whose erased descriptor matches
     *  {@code StreamsBuilder.build(Properties)Topology} when
     *  bound to a StreamsBuilder receiver. */
    @FunctionalInterface
    interface PropsTopologyFactory {
        Topology make(Properties props);
    }

    public Topology buildA() {
        StreamsBuilder builder = new StreamsBuilder();
        return builder.build(props());
    }

    public Topology buildB() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream("source-topic");
        return builder.build(props());
    }

    public Topology buildC(StreamsBuilder externalBuilder) {
        return externalBuilder.build(props());
    }

    public static Topology buildD() {
        StreamsBuilder builder = new StreamsBuilder();
        return builder.build(props());
    }

    public Function<Properties, Topology> buildSupplierFactory(StreamsBuilder builder) {
        return builder::build;
    }

    public BiFunction<StreamsBuilder, Properties, Topology> buildFunctionFactory() {
        return StreamsBuilder::build;
    }

    public Topology useLocalFactory(StreamsBuilder builder) {
        PropsTopologyFactory factory = builder::build;
        return factory.make(props());
    }

    public Stream<Topology> buildAll(List<StreamsBuilder> builders) {
        return builders.stream().map(b -> b.build(props()));
    }

    public static void main(String[] args) {
        new GoodStreamsBuilderBuildNoProperties();
    }
}
