package sample;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * RULE: STREAMS_BUILDER_BUILD_NO_PROPERTIES — must fire
 * EXACTLY 8 times across this class (one per method below).
 *
 * <p>Exercises the unsafe overload descriptor of
 * {@link StreamsBuilder#build()} — the no-argument variant
 * whose erased descriptor is {@code
 * ()Lorg/apache/kafka/streams/Topology;}. Every reach must
 * fire, whether as a direct {@code INVOKEVIRTUAL} or as an
 * {@code INVOKEDYNAMIC} method-reference capture. The
 * {@link StreamsBuilder#build(java.util.Properties)} overload
 * is the safe one and is intentionally not exercised here.
 */
public final class BadStreamsBuilderBuildNoProperties {

    /** 0-arg SAM whose erased descriptor matches
     *  {@code StreamsBuilder.build()Topology} when bound to a
     *  StreamsBuilder receiver. */
    @FunctionalInterface
    interface TopologyFactory {
        Topology make();
    }

    // ===== Direct INVOKEVIRTUAL on StreamsBuilder.build() =====

    /** MUST FIRE — direct INVOKEVIRTUAL on
     *  StreamsBuilder.build() (no Properties). */
    public Topology buildA() {
        StreamsBuilder builder = new StreamsBuilder();
        return builder.build();
    }

    /** MUST FIRE — direct INVOKEVIRTUAL on
     *  StreamsBuilder.build() after some DSL setup; the
     *  topology graph is non-empty so build() does real
     *  work, but still un-optimized because no props. */
    public Topology buildB() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream("source-topic");
        return builder.build();
    }

    /** MUST FIRE — direct INVOKEVIRTUAL on a builder
     *  received as a method parameter. */
    public Topology buildC(StreamsBuilder externalBuilder) {
        return externalBuilder.build();
    }

    /** MUST FIRE — direct INVOKEVIRTUAL inside a {@code
     *  static} helper; descriptor and owner unchanged. */
    public static Topology buildD() {
        StreamsBuilder builder = new StreamsBuilder();
        return builder.build();
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code builder::build} as a bound-
     *  receiver method reference. Indy implMethod handle
     *  descriptor matches StreamsBuilder.build()Topology
     *  — the no-Properties overload. The user-class
     *  bytecode contains zero direct INVOKEVIRTUAL on
     *  build() inside this method; the unsafe call is
     *  only reachable through the indy site. */
    public Supplier<Topology> buildSupplierFactory(StreamsBuilder builder) {
        return builder::build;
    }

    /** MUST FIRE — {@code StreamsBuilder::build} as an
     *  UNBOUND class-method reference. The indy site's
     *  bsm-args still carry a REF_invokeVirtual Handle on
     *  StreamsBuilder.build()Topology — same unsafe
     *  descriptor, just discovered through a Function
     *  rather than a Supplier SAM. */
    public Function<StreamsBuilder, Topology> buildFunctionFactory() {
        return StreamsBuilder::build;
    }

    /** MUST FIRE — local SAM binding via bound-receiver
     *  method reference, applied inside the same method.
     *  The indy site is in this method's bytecode; the
     *  factory.make() call is just an invokeinterface on
     *  TopologyFactory.make() which is not flagged. */
    public Topology useLocalFactory(StreamsBuilder builder) {
        TopologyFactory factory = builder::build;
        return factory.make();
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code b.build()} per element. The synthetic
     *  {@code lambda$buildAll$0} carries a direct
     *  INVOKEVIRTUAL on StreamsBuilder.build()Topology.
     *  The rule walks all methods including synthetic
     *  ones, so the fire lands on the synthetic method's
     *  source location, not on buildAll itself. */
    public Stream<Topology> buildAll(List<StreamsBuilder> builders) {
        return builders.stream().map(b -> b.build());
    }

    public static void main(String[] args) {
        new BadStreamsBuilderBuildNoProperties();
    }
}
