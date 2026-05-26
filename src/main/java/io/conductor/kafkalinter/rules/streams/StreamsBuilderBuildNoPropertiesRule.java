package io.conductor.kafkalinter.rules.streams;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Fires for every reach of {@link
 * org.apache.kafka.streams.StreamsBuilder#build()
 * StreamsBuilder.build()} — the no-argument overload whose
 * erased descriptor is {@code
 * ()Lorg/apache/kafka/streams/Topology;}. The
 * {@code build(Properties)} overload
 * ({@code (Ljava/util/Properties;)Lorg/apache/kafka/streams/Topology;})
 * is the safe one and is intentionally not flagged.
 *
 * <p>Catches both direct {@code INVOKEVIRTUAL} calls and
 * {@code INVOKEDYNAMIC} method-reference captures
 * (e.g. {@code builder::build} bound to {@code
 * Supplier<Topology>}).
 *
 * <h2>Why {@code StreamsBuilder.build()} (no Properties)
 * silently emits an UN-OPTIMIZED topology even when
 * {@code topology.optimization} is set in the Streams config</h2>
 *
 * <p>The Kafka Streams DSL builds a logical processor-graph as
 * methods are chained, then translates that graph into a
 * physical {@link org.apache.kafka.streams.Topology Topology}
 * when {@code build()} is called. Two of Streams' most
 * impactful optimizations — {@code REUSE_KTABLE_SOURCE_TOPICS}
 * (KIP-295, "source-KTable reuse") and {@code
 * MERGE_REPARTITION_TOPICS} (KIP-733, "self-join optimization"
 * and repartition-topic merging) — are graph rewrites that
 * mutate node identity and edge structure. They are toggled by
 * the {@code topology.optimization} Streams config key (values
 * {@code all}, {@code none}, or a comma-separated list of
 * specific optimization names from {@code
 * StreamsConfig.OPTIMIZE} / {@code REUSE_KTABLE_SOURCE_TOPICS}
 * / {@code MERGE_REPARTITION_TOPICS}).
 *
 * <p>The catch — and the reason this rule exists — is that
 * the config value is read by {@code build(Properties)} from
 * the Properties argument, NOT from any cluster-wide or
 * application-wide global. The no-argument {@code build()}
 * overload has no Properties to read; the optimization flag is
 * SILENTLY IGNORED and {@code build()} returns the un-optimized
 * topology. The Streams application is then created with
 * {@code new KafkaStreams(topology, props)} where props
 * contains {@code topology.optimization=all} — but the topology
 * has already been emitted un-optimized; the runtime applies
 * no further rewrites and the props key becomes dead config.
 *
 * <h2>Concrete failure modes (carried verbatim into the
 * violation message)</h2>
 *
 * <ul>
 *   <li><b>REUSE_KTABLE_SOURCE_TOPICS optimization disabled —
 *       every KTable source materializes a redundant changelog
 *       topic and a redundant state store.</b> A topology with
 *       {@code builder.table("orders-topic", ...)} normally
 *       triggers Streams to reuse the source topic
 *       {@code orders-topic} AS the changelog for the materialized
 *       state store (the topic is already compacted and contains
 *       the full table state). With the optimization disabled,
 *       Streams creates an internal changelog topic {@code
 *       {app-id}-orders-table-CHANGELOG} and BACKUP-WRITES every
 *       record from orders-topic into it — doubling broker disk
 *       usage for the table's lifetime, doubling the produce
 *       throughput the application generates, and doubling the
 *       page cache footprint. For a 500 GB compacted source
 *       topic this means an extra 500 GB on the broker that
 *       could be reclaimed by enabling the optimization.</li>
 *   <li><b>MERGE_REPARTITION_TOPICS optimization disabled —
 *       a chained {@code selectKey -> groupByKey -> aggregate}
 *       creates TWO repartition topics instead of one.</b> The
 *       un-optimized topology emits one repartition topic after
 *       {@code selectKey} (re-keying the stream) and a SECOND
 *       repartition topic implicit in {@code groupByKey} (the
 *       grouping operator's own repartition because the
 *       upstream is now repartition-tainted). The optimized
 *       form merges these into one repartition topic by
 *       deferring the re-key materialization until the grouping
 *       boundary. With the optimization off the application
 *       produces 2x repartition traffic and reads 2x repartition
 *       traffic per grouped record — measured at scale, a
 *       100 MB/s input stream becomes 200 MB/s of repartition
 *       broker traffic instead of 100 MB/s.</li>
 *   <li><b>Internal topic names DIFFER between optimized and
 *       un-optimized topologies — flipping the flag in
 *       production orphans every internal topic.</b> A team
 *       running with {@code build()} (un-optimized) for months
 *       has internal topic names like {@code
 *       {app-id}-KSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition}
 *       and {@code {app-id}-KSTREAM-AGGREGATE-STATE-STORE-
 *       0000000005-changelog}. Migrating to {@code
 *       build(props)} with {@code topology.optimization=all}
 *       SHIFTS the node identities — the same logical aggregate
 *       now sits at graph index 4 (one fewer node, because
 *       repartition merging removed one). The application starts
 *       up creating {@code KSTREAM-AGGREGATE-STATE-STORE-
 *       0000000004-*} topics, orphans the {@code -0000000005-*}
 *       topics on the broker (still carrying months of compacted
 *       state with INFINITE retention), and has to fully restore
 *       state from the new (empty) changelog — multi-hour blank
 *       restoration time on first restart after the flag flip,
 *       during which the application produces NO output.</li>
 *   <li><b>{@code topology.optimization=all} in props becomes
 *       silent dead config; configuration drift is undetectable
 *       by inspecting the prod props file.</b> An operator
 *       reading {@code application.properties} sees {@code
 *       topology.optimization=all} and trusts that the topology
 *       is optimized. The {@code build()} site (in code, not
 *       in props) silently ignores the directive. There is no
 *       startup-log warning, no metric, and no exception — the
 *       only evidence is a topology that doesn't match what the
 *       config asks for. A new SRE joining the team has no way
 *       to discover the mismatch without reading the build site
 *       and the props file together and reasoning about the
 *       overload dispatch.</li>
 *   <li><b>topology.describe() output differs between dev (run
 *       through a test that calls {@code build(props)}) and
 *       prod (which calls {@code build()}) — making
 *       reproducing a prod-only bug locally impossible.</b> A
 *       repartition-topic-related bug that only manifests in
 *       prod is unreproducible in dev because dev's topology
 *       has different node names and different repartition
 *       structure. The team chases a phantom for days before
 *       realizing the test harness called {@code
 *       build(testProps)} and prod called {@code build()}.</li>
 *   <li><b>KIP-733 self-join optimization is disabled — a
 *       self-join allocates two state stores instead of one.</b>
 *       The KStream self-join pattern {@code stream.join(stream,
 *       joiner, windows)} normally allocates ONE state store
 *       shared between both sides (since the data is identical).
 *       With the optimization off, the un-optimized topology
 *       treats the two sides as independent KStreams, allocates
 *       TWO state stores, and writes every record to BOTH —
 *       doubling state-store disk, doubling RocksDB write
 *       amplification, and doubling the restoration time on
 *       changelog replay.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b>
 *       A topology factory built as {@code Supplier<Topology>
 *       f = builder::build} captures an {@code INVOKEDYNAMIC}
 *       whose bsm-args contain a {@code REF_invokeVirtual}
 *       Handle on {@code StreamsBuilder.build()Topology}. The
 *       user-class bytecode contains zero direct {@code
 *       INVOKEVIRTUAL} on the no-arg overload, only the indy
 *       site. The deferred call has the same effect as a direct
 *       no-arg {@code build()}.</li>
 * </ul>
 *
 * <p>Migration: pass the Streams Properties to {@code build()}
 * — {@code Properties props = streamsProperties();
 * Topology topology = builder.build(props); new
 * KafkaStreams(topology, props)}. The same props object should
 * be passed to BOTH {@code build(props)} (for topology-graph
 * rewrites) AND {@code new KafkaStreams(topology, props)} (for
 * runtime config). If you do not want any optimization, set
 * {@code topology.optimization=none} explicitly in props so
 * the intent is visible at the config layer rather than hidden
 * in the {@code build()} overload choice.
 */
public final class StreamsBuilderBuildNoPropertiesRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.STREAMS_BUILDER);
    private static final String METHOD_NAME = "build";
    private static final String UNSAFE_DESC = "()Lorg/apache/kafka/streams/Topology;";

    private final Severity severity;

    public StreamsBuilderBuildNoPropertiesRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_BUILDER_BUILD_NO_PROPERTIES;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && UNSAFE_DESC.equals(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy
                        && AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, UNSAFE_DESC) != null) {
                    out.add(violation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_BUILDER_BUILD_NO_PROPERTIES, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "StreamsBuilder.build() (no Properties argument) "
                        + "is reached here — either as a direct "
                        + "INVOKEVIRTUAL or as an INVOKEDYNAMIC "
                        + "method-reference capture (e.g. "
                        + "`builder::build` bound to "
                        + "Supplier<Topology> whose erased "
                        + "implMethod descriptor matches "
                        + "()Lorg/apache/kafka/streams/Topology;). "
                        + "The Kafka Streams DSL builds a logical "
                        + "processor-graph as methods are chained, "
                        + "then translates that graph into a "
                        + "physical Topology when build() is "
                        + "called. Two of Streams' most impactful "
                        + "optimizations — REUSE_KTABLE_SOURCE_"
                        + "TOPICS (KIP-295, source-KTable reuse) "
                        + "and MERGE_REPARTITION_TOPICS (KIP-733, "
                        + "self-join optimization and repartition-"
                        + "topic merging) — are graph rewrites "
                        + "that mutate node identity and edge "
                        + "structure. They are toggled by the "
                        + "topology.optimization Streams config "
                        + "key. The catch is that the config "
                        + "value is read by build(Properties) "
                        + "from the Properties argument, NOT from "
                        + "any cluster-wide or application-wide "
                        + "global. The no-argument build() "
                        + "overload has no Properties to read; "
                        + "the optimization flag is SILENTLY "
                        + "IGNORED and build() returns the un-"
                        + "optimized topology. The Streams "
                        + "application is then created with new "
                        + "KafkaStreams(topology, props) where "
                        + "props contains topology.optimization="
                        + "all — but the topology has already "
                        + "been emitted un-optimized; the runtime "
                        + "applies no further rewrites and the "
                        + "props key becomes dead config. "
                        + "Concrete failure modes: (1) REUSE_"
                        + "KTABLE_SOURCE_TOPICS optimization "
                        + "disabled — every KTable source "
                        + "materializes a redundant changelog "
                        + "topic and a redundant state store. A "
                        + "topology with builder.table(\"orders-"
                        + "topic\", ...) normally triggers Streams "
                        + "to reuse the source topic orders-topic "
                        + "AS the changelog for the materialized "
                        + "state store (the topic is already "
                        + "compacted and contains the full table "
                        + "state); with the optimization disabled "
                        + "Streams creates an internal changelog "
                        + "topic {app-id}-orders-table-CHANGELOG "
                        + "and BACKUP-WRITES every record from "
                        + "orders-topic into it — doubling broker "
                        + "disk usage for the table's lifetime, "
                        + "doubling the produce throughput the "
                        + "application generates, and doubling "
                        + "the page cache footprint; for a 500 GB "
                        + "compacted source topic this means an "
                        + "extra 500 GB on the broker that could "
                        + "be reclaimed by enabling the "
                        + "optimization; (2) MERGE_REPARTITION_"
                        + "TOPICS optimization disabled — a "
                        + "chained selectKey -> groupByKey -> "
                        + "aggregate creates TWO repartition "
                        + "topics instead of one; the un-"
                        + "optimized topology emits one "
                        + "repartition topic after selectKey "
                        + "(re-keying the stream) and a SECOND "
                        + "repartition topic implicit in "
                        + "groupByKey (the grouping operator's "
                        + "own repartition because the upstream "
                        + "is now repartition-tainted); the "
                        + "optimized form merges these into one "
                        + "repartition topic by deferring the "
                        + "re-key materialization until the "
                        + "grouping boundary; with the "
                        + "optimization off the application "
                        + "produces 2x repartition traffic and "
                        + "reads 2x repartition traffic per "
                        + "grouped record — a 100 MB/s input "
                        + "stream becomes 200 MB/s of "
                        + "repartition broker traffic instead of "
                        + "100 MB/s; (3) internal topic names "
                        + "DIFFER between optimized and un-"
                        + "optimized topologies — flipping the "
                        + "flag in production orphans every "
                        + "internal topic; a team running with "
                        + "build() (un-optimized) for months has "
                        + "internal topic names like {app-id}-"
                        + "KSTREAM-AGGREGATE-STATE-STORE-"
                        + "0000000005-repartition and {app-id}-"
                        + "KSTREAM-AGGREGATE-STATE-STORE-"
                        + "0000000005-changelog; migrating to "
                        + "build(props) with topology."
                        + "optimization=all SHIFTS the node "
                        + "identities — the same logical "
                        + "aggregate now sits at graph index 4 "
                        + "(one fewer node because repartition "
                        + "merging removed one); the application "
                        + "starts up creating KSTREAM-AGGREGATE-"
                        + "STATE-STORE-0000000004-* topics, "
                        + "orphans the -0000000005-* topics on "
                        + "the broker (still carrying months of "
                        + "compacted state with INFINITE "
                        + "retention), and has to fully restore "
                        + "state from the new (empty) changelog "
                        + "— multi-hour blank restoration time "
                        + "on first restart after the flag flip, "
                        + "during which the application produces "
                        + "NO output; (4) topology.optimization="
                        + "all in props becomes silent dead "
                        + "config; configuration drift is "
                        + "undetectable by inspecting the prod "
                        + "props file — an operator reading "
                        + "application.properties sees topology."
                        + "optimization=all and trusts that the "
                        + "topology is optimized; the build() "
                        + "site (in code, not in props) silently "
                        + "ignores the directive; there is no "
                        + "startup-log warning, no metric, and "
                        + "no exception — the only evidence is "
                        + "a topology that doesn't match what "
                        + "the config asks for; a new SRE "
                        + "joining the team has no way to "
                        + "discover the mismatch without reading "
                        + "the build site and the props file "
                        + "together; (5) topology.describe() "
                        + "output differs between dev (run "
                        + "through a test that calls "
                        + "build(props)) and prod (which calls "
                        + "build()) — making reproducing a prod-"
                        + "only bug locally impossible; a "
                        + "repartition-topic-related bug that "
                        + "only manifests in prod is "
                        + "unreproducible in dev because dev's "
                        + "topology has different node names and "
                        + "different repartition structure; the "
                        + "team chases a phantom for days before "
                        + "realizing the test harness called "
                        + "build(testProps) and prod called "
                        + "build(); (6) KIP-733 self-join "
                        + "optimization is disabled — a self-"
                        + "join allocates two state stores "
                        + "instead of one; the KStream self-"
                        + "join pattern stream.join(stream, "
                        + "joiner, windows) normally allocates "
                        + "ONE state store shared between both "
                        + "sides (since the data is identical); "
                        + "with the optimization off the un-"
                        + "optimized topology treats the two "
                        + "sides as independent KStreams, "
                        + "allocates TWO state stores, and "
                        + "writes every record to BOTH — "
                        + "doubling state-store disk, doubling "
                        + "RocksDB write amplification, and "
                        + "doubling the restoration time on "
                        + "changelog replay; (7) INVOKEDYNAMIC "
                        + "method-reference captures bypass "
                        + "naive MethodInsnNode-only lint — "
                        + "Supplier<Topology> f = builder::build "
                        + "captures an INVOKEDYNAMIC whose bsm-"
                        + "args contain a REF_invokeVirtual "
                        + "Handle on StreamsBuilder.build()"
                        + "Topology; the user-class bytecode "
                        + "contains zero direct INVOKEVIRTUAL "
                        + "on the no-arg overload, only the "
                        + "indy site; the deferred call has the "
                        + "same effect as a direct no-arg "
                        + "build(). Migration: pass the Streams "
                        + "Properties to build() — Properties "
                        + "props = streamsProperties(); Topology "
                        + "topology = builder.build(props); new "
                        + "KafkaStreams(topology, props). The "
                        + "same props object should be passed "
                        + "to BOTH build(props) (for topology-"
                        + "graph rewrites) AND new KafkaStreams("
                        + "topology, props) (for runtime "
                        + "config). If you do not want any "
                        + "optimization, set topology."
                        + "optimization=none explicitly in props "
                        + "so the intent is visible at the "
                        + "config layer rather than hidden in "
                        + "the build() overload choice.");
    }
}
