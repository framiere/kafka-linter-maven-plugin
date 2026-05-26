package io.conductor.kafkalinter.rules.streams;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Handle;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Fires for every reach of {@link
 * org.apache.kafka.streams.state.StoreBuilder#withLoggingDisabled()
 * StoreBuilder.withLoggingDisabled()} — the fluent setter that
 * strips the changelog topic from a Processor-API state store,
 * removing the entire fault-tolerance and restore mechanism for
 * the store.
 *
 * <p>Catches both direct {@code INVOKEINTERFACE} calls on
 * {@code StoreBuilder} (StoreBuilder is an interface) and
 * {@code INVOKEDYNAMIC} method-reference captures (e.g.
 * {@code builder::withLoggingDisabled} bound to a {@link
 * java.util.function.Supplier Supplier}/{@link
 * java.util.function.UnaryOperator UnaryOperator} of
 * {@code StoreBuilder}, or {@code
 * StoreBuilder::withLoggingDisabled} bound to a {@link
 * java.util.function.Function Function} of
 * {@code StoreBuilder -> StoreBuilder}).
 *
 * <h2>StoreBuilder vs Materialized — same hazard, different
 * surface — why this rule is a NECESSARY companion to
 * STREAMS_MATERIALIZED_WITH_LOGGING_DISABLED</h2>
 *
 * <p>Kafka Streams has two parallel state-store wiring paths:
 *
 * <ul>
 *   <li><b>DSL path — {@link
 *       org.apache.kafka.streams.kstream.Materialized
 *       Materialized}.</b> Used by {@code count()}, {@code
 *       reduce()}, {@code aggregate()}, {@code KTable},
 *       {@code GlobalKTable}, {@code KStream.toTable()},
 *       foreign-key joins, etc. Covered by {@link
 *       StreamsMaterializedWithLoggingDisabledRule}.</li>
 *   <li><b>Processor-API path — {@link
 *       org.apache.kafka.streams.state.StoreBuilder
 *       StoreBuilder}.</b> Used by {@code Topology
 *       .addStateStore(builder)}, {@code
 *       StreamsBuilder.addStateStore(builder)}, custom {@code
 *       Processor.process()} bodies that {@code context()
 *       .getStateStore(name)} their store and write to it
 *       directly. Covered by THIS rule.</li>
 * </ul>
 *
 * <p>The two surfaces compile to disjoint bytecode — a single-
 * rule "look for {@code withLoggingDisabled} on anything" would
 * over-match Streams-internal classes and miss the type-checked
 * dispatch site. The two dedicated rules pair-cover the entire
 * state-store change-fault-tolerance surface of Streams.
 *
 * <h2>Why the Processor-API loss is even worse than the DSL
 * loss</h2>
 *
 * <p>The DSL aggregations (covered by the sibling rule) at
 * least have a bounded recovery path on changelog loss:
 * Streams replays the upstream source topic, re-runs the
 * aggregator, and eventually catches up — wrong during the
 * window but eventually self-healing.
 *
 * <p>Processor-API stores have NO such guarantee. Common
 * Processor-API patterns include:
 *
 * <ul>
 *   <li><b>Stateful deduplication.</b> A {@code
 *       Processor.process()} that consults the store for "has
 *       this message ID been seen before? if no, emit and
 *       record". On changelog loss the store is empty after
 *       reassignment; every message ID seen before the
 *       reassignment will be emitted AGAIN; downstream
 *       consumers see duplicates with no upstream signal that
 *       this is a replay vs a genuinely-new message.</li>
 *   <li><b>Sessionization / windowing on the PAPI path.</b> A
 *       custom session tracker that holds an "active session"
 *       record per user in the store. On reassignment the
 *       store is empty; every subsequent input record is
 *       treated as a NEW session start; the actual session
 *       boundaries are silently shifted; downstream session-
 *       length metrics report shorter sessions than reality.</li>
 *   <li><b>Cross-record state machines.</b> A Processor that
 *       maintains a per-key FSM ({@code PENDING -> CONFIRMED
 *       -> SHIPPED}). On reassignment every key snaps back to
 *       the initial state of the FSM; events that should have
 *       transitioned the FSM forward are dropped because the
 *       preconditions are no longer met; the downstream
 *       audit log is wrong with no error signal.</li>
 *   <li><b>Punctuator-driven scheduled output.</b> A
 *       {@link org.apache.kafka.streams.processor.api.ProcessorContext
 *       ProcessorContext}.{@code schedule()} punctuator that
 *       reads the store at wall-clock intervals and emits an
 *       aggregate ({@code "open orders > 1 hour old"}). On
 *       reassignment the store is empty; the punctuator emits
 *       zero output for the affected partitions; the
 *       downstream alarm that fires on "no open orders" goes
 *       false-quiet for the recovery window.</li>
 * </ul>
 *
 * <p>The common pattern: Processor-API stores hold logic the
 * DSL cannot recover from a source topic alone. The state
 * stored is not a reduction OVER input; it is a derived
 * representation that requires an Initializer / setup step the
 * runtime has no way to re-execute.
 *
 * <h2>Other Processor-API hazards bundled into the failure
 * message</h2>
 *
 * <ul>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       store-wiring helper that builds the StoreBuilder via
 *       {@code builder::withLoggingDisabled} bound to a
 *       UnaryOperator compiles to {@code INVOKEDYNAMIC} whose
 *       bsm-args contain a {@code REF_invokeInterface} Handle
 *       pointing at {@code StoreBuilder
 *       .withLoggingDisabled()StoreBuilder}. Naive linters
 *       miss it.</li>
 * </ul>
 *
 * <p>Migration: remove the {@code .withLoggingDisabled()}
 * call. If you need to tune the changelog topic, use {@code
 * .withLoggingEnabled(Map.of("retention.ms", "..."))} to set
 * topic-config overrides without losing fault-tolerance.
 */
public final class StreamsStoreBuilderWithLoggingDisabledRule implements Rule {

    private static final String OWNER = KafkaTypes.STORE_BUILDER;
    private static final Set<String> OWNERS = Set.of(OWNER);
    private static final String METHOD_NAME = "withLoggingDisabled";

    private final Severity severity;

    public StreamsStoreBuilderWithLoggingDisabledRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_STOREBUILDER_WITH_LOGGING_DISABLED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNER.equals(mi.owner)
                        && METHOD_NAME.equals(mi.name)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, null);
                    if (h != null) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.STREAMS_STOREBUILDER_WITH_LOGGING_DISABLED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "StoreBuilder.withLoggingDisabled() — the fluent "
                        + "setter that strips the changelog topic "
                        + "from a Processor-API state store, removing "
                        + "the entire fault-tolerance and restore "
                        + "mechanism for the store — reached here "
                        + "either as a direct INVOKEINTERFACE "
                        + "(StoreBuilder is an interface) or as an "
                        + "INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `builder::withLoggingDisabled` bound "
                        + "to a Supplier/UnaryOperator of "
                        + "StoreBuilder, or `StoreBuilder::"
                        + "withLoggingDisabled` bound to a "
                        + "Function<StoreBuilder, StoreBuilder>). "
                        + "Kafka Streams has two parallel state-store "
                        + "wiring paths: the DSL path (Materialized) "
                        + "used by count/reduce/aggregate/KTable/"
                        + "GlobalKTable/toTable/foreign-key-joins, and "
                        + "the Processor-API path (StoreBuilder) used "
                        + "by Topology.addStateStore(builder), "
                        + "StreamsBuilder.addStateStore(builder), "
                        + "custom Processor.process() bodies that "
                        + "context().getStateStore(name) their store "
                        + "and write to it directly — this rule "
                        + "covers the PAPI path; the DSL path is "
                        + "covered by STREAMS_MATERIALIZED_WITH_"
                        + "LOGGING_DISABLED. The Processor-API loss "
                        + "is even worse than the DSL loss: the DSL "
                        + "aggregations at least have a bounded "
                        + "recovery path on changelog loss (Streams "
                        + "replays the upstream source topic, re-runs "
                        + "the aggregator, and eventually catches up "
                        + "— wrong during the window but eventually "
                        + "self-healing); Processor-API stores have "
                        + "NO such guarantee. Common Processor-API "
                        + "patterns that go badly wrong on changelog "
                        + "loss: (1) stateful deduplication — a "
                        + "Processor.process() that consults the "
                        + "store for \"has this message ID been seen "
                        + "before? if no, emit and record\", on "
                        + "changelog loss the store is empty after "
                        + "reassignment, every message ID seen before "
                        + "the reassignment will be emitted AGAIN, "
                        + "downstream consumers see duplicates with "
                        + "no upstream signal that this is a replay "
                        + "vs a genuinely-new message; (2) "
                        + "sessionization / windowing on the PAPI "
                        + "path — a custom session tracker that "
                        + "holds an \"active session\" record per "
                        + "user in the store, on reassignment the "
                        + "store is empty, every subsequent input "
                        + "record is treated as a NEW session start, "
                        + "the actual session boundaries are silently "
                        + "shifted, downstream session-length metrics "
                        + "report shorter sessions than reality; (3) "
                        + "cross-record state machines — a Processor "
                        + "that maintains a per-key FSM (PENDING -> "
                        + "CONFIRMED -> SHIPPED), on reassignment "
                        + "every key snaps back to the initial state "
                        + "of the FSM, events that should have "
                        + "transitioned the FSM forward are dropped "
                        + "because the preconditions are no longer "
                        + "met, the downstream audit log is wrong "
                        + "with no error signal; (4) punctuator-"
                        + "driven scheduled output — a "
                        + "ProcessorContext.schedule() punctuator "
                        + "that reads the store at wall-clock "
                        + "intervals and emits an aggregate (\"open "
                        + "orders > 1 hour old\"), on reassignment "
                        + "the store is empty, the punctuator emits "
                        + "zero output for the affected partitions, "
                        + "the downstream alarm that fires on \"no "
                        + "open orders\" goes false-quiet for the "
                        + "recovery window. The common pattern: "
                        + "Processor-API stores hold logic the DSL "
                        + "cannot recover from a source topic alone; "
                        + "the state stored is not a reduction OVER "
                        + "input but a derived representation that "
                        + "requires an Initializer / setup step the "
                        + "runtime has no way to re-execute. (5) "
                        + "INVOKEDYNAMIC method-reference captures "
                        + "bypass naive MethodInsnNode-only lint — a "
                        + "store-wiring helper that builds the "
                        + "StoreBuilder via `builder::"
                        + "withLoggingDisabled` bound to a "
                        + "UnaryOperator compiles to INVOKEDYNAMIC "
                        + "whose bsm-args contain a "
                        + "REF_invokeInterface Handle pointing at "
                        + "StoreBuilder.withLoggingDisabled()"
                        + "StoreBuilder, naive linters miss it. "
                        + "Migration: remove the "
                        + ".withLoggingDisabled() call. If you need "
                        + "to tune the changelog topic, use "
                        + ".withLoggingEnabled(Map.of(\"retention."
                        + "ms\", \"...\")) to set topic-config "
                        + "overrides without losing fault-"
                        + "tolerance.");
    }
}
