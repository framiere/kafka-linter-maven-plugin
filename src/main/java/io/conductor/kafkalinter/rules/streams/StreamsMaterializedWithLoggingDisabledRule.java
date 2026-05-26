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
 * org.apache.kafka.streams.kstream.Materialized#withLoggingDisabled()
 * Materialized.withLoggingDisabled()} — the fluent setter that
 * strips the state-store's changelog topic, removing the entire
 * fault-tolerance and restore mechanism for the materialized
 * store.
 *
 * <p>Catches both direct {@code INVOKEVIRTUAL} calls on
 * {@code Materialized} and {@code INVOKEDYNAMIC} method-
 * reference captures (e.g. {@code mat::withLoggingDisabled}
 * bound to a {@link java.util.function.UnaryOperator
 * UnaryOperator}/{@link java.util.function.Supplier Supplier} of
 * {@code Materialized}, or {@code Materialized::withLoggingDisabled}
 * bound to a {@link java.util.function.Function Function} of
 * {@code Materialized -> Materialized}).
 *
 * <h2>What withLoggingDisabled actually removes — the full
 * fault-tolerance contract of a Streams state store</h2>
 *
 * <p>A Materialized state store in Kafka Streams is fault-
 * tolerant by default: every write to the local RocksDB (or
 * in-memory) store is mirrored to a compacted Kafka topic
 * called the changelog ({@code app-id-<store-name>-changelog}).
 * The changelog is the ONLY source of truth for the store's
 * contents — the local store is a cache, the changelog is the
 * authority. On task reassignment (rebalance, pod restart,
 * node failure, scale-out, scale-in), Streams reads the
 * changelog from offset 0 (or from the last committed offset
 * for that task) and replays every record into the new task's
 * local store BEFORE that task starts processing input. This
 * is the "stateful restore" contract.
 *
 * <p>{@code withLoggingDisabled()} drops the changelog topic
 * entirely. Every consequence of that change is severe:
 *
 * <ul>
 *   <li><b>The state store is no longer fault-tolerant.</b>
 *       On task reassignment the new task starts with an
 *       EMPTY local store. There is no changelog to replay.
 *       Any aggregate, join-table-state, deduplication-tracker,
 *       or sessionization-state previously held in the store
 *       is irretrievable.</li>
 *   <li><b>Downstream output silently goes wrong.</b> An
 *       aggregate that previously tracked a running sum/max/
 *       quantile now restarts from the Initializer's seed
 *       value on every reassignment. A foreign-key join
 *       backed by this Materialized loses its lookup table
 *       and emits join misses (or empty right sides) until
 *       enough input flows through to repopulate. No error
 *       fires; no alert triggers; the downstream consumer
 *       sees a working but incorrect result.</li>
 *   <li><b>Scale-out is destructive.</b> Adding a new Streams
 *       instance triggers a rebalance; the rebalance reassigns
 *       partitions; each reassigned task's local store is
 *       empty post-rebalance. Without a changelog there is no
 *       restore — the aggregate result drops to zero for every
 *       key on the reassigned partitions for the duration it
 *       takes input to rebuild. Scale-out is supposed to be
 *       transparent; without logging it is a silent data-loss
 *       event.</li>
 *   <li><b>Pod restarts amplify the damage.</b> Streams
 *       instances are commonly run in Kubernetes / Nomad /
 *       ECS with rolling-restart pipelines triggered on every
 *       deploy. Every rolling restart causes a reassignment
 *       wave; every reassignment wave drops state for any
 *       store using {@code withLoggingDisabled()}. A weekly
 *       deploy cadence means weekly silent state loss for
 *       every affected store.</li>
 *   <li><b>Standby replicas are useless.</b>
 *       {@code num.standby.replicas} maintains warm replica
 *       copies of state stores on other instances so that
 *       failover skips the slow changelog replay. With logging
 *       disabled the standby has nothing to warm — the
 *       configuration silently degrades into a no-op for
 *       affected stores.</li>
 *   <li><b>{@code INVOKEDYNAMIC} method-reference captures
 *       bypass naïve {@code MethodInsnNode}-only lint.</b> A
 *       deployment-config builder that defers store hardening
 *       via {@code mat::withLoggingDisabled} bound to a custom
 *       SAM compiles to {@code INVOKEDYNAMIC} whose bsm-args
 *       contain a {@code REF_invokeVirtual} Handle pointing at
 *       {@code Materialized.withLoggingDisabled()Materialized}.
 *       The user-class bytecode contains zero direct {@code
 *       INVOKEVIRTUAL} on the method, only the indy site.</li>
 * </ul>
 *
 * <h2>When (if ever) is withLoggingDisabled safe?</h2>
 *
 * <p>Two narrow cases:
 *
 * <ol>
 *   <li>The Materialized store is a pure-projection of an
 *       upstream <b>compacted</b> source topic with no
 *       in-flight aggregation logic — i.e. the store can be
 *       recomputed entirely by replaying the source topic
 *       from offset 0, and the recompute cost is bounded.</li>
 *   <li>The Streams app is single-instance, never restarted,
 *       and the data is genuinely ephemeral (e.g. a local-only
 *       development setup). In production, this case never
 *       holds.</li>
 * </ol>
 *
 * <p>In every other case the disabled-logging change is a
 * fault-tolerance regression. The rule fires unconditionally
 * and the operator must justify the exemption (case 1 above)
 * in the suppression site.
 *
 * <p>Migration: remove the {@code .withLoggingDisabled()}
 * call. If you need to tune the changelog topic, use {@code
 * .withLoggingEnabled(Map.of("retention.ms", "..."))} to set
 * topic-config overrides without losing fault-tolerance.
 */
public final class StreamsMaterializedWithLoggingDisabledRule implements Rule {

    private static final String OWNER = KafkaTypes.MATERIALIZED;
    private static final Set<String> OWNERS = Set.of(OWNER);
    private static final String METHOD_NAME = "withLoggingDisabled";

    private final Severity severity;

    public StreamsMaterializedWithLoggingDisabledRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_MATERIALIZED_WITH_LOGGING_DISABLED;
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
                RuleId.STREAMS_MATERIALIZED_WITH_LOGGING_DISABLED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Materialized.withLoggingDisabled() — the fluent "
                        + "setter that strips the state-store's "
                        + "changelog topic, removing the entire "
                        + "fault-tolerance and restore mechanism for "
                        + "the materialized store — reached here "
                        + "either as a direct INVOKEVIRTUAL or as an "
                        + "INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `mat::withLoggingDisabled` bound to "
                        + "a UnaryOperator/Supplier of Materialized, "
                        + "or `Materialized::withLoggingDisabled` "
                        + "bound to a Function<Materialized, "
                        + "Materialized>). A Materialized state "
                        + "store in Kafka Streams is fault-tolerant "
                        + "by default: every write to the local "
                        + "RocksDB (or in-memory) store is mirrored "
                        + "to a compacted Kafka topic called the "
                        + "changelog (app-id-<store-name>-changelog); "
                        + "the changelog is the ONLY source of truth "
                        + "for the store's contents — the local "
                        + "store is a cache, the changelog is the "
                        + "authority; on task reassignment "
                        + "(rebalance, pod restart, node failure, "
                        + "scale-out, scale-in), Streams reads the "
                        + "changelog from offset 0 (or from the last "
                        + "committed offset for that task) and "
                        + "replays every record into the new task's "
                        + "local store BEFORE that task starts "
                        + "processing input. withLoggingDisabled() "
                        + "drops the changelog topic entirely. Every "
                        + "consequence of that change is severe: (1) "
                        + "the state store is no longer fault-"
                        + "tolerant — on task reassignment the new "
                        + "task starts with an EMPTY local store, "
                        + "there is no changelog to replay, any "
                        + "aggregate / join-table-state / "
                        + "deduplication-tracker / sessionization-"
                        + "state previously held in the store is "
                        + "irretrievable; (2) downstream output "
                        + "silently goes wrong — an aggregate that "
                        + "previously tracked a running sum/max/"
                        + "quantile now restarts from the "
                        + "Initializer's seed value on every "
                        + "reassignment, a foreign-key join backed "
                        + "by this Materialized loses its lookup "
                        + "table and emits join misses (or empty "
                        + "right sides) until enough input flows "
                        + "through to repopulate, no error fires, "
                        + "no alert triggers, the downstream "
                        + "consumer sees a working but incorrect "
                        + "result; (3) scale-out is destructive — "
                        + "adding a new Streams instance triggers a "
                        + "rebalance, the rebalance reassigns "
                        + "partitions, each reassigned task's local "
                        + "store is empty post-rebalance, without a "
                        + "changelog there is no restore, the "
                        + "aggregate result drops to zero for every "
                        + "key on the reassigned partitions for the "
                        + "duration it takes input to rebuild, "
                        + "scale-out is supposed to be transparent "
                        + "but without logging it is a silent data-"
                        + "loss event; (4) pod restarts amplify the "
                        + "damage — Streams instances are commonly "
                        + "run in Kubernetes / Nomad / ECS with "
                        + "rolling-restart pipelines triggered on "
                        + "every deploy, every rolling restart "
                        + "causes a reassignment wave, every "
                        + "reassignment wave drops state for any "
                        + "store using withLoggingDisabled(), a "
                        + "weekly deploy cadence means weekly silent "
                        + "state loss for every affected store; (5) "
                        + "standby replicas are useless — num."
                        + "standby.replicas maintains warm replica "
                        + "copies of state stores on other instances "
                        + "so that failover skips the slow changelog "
                        + "replay, with logging disabled the standby "
                        + "has nothing to warm, the configuration "
                        + "silently degrades into a no-op for "
                        + "affected stores; (6) INVOKEDYNAMIC method-"
                        + "reference captures bypass naive "
                        + "MethodInsnNode-only lint — a deployment-"
                        + "config builder that defers store "
                        + "hardening via `mat::withLoggingDisabled` "
                        + "bound to a custom SAM compiles to "
                        + "INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeVirtual Handle pointing at "
                        + "Materialized.withLoggingDisabled()"
                        + "Materialized, the user-class bytecode "
                        + "contains zero direct INVOKEVIRTUAL on "
                        + "the method, only the indy site. Two "
                        + "narrow exemptions exist: (a) the "
                        + "Materialized store is a pure-projection "
                        + "of an upstream COMPACTED source topic "
                        + "with no in-flight aggregation logic — "
                        + "the store can be recomputed entirely by "
                        + "replaying the source topic from offset 0 "
                        + "and the recompute cost is bounded; (b) "
                        + "the Streams app is single-instance, "
                        + "never restarted, and the data is "
                        + "genuinely ephemeral (e.g. a local-only "
                        + "development setup) — in production this "
                        + "case never holds. Migration: remove the "
                        + ".withLoggingDisabled() call. If you need "
                        + "to tune the changelog topic, use "
                        + ".withLoggingEnabled(Map.of(\"retention."
                        + "ms\", \"...\")) to set topic-config "
                        + "overrides without losing fault-"
                        + "tolerance.");
    }
}
