package io.conductor.kafkalinter.rules.clients;

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
 * Fires for every reach of the unbounded
 * {@link org.apache.kafka.common.KafkaFuture#get()} overload — the
 * {@code ()Ljava/lang/Object;} descriptor with no timeout argument.
 *
 * <p>The rule catches both direct {@code INVOKEVIRTUAL} calls and
 * indirect {@code INVOKEDYNAMIC} method-reference captures (e.g.
 * {@code future::get} bound to a {@code Supplier<T>} SAM) via a dual
 * walk over each method's instructions.
 *
 * <h2>Why no-argument get() is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.common.KafkaFuture#get()} parks the
 * calling thread until the kafka-clients machinery resolves the
 * future, with no caller-side deadline. The Admin/AdminClient API
 * surface returns KafkaFuture for every operation that touches the
 * controller — createTopics, deleteTopics, describeCluster,
 * alterConfigs, listConsumerGroupOffsets, describeTopics,
 * incrementalAlterConfigs, listOffsets, deleteRecords, and many
 * more. Every one of those futures resolves on a controller
 * round-trip, and the controller can be:
 *
 * <ul>
 *   <li><b>Unreachable</b> — partial-partition broker outage,
 *       transient DNS / TLS handshake failure, network policy edit
 *       that drops the controller's IP. The kafka-clients metadata
 *       refresh logic retries forever; the unbounded get() does
 *       not surface this to the caller as a deadline.</li>
 *   <li><b>Slow</b> — controller saturated by partition reassignment
 *       or large topic creation. {@code default.api.timeout.ms} is
 *       the only ceiling, and it is configurable to
 *       {@code Long.MAX_VALUE} (and is, by default,
 *       {@code Integer.MAX_VALUE} ms ≈ 24 days in some clients).</li>
 *   <li><b>Stuck</b> — a deadlock in the broker-side controller
 *       state machine. Has happened in production after Kafka
 *       version upgrades that change controller-election semantics.
 *       The future never completes; the caller never returns.</li>
 * </ul>
 *
 * <p>Five concrete failure modes (carried verbatim into the violation
 * message so the engineer reading the lint report understands the
 * "why" without leaving the IDE):
 *
 * <ul>
 *   <li><b>HTTP request handlers hang the request thread
 *       indefinitely.</b> A typical REST endpoint that calls
 *       {@code adminClient.createTopics(...).all().get()} blocks the
 *       servlet thread; under a hung controller, every concurrent
 *       request piles up on the same call, exhausts the servlet
 *       thread pool, and the entire service stops responding. The
 *       request-budget deadline (typically a few seconds) is never
 *       enforced because the unbounded get() has no knowledge of
 *       it.</li>
 *   <li><b>Kubernetes pod terminationGracePeriodSeconds is
 *       exceeded.</b> A graceful-shutdown hook that flushes an
 *       AdminClient operation via {@code future.get()} cannot
 *       complete within the terminationGracePeriodSeconds budget
 *       under a slow controller; the kubelet sends SIGKILL,
 *       in-flight side effects are partially applied, and the next
 *       Pod replay sees the inconsistent state.</li>
 *   <li><b>Reconciliation loops fall behind silently.</b> A
 *       controller-manager-style reconciler that calls
 *       {@code describeCluster().nodes().get()} once per loop has no
 *       cap on how long an individual iteration takes. Under a slow
 *       controller, the loop's effective period stretches from
 *       seconds to hours; reconcile-lag metrics keep climbing but
 *       there is no error to alert on — the call is "still
 *       running".</li>
 *   <li><b>CompletableFuture composition silently inherits the
 *       hang.</b> {@code CompletableFuture.supplyAsync(future::get)}
 *       — a near-universal pattern when bridging the Admin API into
 *       Project Reactor / a CompletableFuture pipeline — captures
 *       the unbounded get() as a method reference. The
 *       CompletableFuture stage never completes, every downstream
 *       {@code .thenCompose} / {@code .thenApply} chained on it
 *       remains pending, and an upstream {@code orTimeout} only
 *       cancels the CompletableFuture wrapper, not the underlying
 *       blocked thread.</li>
 *   <li><b>INVOKEDYNAMIC {@code future::get} captures bypass naïve
 *       MethodInsnNode-only lint.</b> A {@code Supplier<T>}
 *       parameter bound by a {@code future::get} method reference
 *       compiles to {@code INVOKEDYNAMIC} whose bsm-args contain a
 *       {@code REF_invokeVirtual} handle pointing at
 *       {@code KafkaFuture.get()Ljava/lang/Object;}. The user-class
 *       bytecode contains zero direct {@code INVOKEVIRTUAL} on the
 *       no-arg get — only the indy site. A rule that walks only
 *       {@code MethodInsnNode} misses every such site.</li>
 * </ul>
 *
 * <h2>Descriptor discrimination — exact-match on no-arg overload</h2>
 *
 * <p>{@link org.apache.kafka.common.KafkaFuture} declares two get
 * overloads with different descriptors:
 *
 * <ul>
 *   <li>Unbounded: {@code ()Ljava/lang/Object;} — the deprecated
 *       call this rule fires on.</li>
 *   <li>Bounded: {@code (JLjava/util/concurrent/TimeUnit;)Ljava/lang/Object;}
 *       — the supported call passing a deadline.</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the no-arg descriptor; the
 * bounded overload has a strictly different signature and is never
 * flagged.
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code future::get} bound to a SAM whose erased descriptor
 * matches {@code ()Ljava/lang/Object;} (e.g. {@code Supplier<T>},
 * {@code Callable<T>}, any custom {@code @FunctionalInterface
 * Producer<T>}) compiles to {@code INVOKEDYNAMIC} whose bsm-args
 * contain a {@code REF_invokeVirtual} handle (KafkaFuture is an
 * abstract class, not an interface, so the method-handle kind is
 * {@code REF_invokeVirtual}). The rule walks every indy's bsm-args
 * and matches owners × name × descriptor against
 * {@code KafkaFuture × get × ()Ljava/lang/Object;}.
 */
public final class KafkaFutureGetNoTimeoutRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KAFKA_FUTURE);
    private static final String METHOD_NAME = "get";
    private static final String NO_ARG_GET_DESC = "()Ljava/lang/Object;";

    private final Severity severity;

    public KafkaFutureGetNoTimeoutRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.KAFKA_FUTURE_GET_NO_TIMEOUT;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && NO_ARG_GET_DESC.equals(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy
                        && AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, NO_ARG_GET_DESC) != null) {
                    out.add(violation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.KAFKA_FUTURE_GET_NO_TIMEOUT, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KafkaFuture.get() (no-argument, unbounded) is reached here — "
                        + "either as a direct INVOKEVIRTUAL call or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`future::get` bound to a Supplier<T> / Callable<T> "
                        + "SAM, the universal bridge from AdminClient/Producer "
                        + "futures into CompletableFuture pipelines). The no-arg "
                        + "get() parks the calling thread until the kafka-clients "
                        + "machinery resolves the future, with no caller-side "
                        + "deadline. In AdminClient code paths (createTopics, "
                        + "deleteTopics, describeCluster, alterConfigs, "
                        + "listConsumerGroupOffsets, describeTopics, "
                        + "incrementalAlterConfigs, listOffsets, deleteRecords, "
                        + "...), a slow/unreachable/stuck controller can hang the "
                        + "caller indefinitely; the AdminClient's "
                        + "`default.api.timeout.ms` is the only escape and it is "
                        + "configurable to Long.MAX_VALUE. Five concrete failure "
                        + "modes follow: (1) HTTP request handlers hang the "
                        + "request thread indefinitely — a typical REST endpoint "
                        + "that calls "
                        + "`adminClient.createTopics(...).all().get()` blocks the "
                        + "servlet thread; under a hung controller, every "
                        + "concurrent request piles up on the same call, "
                        + "exhausts the servlet thread pool, and the entire "
                        + "service stops responding; (2) Kubernetes pod "
                        + "terminationGracePeriodSeconds is exceeded — a "
                        + "graceful-shutdown hook that flushes an AdminClient "
                        + "operation via `future.get()` cannot complete within "
                        + "the terminationGracePeriodSeconds budget under a slow "
                        + "controller; the kubelet sends SIGKILL, in-flight side "
                        + "effects are partially applied, and the next Pod "
                        + "replay sees the inconsistent state; (3) "
                        + "reconciliation loops fall behind silently — a "
                        + "controller-manager-style reconciler that calls "
                        + "`describeCluster().nodes().get()` once per loop has "
                        + "no cap on how long an individual iteration takes; "
                        + "under a slow controller, the loop's effective period "
                        + "stretches from seconds to hours; reconcile-lag "
                        + "metrics keep climbing but there is no error to alert "
                        + "on — the call is `still running`; (4) "
                        + "CompletableFuture composition silently inherits the "
                        + "hang — `CompletableFuture.supplyAsync(future::get)`, "
                        + "the universal bridge from the Admin API into Project "
                        + "Reactor / a CompletableFuture pipeline, captures the "
                        + "unbounded get() as a method reference; the "
                        + "CompletableFuture stage never completes, every "
                        + "downstream `.thenCompose` / `.thenApply` chained on "
                        + "it remains pending, and an upstream `orTimeout` only "
                        + "cancels the CompletableFuture wrapper, not the "
                        + "underlying blocked thread; (5) INVOKEDYNAMIC "
                        + "`future::get` captures bypass naïve MethodInsnNode-"
                        + "only lint — a Supplier<T> parameter bound by a "
                        + "`future::get` method reference compiles to "
                        + "INVOKEDYNAMIC whose bsm-args contain a "
                        + "REF_invokeVirtual handle pointing at "
                        + "KafkaFuture.get()Ljava/lang/Object;; the user-class "
                        + "bytecode contains zero direct INVOKEVIRTUAL on the "
                        + "no-arg get, only the indy site. Migration: replace "
                        + "with the bounded overload "
                        + "`.get(long timeout, TimeUnit unit)` matched to the "
                        + "surrounding deadline (HTTP request budget, "
                        + "reconciliation interval, "
                        + "terminationGracePeriodSeconds minus a buffer) and "
                        + "let the TimeoutException surface a deadline error "
                        + "rather than an indefinite hang. The bounded overload "
                        + "has descriptor "
                        + "`(JLjava/util/concurrent/TimeUnit;)Ljava/lang/Object;` "
                        + "and is never flagged by this rule.");
    }
}
