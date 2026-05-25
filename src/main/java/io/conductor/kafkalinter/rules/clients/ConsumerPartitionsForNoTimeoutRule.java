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
 * {@link org.apache.kafka.clients.consumer.Consumer#partitionsFor(String)}
 * overload — descriptor {@code (Ljava/lang/String;)Ljava/util/List;}
 * with no Duration argument. Catches both direct
 * {@code INVOKEVIRTUAL}/{@code INVOKEINTERFACE} calls and indirect
 * {@code INVOKEDYNAMIC} method-reference captures (e.g.
 * {@code consumer::partitionsFor} bound to a
 * {@link java.util.function.Function} or a custom SAM) via a dual walk
 * over each method's instructions.
 *
 * <h2>Why no-Duration partitionsFor is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer#partitionsFor(String)}
 * is documented as equivalent to {@code partitionsFor(topic,
 * Duration.ofMillis(defaultApiTimeoutMs))} where
 * {@code default.api.timeout.ms} defaults to 60 s. When the
 * topic-metadata is in the consumer's metadata cache the call returns
 * immediately; when it is missing or stale, the call triggers a
 * metadata-fetch request to the cluster and parks the caller until a
 * response arrives or the budget expires. The cluster controller /
 * brokers can be:
 *
 * <ul>
 *   <li><b>Unreachable</b> — every bootstrap broker the consumer was
 *       configured with is DNS-broken, network-isolated, or simply
 *       wrong (typo in {@code bootstrap.servers}, missing VPC peering,
 *       firewall rule). The metadata fetch reaches no live broker
 *       and the call hangs for the full 60 s before throwing
 *       {@link org.apache.kafka.common.errors.TimeoutException}.</li>
 *   <li><b>Slow</b> — controller GC pause, large cluster metadata
 *       (tens of thousands of partitions), broker overload. The full
 *       60 s budget is consumed per call.</li>
 *   <li><b>Topic-missing</b> — the topic literally does not exist
 *       (typo, not yet created, deleted), and the broker returns
 *       UNKNOWN_TOPIC_OR_PARTITION; the consumer's metadata-refresh
 *       logic retries the fetch until {@code default.api.timeout.ms}
 *       expires before surfacing the error. The caller therefore
 *       waits 60 s to discover a typo that is visible in the broker
 *       response on the first round-trip.</li>
 * </ul>
 *
 * <p>Five concrete failure modes (carried verbatim into the violation
 * message so the engineer reading the lint report understands the
 * "why" without leaving the IDE):
 *
 * <ul>
 *   <li><b>startup-health probes wedge pods for the entire
 *       60 s default.api.timeout.ms on misconfigured bootstrap.</b>
 *       Pattern: a Kubernetes readiness probe (or Spring Boot
 *       {@code HealthIndicator}) calls
 *       {@code consumer.partitionsFor("input-topic")} on startup to
 *       confirm the consumer can reach the cluster. With a
 *       mistyped {@code bootstrap.servers} (or a missing VPC peering)
 *       the call hangs for 60 s; the readiness probe times out; the
 *       pod cycles into CrashLoopBackoff; the actual error (typo in a
 *       single config string) is invisible in the pod's logs because
 *       the lint-target method never returns to throw.</li>
 *   <li><b>topic-existence checks block the request thread of an
 *       admin endpoint for 60 s on a misspelled topic.</b> An admin
 *       REST endpoint receives a {@code POST
 *       /produce?topic=user-events-v2} and calls
 *       {@code consumer.partitionsFor(topic)} to validate the topic
 *       exists before producing; under a typo
 *       ({@code user-events-v3}) the call blocks for 60 s while the
 *       broker repeatedly returns UNKNOWN_TOPIC_OR_PARTITION and the
 *       Consumer's metadata-refresh retries until the budget
 *       expires; the request handler is pinned for a full minute on
 *       a one-character typo.</li>
 *   <li><b>topic-discovery loops that 'fan out' across configured
 *       topics multiply 60 s waits.</b> Pattern: a
 *       {@code for (String topic : configuredTopics)
 *       consumer.partitionsFor(topic);} loop discovers partition
 *       counts on startup. With 20 configured topics on an unreachable
 *       cluster the loop runs for 20 × 60 s = 20 minutes before the
 *       first TimeoutException surfaces — the startup script appears
 *       hung and is killed by an outer supervisor.</li>
 *   <li><b>scheduler-thread pinning when {@code partitionsFor} is
 *       dispatched via {@link java.util.concurrent.ScheduledExecutorService}
 *       method-reference capture.</b> An indy capture
 *       {@code scheduler.scheduleAtFixedRate(() ->
 *       consumer.partitionsFor(topic), ...)} or
 *       {@code consumer::partitionsFor} bound to a custom SAM pins
 *       the executor worker thread for 60 s on cluster outage;
 *       backlogged tasks queue behind it; the executor's queue
 *       saturates and tasks are silently dropped.</li>
 *   <li><b>INVOKEDYNAMIC {@code consumer::partitionsFor} captures
 *       bypass naïve MethodInsnNode-only lint.</b> A
 *       {@link java.util.function.Function}{@code <String,
 *       List<PartitionInfo>>} or custom
 *       {@code @FunctionalInterface PartitionListReader{
 *       List<PartitionInfo> read(String topic);}} parameter bound by
 *       a {@code consumer::partitionsFor} method reference compiles
 *       to {@code INVOKEDYNAMIC} whose bsm-args contain a
 *       {@code REF_invokeVirtual} (KafkaConsumer typed receiver) or
 *       {@code REF_invokeInterface} (Consumer interface typed
 *       receiver) handle pointing at {@code Consumer.partitionsFor(
 *       String)Ljava/util/List;}. The user-class bytecode contains
 *       zero direct {@code INVOKEVIRTUAL} on the no-Duration overload
 *       — only the indy site. A rule that walks only
 *       {@code MethodInsnNode} misses every such site.</li>
 * </ul>
 *
 * <h2>Descriptor discrimination — one no-Duration overload, one
 * bounded overload</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer} declares two
 * partitionsFor overloads with different descriptors:
 *
 * <ul>
 *   <li>Unbounded: {@code (Ljava/lang/String;)Ljava/util/List;} —
 *       partitionsFor(String)</li>
 *   <li>Bounded:
 *       {@code (Ljava/lang/String;Ljava/time/Duration;)Ljava/util/List;}
 *       — partitionsFor(String, Duration)</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the no-Duration descriptor;
 * the bounded overload has a strictly different signature and is
 * never flagged.
 */
public final class ConsumerPartitionsForNoTimeoutRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.CONSUMER_OWNERS;
    private static final String METHOD_NAME = "partitionsFor";
    private static final String NO_TIMEOUT_DESC = "(Ljava/lang/String;)Ljava/util/List;";

    private final Severity severity;

    public ConsumerPartitionsForNoTimeoutRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_PARTITIONS_FOR_NO_TIMEOUT;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && NO_TIMEOUT_DESC.equals(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy
                        && AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, NO_TIMEOUT_DESC) != null) {
                    out.add(violation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.CONSUMER_PARTITIONS_FOR_NO_TIMEOUT, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Consumer.partitionsFor(String) (no Duration) is reached "
                        + "here — either as a direct "
                        + "INVOKEVIRTUAL/INVOKEINTERFACE call or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`consumer::partitionsFor` bound to a Function "
                        + "or a custom SAM, common shapes when "
                        + "startup-health probes or topic-discovery loops "
                        + "dispatch partition-metadata reads via "
                        + "ScheduledExecutorService). The no-Duration "
                        + "overload is documented as equivalent to "
                        + "partitionsFor(topic, Duration.ofMillis("
                        + "defaultApiTimeoutMs)) where "
                        + "default.api.timeout.ms defaults to 60 s — "
                        + "when the topic-metadata is missing or stale, "
                        + "the call triggers a metadata-fetch request to "
                        + "the cluster and parks the caller until a "
                        + "response arrives or the budget expires. Under "
                        + "bootstrap unreachability (every bootstrap "
                        + "broker DNS-broken, network-isolated, or wrong "
                        + "— typo in bootstrap.servers, missing VPC "
                        + "peering, firewall rule), controller slowness "
                        + "(controller GC pause, large cluster metadata, "
                        + "broker overload), or topic-missing "
                        + "(UNKNOWN_TOPIC_OR_PARTITION returned by the "
                        + "broker for a typoed/uncreated/deleted topic; "
                        + "the metadata-refresh logic retries until the "
                        + "60 s budget expires before surfacing the "
                        + "error), the call blocks for the full "
                        + "default.api.timeout.ms. Five concrete failure "
                        + "modes follow: (1) startup-health probes wedge "
                        + "pods for the entire 60 s "
                        + "default.api.timeout.ms on misconfigured "
                        + "bootstrap — a Kubernetes readiness probe or "
                        + "Spring Boot HealthIndicator calls "
                        + "`consumer.partitionsFor('input-topic')` on "
                        + "startup; with a mistyped bootstrap.servers the "
                        + "call hangs for 60 s; the readiness probe times "
                        + "out; the pod cycles into CrashLoopBackoff; "
                        + "the actual error is invisible in the pod's "
                        + "logs because the lint-target method never "
                        + "returns to throw; (2) topic-existence checks "
                        + "block the request thread of an admin endpoint "
                        + "for 60 s on a misspelled topic — an admin "
                        + "REST endpoint receives a `POST "
                        + "/produce?topic=user-events-v2` and calls "
                        + "`consumer.partitionsFor(topic)` to validate "
                        + "the topic exists before producing; under a "
                        + "typo the call blocks for 60 s while the "
                        + "broker repeatedly returns "
                        + "UNKNOWN_TOPIC_OR_PARTITION and the Consumer's "
                        + "metadata-refresh retries until the budget "
                        + "expires; the request handler is pinned for a "
                        + "full minute on a one-character typo; (3) "
                        + "topic-discovery loops that 'fan out' across "
                        + "configured topics multiply 60 s waits — a "
                        + "`for (String topic : configuredTopics) "
                        + "consumer.partitionsFor(topic);` loop discovers "
                        + "partition counts on startup; with 20 "
                        + "configured topics on an unreachable cluster "
                        + "the loop runs for 20 × 60 s = 20 minutes "
                        + "before the first TimeoutException surfaces — "
                        + "the startup script appears hung and is killed "
                        + "by an outer supervisor; (4) scheduler-thread "
                        + "pinning when `partitionsFor` is dispatched via "
                        + "ScheduledExecutorService method-reference "
                        + "capture — an indy capture "
                        + "`scheduler.scheduleAtFixedRate(() -> "
                        + "consumer.partitionsFor(topic), ...)` or "
                        + "`consumer::partitionsFor` bound to a custom "
                        + "SAM pins the executor worker thread for 60 s "
                        + "on cluster outage; backlogged tasks queue "
                        + "behind it; the executor's queue saturates and "
                        + "tasks are silently dropped; (5) INVOKEDYNAMIC "
                        + "`consumer::partitionsFor` captures bypass "
                        + "naïve MethodInsnNode-only lint — a "
                        + "Function<String, List<PartitionInfo>> or "
                        + "custom `@FunctionalInterface "
                        + "PartitionListReader{List<PartitionInfo> "
                        + "read(String topic);}` parameter bound by a "
                        + "`consumer::partitionsFor` method reference "
                        + "compiles to INVOKEDYNAMIC whose bsm-args "
                        + "contain a REF_invokeVirtual or "
                        + "REF_invokeInterface handle pointing at "
                        + "Consumer.partitionsFor(String)Ljava/util/List;"
                        + "; the user-class bytecode contains zero "
                        + "direct INVOKEVIRTUAL on the no-Duration "
                        + "overload, only the indy site. Migration: use "
                        + "the bounded overload `partitionsFor(String, "
                        + "Duration)` matched to the surrounding "
                        + "deadline (HTTP request deadline, "
                        + "readiness-probe timeout, startup budget) and "
                        + "let the TimeoutException surface a "
                        + "bootstrap/cluster/topic-missing error rather "
                        + "than an indefinite-feeling 60 s hang. The "
                        + "bounded overload has descriptor "
                        + "`(Ljava/lang/String;Ljava/time/Duration;)"
                        + "Ljava/util/List;` and is never flagged by "
                        + "this rule.");
    }
}
