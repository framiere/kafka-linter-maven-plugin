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
 * {@link org.apache.kafka.clients.consumer.Consumer#listTopics()}
 * overload — descriptor {@code ()Ljava/util/Map;} with no Duration
 * argument. Catches both direct
 * {@code INVOKEVIRTUAL}/{@code INVOKEINTERFACE} calls and indirect
 * {@code INVOKEDYNAMIC} method-reference captures (e.g.
 * {@code consumer::listTopics} bound to a
 * {@link java.util.function.Supplier} or
 * {@link java.util.concurrent.Callable}) via a dual walk over each
 * method's instructions.
 *
 * <h2>Why no-Duration listTopics is a correctness hazard</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer#listTopics()}
 * is documented as equivalent to {@code listTopics(
 * Duration.ofMillis(defaultApiTimeoutMs))} where
 * {@code default.api.timeout.ms} defaults to 60 s. The call returns
 * the full-cluster topic-metadata snapshot — every topic the consumer
 * is authorized to see, every partition, every replica assignment.
 * This is one of the most expensive Consumer-side metadata operations
 * by design:
 *
 * <ul>
 *   <li><b>Payload size grows linearly with cluster size.</b> A
 *       cluster with 50 k partitions across 5 k topics returns a
 *       Map with 5 k entries totaling tens of MB on the wire, and
 *       allocates a proportionally-sized object graph inside the
 *       Consumer (PartitionInfo[] per topic, Node[] per
 *       PartitionInfo). The cost is paid in network bytes, GC
 *       pressure, and request-handler CPU.</li>
 *   <li><b>Controller-side cost.</b> The receiving broker has to
 *       serialize its in-memory metadata cache; under controller
 *       election or large-cluster bookkeeping the call queues
 *       behind ongoing metadata changes.</li>
 *   <li><b>Authorization filtering.</b> When ACLs are enabled the
 *       broker must filter the metadata response to topics the
 *       caller is authorized to Describe; on large ACL tables this
 *       is non-trivial CPU per call.</li>
 * </ul>
 *
 * <p>And independent of payload cost, the call blocks for the full
 * 60 s budget under bootstrap unreachability, controller slowness,
 * or stale-metadata retargeting — same as every other no-Duration
 * Consumer overload.
 *
 * <p>Five concrete failure modes (carried verbatim into the violation
 * message so the engineer reading the lint report understands the
 * "why" without leaving the IDE):
 *
 * <ul>
 *   <li><b>autocompletion / topic-picker UIs starve their request
 *       thread on every keystroke.</b> Pattern: an internal admin UI
 *       has a "send a test message to topic" form where the topic
 *       dropdown is populated by calling
 *       {@code consumer.listTopics()} on every focus-or-keystroke.
 *       Under broker slowness the dropdown freezes for 60 s; the
 *       Jetty handler thread is pinned; concurrent admin actions
 *       queue behind it; the whole admin app appears wedged.</li>
 *   <li><b>scheduled metadata-cache refreshers consume the full
 *       60 s budget every tick during cluster slowness.</b> Pattern:
 *       a {@code @Scheduled} method runs
 *       {@code consumer.listTopics()} every 30 s to refresh an
 *       application-side topic catalog. Under broker slowness each
 *       tick takes 60 s; ticks overlap; the scheduler pool fills
 *       up; downstream {@code @Scheduled} jobs (metrics emission,
 *       health probes, position polls) are delayed or skipped.</li>
 *   <li><b>startup boot-checks that 'list all topics to confirm
 *       the cluster is reachable' hang readiness probes.</b>
 *       Pattern: a Spring Boot {@code HealthIndicator} or a
 *       Kubernetes readiness probe runs
 *       {@code consumer.listTopics()} as a generic
 *       cluster-reachability check. With a misconfigured
 *       {@code bootstrap.servers} the call hangs for the full
 *       60 s; the readiness probe times out; the pod cycles into
 *       CrashLoopBackoff; the actual error (typo in a single
 *       config string) is invisible in the pod's logs.</li>
 *   <li><b>scheduler-thread pinning when {@code listTopics} is
 *       dispatched via {@link java.util.concurrent.ScheduledExecutorService}
 *       method-reference capture.</b> A
 *       {@link java.util.concurrent.Callable}-typed indy capture
 *       {@code scheduler.submit((Callable) consumer::listTopics)}
 *       or {@code consumer::listTopics} bound to a
 *       {@link java.util.function.Supplier} pins the executor
 *       worker thread for 60 s on broker outage; backlogged tasks
 *       queue behind it; the executor's queue saturates and tasks
 *       are silently dropped.</li>
 *   <li><b>INVOKEDYNAMIC {@code consumer::listTopics} captures
 *       bypass naïve MethodInsnNode-only lint.</b> A
 *       {@link java.util.function.Supplier},
 *       {@link java.util.concurrent.Callable}, or custom
 *       {@code @FunctionalInterface TopicCatalogReader{
 *       Map<String, List<PartitionInfo>> readAll();}} parameter
 *       bound by a {@code consumer::listTopics} method reference
 *       compiles to {@code INVOKEDYNAMIC} whose bsm-args contain a
 *       {@code REF_invokeVirtual} (KafkaConsumer typed receiver) or
 *       {@code REF_invokeInterface} (Consumer interface typed
 *       receiver) handle pointing at
 *       {@code Consumer.listTopics()Ljava/util/Map;}. The user-class
 *       bytecode contains zero direct {@code INVOKEVIRTUAL} on the
 *       no-Duration overload — only the indy site. A rule that
 *       walks only {@code MethodInsnNode} misses every such
 *       site.</li>
 * </ul>
 *
 * <h2>Descriptor discrimination — one no-Duration overload, one
 * bounded overload</h2>
 *
 * <p>{@link org.apache.kafka.clients.consumer.Consumer} declares two
 * listTopics overloads with different descriptors:
 *
 * <ul>
 *   <li>Unbounded: {@code ()Ljava/util/Map;} — listTopics()</li>
 *   <li>Bounded: {@code (Ljava/time/Duration;)Ljava/util/Map;} —
 *       listTopics(Duration)</li>
 * </ul>
 *
 * <p>Predicate is exact-match against the no-Duration descriptor;
 * the bounded overload has a strictly different signature and is
 * never flagged.
 */
public final class ConsumerListTopicsNoTimeoutRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.CONSUMER_OWNERS;
    private static final String METHOD_NAME = "listTopics";
    private static final String NO_TIMEOUT_DESC = "()Ljava/util/Map;";

    private final Severity severity;

    public ConsumerListTopicsNoTimeoutRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_LIST_TOPICS_NO_TIMEOUT;
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
                RuleId.CONSUMER_LIST_TOPICS_NO_TIMEOUT, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Consumer.listTopics() (no Duration) is reached here — "
                        + "either as a direct "
                        + "INVOKEVIRTUAL/INVOKEINTERFACE call or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`consumer::listTopics` bound to a Supplier, "
                        + "Callable, or custom zero-arg SAM, common shapes "
                        + "when autocompletion UIs, scheduled metadata-"
                        + "cache refreshers, or readiness probes dispatch "
                        + "topic-catalog reads via "
                        + "ScheduledExecutorService). The no-Duration "
                        + "overload is documented as equivalent to "
                        + "listTopics(Duration.ofMillis("
                        + "defaultApiTimeoutMs)) where "
                        + "default.api.timeout.ms defaults to 60 s — and "
                        + "the call returns the full-cluster "
                        + "topic-metadata snapshot (every topic the "
                        + "consumer is authorized to see, every "
                        + "partition, every replica assignment): one of "
                        + "the most expensive Consumer-side metadata "
                        + "operations by design. Payload size grows "
                        + "linearly with cluster size (a 50 k-partition "
                        + "cluster returns a Map with thousands of "
                        + "entries totaling tens of MB on the wire, "
                        + "proportionally-sized object graph inside the "
                        + "Consumer — PartitionInfo[] per topic, Node[] "
                        + "per PartitionInfo — cost paid in network "
                        + "bytes, GC pressure, request-handler CPU); the "
                        + "receiving broker has to serialize its "
                        + "in-memory metadata cache (under controller "
                        + "election or large-cluster bookkeeping the "
                        + "call queues behind ongoing metadata "
                        + "changes); when ACLs are enabled the broker "
                        + "must filter the metadata response to topics "
                        + "the caller is authorized to Describe (on "
                        + "large ACL tables this is non-trivial CPU per "
                        + "call). Independent of payload cost, the call "
                        + "blocks for the full 60 s budget under "
                        + "bootstrap unreachability, controller "
                        + "slowness, or stale-metadata retargeting. "
                        + "Five concrete failure modes follow: (1) "
                        + "autocompletion / topic-picker UIs starve "
                        + "their request thread on every keystroke — "
                        + "an internal admin UI has a 'send a test "
                        + "message to topic' form where the topic "
                        + "dropdown is populated by calling "
                        + "`consumer.listTopics()` on every focus-or-"
                        + "keystroke; under broker slowness the dropdown "
                        + "freezes for 60 s, the Jetty handler thread is "
                        + "pinned, concurrent admin actions queue behind "
                        + "it, the whole admin app appears wedged; (2) "
                        + "scheduled metadata-cache refreshers consume "
                        + "the full 60 s budget every tick during "
                        + "cluster slowness — a `@Scheduled` method "
                        + "runs `consumer.listTopics()` every 30 s to "
                        + "refresh an application-side topic catalog; "
                        + "under broker slowness each tick takes 60 s, "
                        + "ticks overlap, the scheduler pool fills up, "
                        + "downstream `@Scheduled` jobs (metrics "
                        + "emission, health probes, position polls) are "
                        + "delayed or skipped; (3) startup boot-checks "
                        + "that 'list all topics to confirm the cluster "
                        + "is reachable' hang readiness probes — a "
                        + "Spring Boot HealthIndicator or a Kubernetes "
                        + "readiness probe runs "
                        + "`consumer.listTopics()` as a generic "
                        + "cluster-reachability check; with a "
                        + "misconfigured bootstrap.servers the call "
                        + "hangs for the full 60 s, the readiness probe "
                        + "times out, the pod cycles into "
                        + "CrashLoopBackoff, the actual error (typo in "
                        + "a single config string) is invisible in the "
                        + "pod's logs; (4) scheduler-thread pinning "
                        + "when `listTopics` is dispatched via "
                        + "ScheduledExecutorService method-reference "
                        + "capture — a Callable-typed indy capture "
                        + "`scheduler.submit((Callable) "
                        + "consumer::listTopics)` or "
                        + "`consumer::listTopics` bound to a Supplier "
                        + "pins the executor worker thread for 60 s on "
                        + "broker outage; backlogged tasks queue behind "
                        + "it; the executor's queue saturates and tasks "
                        + "are silently dropped; (5) INVOKEDYNAMIC "
                        + "`consumer::listTopics` captures bypass naïve "
                        + "MethodInsnNode-only lint — a Supplier, "
                        + "Callable, or custom `@FunctionalInterface "
                        + "TopicCatalogReader{Map<String, "
                        + "List<PartitionInfo>> readAll();}` parameter "
                        + "bound by a `consumer::listTopics` method "
                        + "reference compiles to INVOKEDYNAMIC whose "
                        + "bsm-args contain a REF_invokeVirtual or "
                        + "REF_invokeInterface handle pointing at "
                        + "Consumer.listTopics()Ljava/util/Map;; the "
                        + "user-class bytecode contains zero direct "
                        + "INVOKEVIRTUAL on the no-Duration overload, "
                        + "only the indy site. Migration: use the "
                        + "bounded overload `listTopics(Duration)` "
                        + "matched to the surrounding deadline (HTTP "
                        + "request deadline, scheduler-tick budget, "
                        + "readiness-probe timeout) and let the "
                        + "TimeoutException surface a "
                        + "cluster-slowness/bootstrap-unreachable error "
                        + "rather than an indefinite-feeling 60 s "
                        + "hang. The bounded overload has descriptor "
                        + "`(Ljava/time/Duration;)Ljava/util/Map;` and "
                        + "is never flagged by this rule.");
    }
}
