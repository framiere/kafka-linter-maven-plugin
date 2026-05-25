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
 * Fires for every reach of the deprecated
 * {@link org.apache.kafka.streams.KafkaStreams#setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler)}
 * overload — whether the call lands directly via {@code INVOKEVIRTUAL}
 * or indirectly through an {@code INVOKEDYNAMIC} method-reference
 * capture (e.g. {@code streams::setUncaughtExceptionHandler} bound to
 * a SAM whose erased signature matches
 * {@code (KafkaStreams, Thread.UncaughtExceptionHandler) -> void}).
 *
 * <h2>Why this overload is deprecated, not just renamed</h2>
 *
 * <p>{@code KafkaStreams.setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler)}
 * is the pre-KIP-671 way to react to a fatal exception thrown by a
 * stream-thread. The handler signature is
 * {@code (Thread, Throwable) -> void}: it can observe that the thread
 * is dying, log it, page someone, write a metric. It cannot do
 * anything else. KIP-671 (Kafka Streams 2.8, April 2021) introduced
 * {@code setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler)},
 * whose handler returns a {@code StreamThreadExceptionResponse} enum
 * value — {@code REPLACE_THREAD}, {@code SHUTDOWN_CLIENT}, or
 * {@code SHUTDOWN_APPLICATION} — telling the Kafka Streams
 * supervisor what to do next. The deprecation tracks five concrete
 * operability failure modes the legacy overload makes silent or
 * unrecoverable:
 *
 * <ul>
 *   <li><b>Silent parallelism loss.</b> When a stream-thread dies and
 *       the legacy handler returns void, the dead thread's task
 *       assignments are not reassigned to a replacement thread within
 *       the same {@code KafkaStreams} instance. The instance keeps
 *       running with {@code N-1} threads; the dead thread's tasks
 *       move to peers via the consumer-group rebalance. If the
 *       instance was already running at capacity, the surviving
 *       threads now lag — throughput silently drops, end-to-end
 *       latency climbs, and rocksdb restore-from-changelog runs on
 *       the peer that inherits the task. None of this surfaces as an
 *       application error; the only signal is monitoring on
 *       {@code KafkaStreams.localThreadsMetadata()} or per-thread
 *       JMX state. {@code REPLACE_THREAD} from the new handler
 *       requests that the supervisor spawn a fresh stream-thread on
 *       this instance, restoring parallelism without rebalancing the
 *       task to a peer.</li>
 *   <li><b>No graceful application-wide shutdown for fatal errors.</b>
 *       Some exceptions are application-fatal — a permanent
 *       authorization failure on the input topic, a Schema Registry
 *       4xx for a contract violation, an unrecoverable serialization
 *       version mismatch. The right response is to shut down every
 *       instance of this application cluster-wide so that a human
 *       investigates before more records are misprocessed. The
 *       legacy handler cannot request this; if the instance returns
 *       from the handler without calling {@code KafkaStreams.close()}
 *       explicitly, the partial-failure mode is: this instance loses
 *       a thread, peers pick up the work, and the same fatal
 *       exception hits a peer next, and so on around the ring until
 *       all instances are degraded. {@code SHUTDOWN_APPLICATION}
 *       from the new handler propagates a sentinel through the
 *       consumer-group rebalance protocol that asks every other
 *       instance to close as well — cluster-wide fail-fast on
 *       contract violations.</li>
 *   <li><b>{@code SHUTDOWN_CLIENT} versus {@code SHUTDOWN_APPLICATION}
 *       is a decision the legacy API cannot express.</b> The right
 *       response to a host-local exception (disk full, OOM-killer
 *       imminent, RocksDB native-library load failure) is to shut down
 *       this client only and let peers take over. The right response
 *       to a cluster-wide exception is the application-wide shutdown
 *       above. The legacy handler returns void and the calling thread
 *       is already dying — by the time the application notices and
 *       calls {@code KafkaStreams.close()}, the operator has already
 *       lost time. The new handler returns a triple enum that lets
 *       the topology author choose the right scope per exception
 *       class.</li>
 *   <li><b>The legacy handler runs on the dying thread.</b>
 *       {@code Thread.UncaughtExceptionHandler.uncaughtException(Thread,
 *       Throwable)} is invoked from inside the dying thread's
 *       {@code run()}, before the JVM removes it from the live-thread
 *       set. Any blocking operation in the handler (a slow PagerDuty
 *       HTTP call, a metric flush that retries) keeps the dying
 *       thread holding its task assignments longer than necessary,
 *       blocking the rebalance and the failover. The new
 *       {@code StreamsUncaughtExceptionHandler} is invoked from the
 *       Kafka Streams supervisor, not from the dying thread, and the
 *       supervisor can act on the returned enum immediately while
 *       the dying thread is already gone.</li>
 *   <li><b>INVOKEDYNAMIC {@code streams::setUncaughtExceptionHandler}
 *       captures silently bind to the legacy overload.</b> Both
 *       overloads share the same method name. A capture site whose
 *       SAM's erased argument type is
 *       {@code (Object, Thread.UncaughtExceptionHandler) -> void}
 *       resolves to the legacy overload; the user-class bytecode
 *       contains zero direct {@code INVOKEVIRTUAL} on the legacy
 *       method — only the {@code INVOKEDYNAMIC} +
 *       {@code LambdaMetafactory} bridge. A name-only MethodInsnNode
 *       walk misses this entirely.</li>
 * </ul>
 *
 * <h2>The replacement API</h2>
 *
 * <p>The new overload takes a
 * {@code org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler}:
 *
 * <pre>{@code
 *   streams.setUncaughtExceptionHandler(throwable -> {
 *       if (isApplicationFatal(throwable)) {
 *           return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
 *       }
 *       if (isHostLocal(throwable)) {
 *           return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
 *       }
 *       return StreamThreadExceptionResponse.REPLACE_THREAD;
 *   });
 * }</pre>
 *
 * <p>Three concrete operability wins over the legacy handler:
 * automatic thread-replacement preserves per-instance parallelism;
 * application-wide shutdown is reachable on contract-violation
 * exceptions; per-exception-class shutdown scope is encoded in
 * source rather than in a void handler's side effects.
 *
 * <h2>Descriptor discrimination — same name, two distinct
 * descriptors</h2>
 *
 * <p>Both overloads share the method name
 * {@code setUncaughtExceptionHandler}. The legacy overload takes
 * {@code Thread$UncaughtExceptionHandler}; the new overload takes
 * {@code org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler}.
 * The rule pins the descriptor:
 *
 * <ul>
 *   <li>Legacy (fires):
 *       {@code (Ljava/lang/Thread$UncaughtExceptionHandler;)V}</li>
 *   <li>New (does not fire):
 *       {@code (Lorg/apache/kafka/streams/errors/StreamsUncaughtExceptionHandler;)V}</li>
 * </ul>
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code streams::setUncaughtExceptionHandler} bound to a SAM
 * whose erased argument type is
 * {@code Thread.UncaughtExceptionHandler} compiles to
 * {@code INVOKEDYNAMIC} whose bsm-args contain a
 * {@code REF_invokeVirtual} handle pointing at the legacy overload.
 * The rule's bsm-arg walk catches this case by checking the handle's
 * {@code (owner, name, desc)} triple against the same legacy
 * descriptor used for direct calls.
 */
public final class StreamsSetUncaughtExceptionHandlerLegacyDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KAFKA_STREAMS);
    private static final String METHOD_NAME = "setUncaughtExceptionHandler";
    private static final String LEGACY_DESC = "(Ljava/lang/Thread$UncaughtExceptionHandler;)V";

    private final Severity severity;

    public StreamsSetUncaughtExceptionHandlerLegacyDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_SET_UNCAUGHT_EXCEPTION_HANDLER_LEGACY_DEPRECATED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && LEGACY_DESC.equals(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, LEGACY_DESC);
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
                RuleId.STREAMS_SET_UNCAUGHT_EXCEPTION_HANDLER_LEGACY_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KafkaStreams.setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler) "
                        + "is reached here — either as a direct call or as an "
                        + "INVOKEDYNAMIC method-reference capture (e.g. "
                        + "`streams::setUncaughtExceptionHandler` bound to a SAM whose "
                        + "erased argument type is Thread.UncaughtExceptionHandler). "
                        + "This overload is deprecated since Kafka Streams 2.8 "
                        + "(KIP-671, April 2021) because Thread.UncaughtExceptionHandler "
                        + "returns void and can only observe the dying thread — it "
                        + "cannot tell the Kafka Streams supervisor what to do next. "
                        + "Five concrete operability failure modes follow: (1) silent "
                        + "parallelism loss — when a stream-thread dies under the "
                        + "legacy handler, the instance keeps running with N-1 threads "
                        + "and the dead thread's tasks move to peers via consumer-"
                        + "group rebalance, silently dropping per-instance throughput "
                        + "and increasing rocksdb restore load on peers; (2) no "
                        + "graceful application-wide shutdown — exceptions that should "
                        + "fail the whole cluster (auth permanent, contract violation, "
                        + "schema mismatch) cascade thread-by-thread around the ring "
                        + "with the same exception until every instance is degraded; "
                        + "(3) no SHUTDOWN_CLIENT vs SHUTDOWN_APPLICATION distinction "
                        + "— the right response to host-local failure (disk, OOM, "
                        + "rocksdb native-load) differs from the right response to "
                        + "cluster-wide failure, but the legacy handler returns void "
                        + "and cannot encode the choice; (4) the legacy handler runs "
                        + "on the dying thread itself, so any blocking action (slow "
                        + "PagerDuty, retried metric flush) keeps the thread holding "
                        + "its task assignments longer than necessary and blocks the "
                        + "rebalance; (5) INVOKEDYNAMIC "
                        + "`streams::setUncaughtExceptionHandler` captures silently "
                        + "bind to the legacy overload — the user-class bytecode "
                        + "contains zero direct INVOKEVIRTUAL on the legacy method "
                        + "and a name-only walk misses it. Migrate to "
                        + "`streams.setUncaughtExceptionHandler(throwable -> "
                        + "isApplicationFatal(throwable) ? SHUTDOWN_APPLICATION : "
                        + "isHostLocal(throwable) ? SHUTDOWN_CLIENT : REPLACE_THREAD)`. "
                        + "REPLACE_THREAD spawns a fresh stream-thread on this "
                        + "instance and preserves parallelism without a rebalance; "
                        + "SHUTDOWN_APPLICATION propagates a sentinel through the "
                        + "consumer-group rebalance protocol that asks every other "
                        + "instance of the application to close as well — cluster-"
                        + "wide fail-fast on contract violations; SHUTDOWN_CLIENT "
                        + "closes only this client and lets peers take over the "
                        + "tasks.");
    }
}
