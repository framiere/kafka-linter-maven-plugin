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
 * Fires for every reach of the deprecated zero-arg instance method
 * {@link org.apache.kafka.streams.KafkaStreams#localThreadsMetadata()}
 * — whether the call lands directly via {@code INVOKEVIRTUAL} or
 * indirectly through an {@code INVOKEDYNAMIC} method-reference capture
 * (e.g. {@code streams::localThreadsMetadata} bound to a
 * {@code Supplier<Set<?>>} or {@code Function<KafkaStreams, Set<?>>}
 * factory used by metrics emitters, admin dashboards, health-check
 * endpoints, or rebalance-monitoring routines that periodically pull
 * thread state for export to Prometheus / a tracing backend / an
 * internal control plane).
 *
 * <h2>Why this method is deprecated, not just a name change</h2>
 *
 * <p>{@code KafkaStreams.localThreadsMetadata()} returns
 * {@code Set<org.apache.kafka.streams.processor.ThreadMetadata>} —
 * the <em>processor-package</em> shape of {@code ThreadMetadata}.
 * That class lived under {@code org.apache.kafka.streams.processor}
 * for historical reasons (it predates the public Streams metadata
 * API surface) and exposes a fixed legacy field set:
 * {@code threadName}, {@code threadState}, {@code activeTasks},
 * {@code standbyTasks}, and {@code consumerClientId}. Once KIP-740
 * landed, the metadata structure was migrated to a new top-level
 * type {@code org.apache.kafka.streams.ThreadMetadata} which adds
 * {@code adminClientId}, {@code restoreConsumerClientId}, the
 * producers' {@code producerClientIds()}, the per-thread
 * {@code threadProducerClientId()}, and reshapes
 * {@code TaskMetadata} to publish topic-partition timestamps —
 * all fields the operations community asked for to do meaningful
 * rebalance debugging and per-task latency reporting.
 *
 * <p>Concretely, continued use of the legacy method has four
 * downstream effects:
 *
 * <ul>
 *   <li><b>New metadata fields are simply not visible.</b> The
 *       deprecated {@code processor.ThreadMetadata} has no
 *       getters for {@code adminClientId} /
 *       {@code restoreConsumerClientId} /
 *       {@code producerClientIds()} / partition timestamps. Health
 *       dashboards built on the legacy method silently miss the
 *       data that KIP-740 made available — there is no error, only
 *       missing detail.</li>
 *   <li><b>Cross-version interop breaks at the type boundary.</b>
 *       Code that imports
 *       {@code org.apache.kafka.streams.processor.ThreadMetadata}
 *       cannot pass values to APIs that take the modern
 *       {@code org.apache.kafka.streams.ThreadMetadata} — the two
 *       are unrelated classes despite the identical simple name.
 *       Mixing the two in the same module requires per-call
 *       conversion stubs.</li>
 *   <li><b>The legacy method is documented as removable.</b>
 *       KIP-740 marks it for removal in a future Kafka release;
 *       upgrading kafka-streams across that boundary breaks the
 *       compile for anything still on the legacy method.</li>
 *   <li><b>{@code INVOKEDYNAMIC streams::localThreadsMetadata}
 *       captures silently bind to the deprecated method.</b> A
 *       metrics-reporter scaffold using a
 *       {@code Function<KafkaStreams, Set<?>>} or
 *       {@code Supplier<Set<?>>} resolves the method-ref by arity
 *       and return-erasure to the legacy overload — the user-class
 *       bytecode contains zero direct {@code INVOKEVIRTUAL} on the
 *       legacy method and a name-only MethodInsnNode walk misses
 *       the call.</li>
 * </ul>
 *
 * <h2>The replacement API</h2>
 *
 * <p>{@link org.apache.kafka.streams.KafkaStreams#metadataForLocalThreads()}
 * returns {@code Set<org.apache.kafka.streams.ThreadMetadata>}
 * — the modern top-level type with the full KIP-740 field set.
 * Code migrating off the legacy method needs to update both the
 * call site and the imported {@code ThreadMetadata} type (the
 * compiler will flag the type mismatch immediately, so the
 * migration is largely mechanical).
 *
 * <h2>Descriptor discrimination</h2>
 *
 * <p>The two methods have identical zero-arg shape but distinct
 * names ({@code localThreadsMetadata} vs {@code metadataForLocalThreads}),
 * so the name filter alone discriminates. The descriptor pin
 * {@code ()Ljava/util/Set;} is kept for consistency with the rest
 * of the dedicated rules and to insulate the rule from any future
 * overload of {@code localThreadsMetadata}.
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code streams::localThreadsMetadata} bound to a zero-arg
 * functional interface (e.g. {@code Supplier<Set<?>>}) compiles
 * to {@code INVOKEDYNAMIC} whose bsm-args contain a
 * {@code REF_invokeVirtual} handle pointing at the resolved
 * legacy method. The rule's bsm-arg walk catches this case by
 * checking the handle's {@code (owner, name, desc)} triple against
 * the same filter used for direct calls.
 */
public final class StreamsLocalThreadsMetadataDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.KAFKA_STREAMS);
    private static final String METHOD_NAME = "localThreadsMetadata";
    private static final String LEGACY_DESC = "()Ljava/util/Set;";

    private final Severity severity;

    public StreamsLocalThreadsMetadataDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_LOCAL_THREADS_METADATA_DEPRECATED;
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
                RuleId.STREAMS_LOCAL_THREADS_METADATA_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "KafkaStreams.localThreadsMetadata() is reached here — either as a "
                        + "direct call or as an INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `streams::localThreadsMetadata` bound to a "
                        + "Supplier<Set<?>> or Function<KafkaStreams, Set<?>> used by a "
                        + "metrics emitter, admin dashboard, health-check endpoint, or "
                        + "rebalance-monitoring routine that periodically pulls thread "
                        + "state for export). This method is deprecated since Kafka "
                        + "Streams 3.0 (KIP-740) because it returns "
                        + "Set<org.apache.kafka.streams.processor.ThreadMetadata> — the "
                        + "processor-package legacy shape with a fixed field set "
                        + "(threadName, threadState, activeTasks, standbyTasks, "
                        + "consumerClientId). KIP-740 migrated the metadata API to a new "
                        + "top-level type org.apache.kafka.streams.ThreadMetadata that "
                        + "adds adminClientId, restoreConsumerClientId, "
                        + "producerClientIds(), threadProducerClientId(), and reshapes "
                        + "TaskMetadata to publish topic-partition timestamps — the "
                        + "fields ops teams need for meaningful rebalance debugging and "
                        + "per-task latency reporting. Four downstream consequences for "
                        + "continued use: (1) new metadata fields are simply not visible "
                        + "— the legacy class has no getters for them and dashboards "
                        + "silently miss the data with no error; (2) cross-version "
                        + "interop breaks at the type boundary — code holding the "
                        + "deprecated `processor.ThreadMetadata` cannot interoperate "
                        + "with APIs that take the modern top-level `ThreadMetadata` "
                        + "despite the identical simple name, forcing per-call conversion "
                        + "stubs; (3) KIP-740 documents the legacy method as removable in "
                        + "a future Kafka release — upgrading across that boundary "
                        + "breaks the compile; (4) INVOKEDYNAMIC "
                        + "`streams::localThreadsMetadata` captures silently bind to the "
                        + "deprecated method whenever the SAM has matching arity and "
                        + "return erasure — the user-class bytecode contains zero direct "
                        + "INVOKEVIRTUAL on the legacy method and a name-only walk "
                        + "misses it. Migrate to `streams.metadataForLocalThreads()` "
                        + "(returns Set<org.apache.kafka.streams.ThreadMetadata>) and "
                        + "update the imported ThreadMetadata type at every consumer of "
                        + "the result — the compiler will flag the type mismatch "
                        + "immediately, so the migration is largely mechanical.");
    }
}
