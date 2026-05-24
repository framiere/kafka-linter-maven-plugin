package io.conductor.kafkalinter.rules.clients;

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
 * Fires for every reach of {@code DeleteTopicsResult.values()} — whether the
 * call lands directly via {@code INVOKEVIRTUAL} or indirectly through an
 * {@code INVOKEDYNAMIC} method-reference capture (e.g. {@code result::values}
 * bound to a {@code Supplier<Map<String, KafkaFuture<Void>>>}).
 *
 * <h2>Why this accessor is dangerous, not merely cosmetic</h2>
 *
 * <p>{@code Admin.deleteTopics(...)} grew a second overload in Kafka 3.0
 * (KIP-516, topic-IDs) that accepts a {@link org.apache.kafka.common.TopicCollection}
 * — either {@code TopicCollection.ofTopicNames(names)} or
 * {@code TopicCollection.ofTopicIds(ids)}. Both overloads return the same
 * {@code DeleteTopicsResult}. The legacy {@code values()} accessor is
 * name-keyed: it returns {@code Map<String, KafkaFuture<Void>>}.
 *
 * <p>If the delete was made with {@code ofTopicIds(ids)}, the result is
 * internally id-keyed; calling the legacy {@code values()} on it throws
 * {@code UnsupportedOperationException} AT RUNTIME — not at compile time,
 * not at delete-request time, but at result-collection time, after the
 * actual broker delete is in flight or already completed. The exception
 * surfaces from a code path that LOOKS like a cheap Map-fetch but is
 * actually a key-space mismatch.
 *
 * <h2>Operational impact</h2>
 *
 * <p>The failure mode: a topic-cleanup job (CI tear-down, namespace
 * decommission, GDPR-erasure tooling) issues an id-keyed delete to refuse
 * silent matches against a recreated-same-name topic, then calls
 * {@code values()} on the result expecting a per-topic future to await.
 * The deletes already executed on the brokers — the topics are gone — but
 * the calling code throws {@code UnsupportedOperationException} BEFORE it
 * can call {@code .get()} on the futures, so it cannot distinguish
 * partial-success from total-success. Retry logic that re-runs the delete
 * on exception then issues a second delete against topics that no longer
 * exist, which (depending on idempotency handling upstream) can be a
 * benign no-op OR an UnknownTopicOrPartitionException re-thrown as a
 * cleanup failure that pages the on-call engineer.
 *
 * <p>The replacement accessors make the key-space explicit at the
 * call-site and refuse the wrong-keyed mapping at compile time (via type
 * mismatch) instead of at runtime (via UOE):
 *
 * <ul>
 *   <li>{@code topicNameValues()} → {@code Map<String, KafkaFuture<Void>>}
 *       — valid only when the delete was made by NAME.</li>
 *   <li>{@code topicIdValues()} → {@code Map<Uuid, KafkaFuture<Void>>}
 *       — valid only when the delete was made by ID.</li>
 * </ul>
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code result::values} compiles to an {@code INVOKEDYNAMIC} whose
 * bsm-args contain a {@code REF_invokeVirtual} handle pointing at
 * {@code DeleteTopicsResult.values()Ljava/util/Map;}. The user-class
 * bytecode contains zero {@code INVOKE*} targeting {@code values}; the
 * call lives only in the {@code LambdaMetafactory}-synthesized bridge,
 * so a name-only walk on {@code MethodInsnNode} would miss it. The
 * rule's {@code INVOKEDYNAMIC} walk inspects the bsmArg handle's
 * owner+name to catch this case.
 *
 * <h2>Why we match by name alone</h2>
 *
 * <p>{@code DeleteTopicsResult} is a final class with no overloads of
 * {@code values()} and no base type that exposes it. The replacement
 * accessors are named differently ({@code topicNameValues},
 * {@code topicIdValues}). Matching on the (owner, name) pair is
 * unambiguous — no descriptor discrimination is needed.
 */
public final class AdminDeleteTopicsResultValuesDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.ADMIN_DELETE_TOPICS_RESULT);
    private static final String VALUES = "values";

    private final Severity severity;

    public AdminDeleteTopicsResultValuesDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_DELETE_TOPICS_RESULT_VALUES_DEPRECATED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && VALUES.equals(mi.name)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, VALUES, null);
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
                RuleId.ADMIN_DELETE_TOPICS_RESULT_VALUES_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "DeleteTopicsResult.values() is reached here — either as a direct call or "
                        + "as a method-reference capture (e.g. `result::values` assigned to a "
                        + "Supplier<Map<String, KafkaFuture<Void>>> whose deferred invocation "
                        + "has the same legacy semantics). This accessor is deprecated since "
                        + "Kafka 3.0 (KIP-516) because it predates topic-IDs: when the "
                        + "underlying Admin.deleteTopics call was made with "
                        + "TopicCollection.ofTopicIds(...), the result is internally id-keyed "
                        + "and calling the legacy name-keyed values() throws "
                        + "UnsupportedOperationException AT RUNTIME, after the brokers have "
                        + "already executed the deletes. A topic-cleanup job that issues an "
                        + "id-keyed delete to refuse same-name matches then cannot collect "
                        + "the per-topic Futures it needs to distinguish partial-success "
                        + "from total-success; naive retry logic re-runs the delete against "
                        + "topics that no longer exist, surfacing as "
                        + "UnknownTopicOrPartitionException on the second pass. Migrate to "
                        + "the key-space-explicit accessors: topicNameValues() if you "
                        + "deleted by topic NAME, or topicIdValues() (returns Map<Uuid, "
                        + "KafkaFuture<Void>>) if you deleted by topic ID. The new accessors "
                        + "make the key-space lexically obvious at the call site and refuse "
                        + "the wrong-keyed mapping at compile time via type mismatch instead "
                        + "of at runtime via UnsupportedOperationException.");
    }
}
