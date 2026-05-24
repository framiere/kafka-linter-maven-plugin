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
 * Fires for every reach of the legacy DescribeTopicsResult accessors
 * {@code values()} and {@code all()} — whether the call lands directly via
 * {@code INVOKEVIRTUAL} or indirectly through an {@code INVOKEDYNAMIC}
 * method-reference capture (e.g. {@code result::values} bound to a
 * {@code Supplier<Map<String, KafkaFuture<TopicDescription>>>} or
 * {@code result::all} bound to a {@code Supplier<KafkaFuture<Map<String, TopicDescription>>>}).
 *
 * <h2>Why this overload is dangerous, not merely cosmetic</h2>
 *
 * <p>{@code Admin.describeTopics(...)} grew a second overload in Kafka 3.1
 * (KIP-516, topic-IDs) that accepts a {@link org.apache.kafka.common.TopicCollection}
 * — either {@code TopicCollection.ofTopicNames(names)} or
 * {@code TopicCollection.ofTopicIds(ids)}. The Result type returned by both
 * overloads is the same {@code DescribeTopicsResult}. To keep binary-compat
 * with code that pre-dated topic-IDs, the legacy accessors
 * {@code values()} / {@code all()} are still present — BUT they return a map
 * keyed by topic NAME. If your describe call was made with
 * {@code ofTopicIds(ids)}, the result is in fact an ID-keyed map internally,
 * and the legacy name-keyed accessors silently return an EMPTY map: the
 * keys you would have asked for (names) are not present, and there is no
 * exception, no log, no missing-key signal.
 *
 * <p>The new accessors make the key-space explicit and refuse to silently
 * mis-key:
 *
 * <ul>
 *   <li>{@code topicNameValues()} / {@code allTopicNames()} — name-keyed,
 *       use when your describe was {@code TopicCollection.ofTopicNames(...)}
 *       or the legacy {@code Collection<String>} overload.</li>
 *   <li>{@code topicIdValues()} / {@code allTopicIds()} — id-keyed, use
 *       when your describe was {@code TopicCollection.ofTopicIds(...)}.</li>
 * </ul>
 *
 * <h2>Operational impact</h2>
 *
 * <p>The failure surfaces as a topic-management script that runs to
 * completion with no errors, no exceptions, no warnings — and silently
 * returns &laquo;no topics found&raquo; for the ID-keyed query path.
 * Healthchecks, monitoring rules, and cleanup jobs built on the empty
 * result then make WRONG decisions: an idempotent reconciler concludes
 * the topic doesn't exist and re-creates it; a quota-cleanup job
 * concludes there are no consumers to drain; a partition-rebalance
 * script silently skips its work. The shape that looks correct in
 * code-review (a freshly-fetched result, a loop over its entries) is
 * the same shape that produces these silent zeros.
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code result::values} compiles to an {@code INVOKEDYNAMIC} whose
 * bsm-args contain a {@code REF_invokeVirtual} handle pointing at
 * {@code DescribeTopicsResult.values()Ljava/util/Map;}. Likewise
 * {@code result::all} produces a handle at
 * {@code DescribeTopicsResult.all()Lorg/apache/kafka/common/KafkaFuture;}.
 * The user-class bytecode contains zero {@code INVOKE*} targeting the
 * method name; the resolved handle lives only in the bootstrap-method
 * arguments. The rule's {@code INVOKEDYNAMIC} walk inspects the bsmArg
 * handle's owner+name to catch this case.
 *
 * <h2>Why we match by name alone</h2>
 *
 * <p>{@code DescribeTopicsResult} is a final class with NO overloads of
 * {@code values()} or {@code all()} and NO base type that exposes those
 * names. The replacement accessors are named differently
 * ({@code topicNameValues}, {@code allTopicNames}, {@code topicIdValues},
 * {@code allTopicIds}). Matching on the (owner, name) pair is therefore
 * unambiguous — no descriptor discrimination is needed.
 */
public final class AdminDescribeTopicsResultLegacyDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.ADMIN_DESCRIBE_TOPICS_RESULT);
    private static final Set<String> LEGACY_NAMES = Set.of("values", "all");

    private final Severity severity;

    public AdminDescribeTopicsResultLegacyDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_DESCRIBE_TOPICS_RESULT_LEGACY_DEPRECATED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && LEGACY_NAMES.contains(mi.name)) {
                    out.add(violation(ctx, mn, insn, mi.name));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    for (String name : LEGACY_NAMES) {
                        Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, name, null);
                        if (h != null) {
                            out.add(violation(ctx, mn, insn, name));
                            break;
                        }
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn, String name) {
        return new Violation(
                RuleId.ADMIN_DESCRIBE_TOPICS_RESULT_LEGACY_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "DescribeTopicsResult." + name + "() is reached here — either as a direct "
                        + "call or as a method-reference capture (e.g. `result::" + name + "` "
                        + "assigned to a Supplier whose deferred invocation has the same "
                        + "legacy semantics). This accessor is deprecated since Kafka 3.1 "
                        + "(KIP-516) because it predates topic IDs and silently returns an "
                        + "EMPTY map when the underlying Admin.describeTopics call was made "
                        + "with TopicCollection.ofTopicIds(...): the result is internally "
                        + "id-keyed, the legacy accessor is name-keyed, and the missing-key "
                        + "lookup produces no exception, no log, no signal — just an empty "
                        + "Map / a KafkaFuture that resolves to an empty Map. Topic-management "
                        + "scripts, healthchecks, idempotent reconcilers and cleanup jobs "
                        + "built on the empty result then make WRONG decisions silently. "
                        + "Migrate to the key-space-explicit accessors: "
                        + "topicNameValues() / allTopicNames() if you queried by topic NAME, "
                        + "or topicIdValues() / allTopicIds() if you queried by topic ID. The "
                        + "new accessors are identical in shape — same Map type, same "
                        + "KafkaFuture wrapping — they just refuse to silently return empty "
                        + "results from the wrong-keyed query.");
    }
}
