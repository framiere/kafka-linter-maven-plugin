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
 * Fires for every reach of the legacy {@code DescribeLogDirsResult} accessors
 * {@code values()} and {@code all()} — whether the call lands directly via
 * {@code INVOKEVIRTUAL} or indirectly through an {@code INVOKEDYNAMIC}
 * method-reference capture (e.g. {@code result::values} bound to a
 * {@code Supplier<Map<Integer, KafkaFuture<Map<String, LogDirInfo>>>>}).
 *
 * <h2>Why this overload is dangerous, not merely cosmetic</h2>
 *
 * <p>{@code Admin.describeLogDirs(brokers)} returns a
 * {@code DescribeLogDirsResult} that, historically, exposed broker
 * log-dir state through {@code values()} and {@code all()}. The return
 * type of those legacy accessors is keyed by broker ID and ultimately
 * wraps {@code org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo}
 * — an INTERNAL wire-protocol type living in the
 * {@code org.apache.kafka.common.requests} package, which the Kafka
 * project explicitly does NOT treat as public API. Two consequences
 * follow:
 *
 * <ul>
 *   <li><b>Binary compatibility breaks between minor Kafka client
 *       releases.</b> The field layout of {@code LogDirInfo} (and its
 *       nested {@code ReplicaInfo}) is changed in-place when the
 *       wire-format grows: KIP-630 added {@code totalBytes} /
 *       {@code usableBytes} for the on-disk log-dir capacity; KIP-859
 *       added {@code futureReplica} semantics around reassignments.
 *       Tooling that compiles against {@code LogDirInfo} from one
 *       Kafka client version may fail to link against another, even
 *       inside the {@code clients} jar.</li>
 *   <li><b>The new public fields are simply absent from the legacy
 *       accessor.</b> Capacity-monitoring jobs (free-space alerting,
 *       rolling-restart precondition checks) built on
 *       {@code values()} / {@code all()} cannot see
 *       {@code totalBytes} / {@code usableBytes} — they were added on
 *       the new {@link org.apache.kafka.clients.admin.LogDirDescription}
 *       type that the supported {@code descriptions()} /
 *       {@code allDescriptions()} accessors return. Engineers reading
 *       the legacy field-set conclude the data isn't there, when in
 *       fact it is there but only exposed through the new path.</li>
 * </ul>
 *
 * <p>{@code descriptions()} returns
 * {@code Map<Integer, Map<String, LogDirDescription>>} and
 * {@code allDescriptions()} returns
 * {@code KafkaFuture<Map<Integer, Map<String, LogDirDescription>>>}.
 * Both expose the new public-API type
 * {@link org.apache.kafka.clients.admin.LogDirDescription} with stable
 * getter contracts ({@code totalBytes()}, {@code usableBytes()},
 * {@code replicaInfos()}).
 *
 * <h2>Operational impact</h2>
 *
 * <p>The failure mode: a disk-pressure healthcheck or rolling-restart
 * preflight wires {@code admin.describeLogDirs(...).all()} into a
 * monitoring callback that aggregates per-broker free space. On Kafka
 * 3.0+ the new {@code totalBytes} / {@code usableBytes} fields are
 * available on every broker — but the legacy accessor's
 * {@code LogDirInfo} does not surface them, so the monitor concludes
 * &laquo;capacity unknown&raquo; and either alerts noisily or, worse,
 * silently degrades to a less-safe heuristic (counting active
 * partitions instead of measuring bytes).
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code result::values} and {@code result::all} compile to
 * {@code INVOKEDYNAMIC} whose bsm-args contain a
 * {@code REF_invokeVirtual} handle pointing at the legacy accessor.
 * The user-class bytecode contains zero {@code INVOKE*} targeting
 * the method name; the call lives only in the
 * {@code LambdaMetafactory}-synthesized bridge. The rule's
 * {@code INVOKEDYNAMIC} walk inspects the bsmArg handle's owner+name
 * to catch this case.
 *
 * <h2>Why we match by name alone</h2>
 *
 * <p>{@code DescribeLogDirsResult} is a final class with no overloads
 * of {@code values()} or {@code all()} and no base type that exposes
 * those names. The replacement accessors are named differently
 * ({@code descriptions} / {@code allDescriptions}). Matching on the
 * (owner, name) pair is therefore unambiguous — no descriptor
 * discrimination is needed.
 */
public final class AdminDescribeLogDirsResultLegacyDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.ADMIN_DESCRIBE_LOG_DIRS_RESULT);
    private static final Set<String> LEGACY_NAMES = Set.of("values", "all");

    private final Severity severity;

    public AdminDescribeLogDirsResultLegacyDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_DESCRIBE_LOG_DIRS_RESULT_LEGACY_DEPRECATED;
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
                RuleId.ADMIN_DESCRIBE_LOG_DIRS_RESULT_LEGACY_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "DescribeLogDirsResult." + name + "() is reached here — either as a direct "
                        + "call or as a method-reference capture (e.g. `result::" + name + "` "
                        + "assigned to a Supplier whose deferred invocation has the same "
                        + "legacy semantics). This accessor is deprecated since Kafka 3.0 "
                        + "(KIP-743) because its return type bottoms out in the INTERNAL "
                        + "org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo, "
                        + "a wire-protocol type living in the non-public `common.requests` "
                        + "package. The field layout of LogDirInfo (and its nested "
                        + "ReplicaInfo) is changed in-place between minor Kafka releases when "
                        + "the wire-format grows — KIP-630 added totalBytes/usableBytes for "
                        + "on-disk capacity, KIP-859 added futureReplica reassignment fields. "
                        + "Tooling that compiles against LogDirInfo from one client version "
                        + "may fail to link against another, even inside the same `clients` "
                        + "jar. Worse, the new public-API fields are simply ABSENT from "
                        + "LogDirInfo: capacity-monitoring jobs and rolling-restart "
                        + "preflights built on values()/all() cannot see totalBytes / "
                        + "usableBytes and silently degrade to less-safe heuristics. "
                        + "Migrate to descriptions() / allDescriptions(), which return "
                        + "Map<Integer, Map<String, LogDirDescription>> and "
                        + "KafkaFuture<Map<Integer, Map<String, LogDirDescription>>> "
                        + "respectively. LogDirDescription is the public-API type with "
                        + "stable getter contracts (totalBytes(), usableBytes(), "
                        + "replicaInfos()) and survives Kafka version upgrades.");
    }
}
