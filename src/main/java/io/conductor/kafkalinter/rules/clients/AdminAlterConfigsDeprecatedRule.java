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

/**
 * Fires for every {@code Admin.alterConfigs(...)} / {@code AdminClient.alterConfigs(...)}
 * call, regardless of overload. The whole method family is deprecated since
 * Kafka 2.3 (KIP-339) and should be replaced with
 * {@code incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>>)}.
 *
 * <h2>Why the rule matches by NAME and not by descriptor</h2>
 *
 * <p>Two deprecated overloads exist on the {@code Admin} / {@code AdminClient}
 * type:
 * <ul>
 *   <li>{@code alterConfigs(Map<ConfigResource, Config>)} — descriptor
 *       {@code (Ljava/util/Map;)Lorg/apache/kafka/clients/admin/AlterConfigsResult;}</li>
 *   <li>{@code alterConfigs(Map<ConfigResource, Config>, AlterConfigsOptions)} — descriptor
 *       {@code (Ljava/util/Map;Lorg/apache/kafka/clients/admin/AlterConfigsOptions;)Lorg/apache/kafka/clients/admin/AlterConfigsResult;}</li>
 * </ul>
 * Both carry the {@code @Deprecated} annotation and both have identical
 * full-replacement semantics. The non-deprecated replacement is
 * {@code incrementalAlterConfigs(...)} — a SEPARATE method whose name is
 * NOT prefix-equal to {@code alterConfigs}. Matching by exact method name
 * {@code alterConfigs} therefore catches both deprecated overloads and never
 * mis-fires on the replacement.
 *
 * <h2>Why the replacement matters operationally</h2>
 *
 * <p>{@code alterConfigs(Map<ConfigResource, Config>)} treats the supplied
 * {@link org.apache.kafka.clients.admin.Config} as the COMPLETE desired state
 * for that resource. Any per-resource config key NOT included in the supplied
 * Config gets reset to the broker's default value when the request is applied.
 * A script that calls
 * {@code admin.alterConfigs(Map.of(topic, new Config(List.of(new ConfigEntry("retention.ms", "604800000")))))}
 * to raise retention on a topic ALSO wipes that topic's
 * {@code cleanup.policy}, {@code compression.type}, {@code min.insync.replicas},
 * and every other previously-set per-topic config back to broker defaults.
 * For a compacted topic this is data-destructive: {@code cleanup.policy} flips
 * from {@code compact} to the broker default {@code delete}, and the broker
 * starts segment-deleting compacted records on the next log-cleaner run.
 *
 * <p>The KIP-339 replacement
 * {@code incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>>)}
 * accepts a per-key operation (SET / DELETE / APPEND / SUBTRACT) and applies
 * each operation surgically — keys not mentioned in the request are left
 * untouched. The equivalent safe call:
 * {@code admin.incrementalAlterConfigs(Map.of(topic, List.of(new AlterConfigOp(new ConfigEntry("retention.ms", "604800000"), OpType.SET))))}.
 *
 * <h2>Method-reference capture</h2>
 *
 * <p>{@code Admin::alterConfigs} compiles to an {@code INVOKEDYNAMIC} whose
 * bootstrap-method args include a {@code REF_invokeInterface} (or
 * {@code REF_invokeVirtual} on the {@code AdminClient} abstract class) handle
 * to {@code alterConfigs} — no {@code INVOKE*} instruction in the outer method.
 * The deferred call has the same deprecated semantics, so the rule's
 * {@code INVOKEDYNAMIC} walk inspects {@code bsmArgs} via
 * {@link AsmUtil#indyTargetHandle(InvokeDynamicInsnNode, java.util.Set, String, String)}
 * with {@code descriptor=null} (accept any descriptor) and flags the capture site.
 */
public final class AdminAlterConfigsDeprecatedRule implements Rule {

    private static final String ALTER_CONFIGS = "alterConfigs";

    private final Severity severity;

    public AdminAlterConfigsDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_ALTER_CONFIGS_DEPRECATED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && KafkaTypes.ADMIN_OWNERS.contains(mi.owner)
                        && ALTER_CONFIGS.equals(mi.name)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy
                        && AsmUtil.indyTargetHandle(indy, KafkaTypes.ADMIN_OWNERS, ALTER_CONFIGS, null) != null) {
                    out.add(violation(ctx, mn, insn));
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.ADMIN_ALTER_CONFIGS_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Admin.alterConfigs(...) is reached here — either as a direct call or as a "
                        + "method-reference capture (e.g. `admin::alterConfigs`) whose deferred "
                        + "invocation has the same deprecated semantics. This method family is "
                        + "deprecated since Kafka 2.3 (KIP-339) and is DATA-DESTRUCTIVE by design: "
                        + "the supplied Config is treated as the COMPLETE desired state for each "
                        + "resource, so any per-resource config key NOT present in the supplied "
                        + "Config gets RESET to the broker default. A call meaning 'raise retention "
                        + "on this topic' silently wipes every other per-topic config that was set "
                        + "(cleanup.policy, compression.type, min.insync.replicas, ...) back to "
                        + "broker defaults — for a compacted topic, cleanup.policy flips from "
                        + "`compact` to `delete` and the broker starts segment-deleting compacted "
                        + "records on the next log-cleaner run. Replace with "
                        + "incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>>) "
                        + "which applies each AlterConfigOp (SET / DELETE / APPEND / SUBTRACT) "
                        + "surgically; keys not mentioned in the request are left untouched. "
                        + "Equivalent safe call: admin.incrementalAlterConfigs(Map.of(resource, "
                        + "List.of(new AlterConfigOp(new ConfigEntry(\"key\", \"value\"), "
                        + "AlterConfigOp.OpType.SET)))).");
    }
}
