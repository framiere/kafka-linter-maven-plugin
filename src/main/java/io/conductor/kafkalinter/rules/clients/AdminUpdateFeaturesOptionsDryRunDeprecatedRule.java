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
 * Fires for every reach of the deprecated
 * {@link org.apache.kafka.clients.admin.UpdateFeaturesOptions#dryRun(boolean)}
 * setter and its sibling
 * {@link org.apache.kafka.clients.admin.UpdateFeaturesOptions#dryRun()}
 * getter — whether the call lands directly via {@code INVOKEVIRTUAL}
 * or indirectly through an {@code INVOKEDYNAMIC} method-reference
 * capture (e.g. {@code options::dryRun} bound to a fluent-builder SAM,
 * or {@code UpdateFeaturesOptions::dryRun} bound to a Function-style
 * predicate API).
 *
 * <h2>Why these methods are deprecated</h2>
 *
 * <p>KIP-919 (Kafka 3.5, June 2023) renamed
 * {@code UpdateFeaturesOptions.dryRun} to
 * {@code UpdateFeaturesOptions.validateOnly} so that the
 * validate-without-apply mode matches the canonical naming convention
 * used by every other AdminClient {@code *Options} class
 * ({@code AlterConfigsOptions.validateOnly},
 * {@code CreateTopicsOptions.validateOnly},
 * {@code DeleteRecordsOptions.validateOnly},
 * {@code FeatureUpdate.allowDowngrade} — all use {@code validateOnly}
 * for the dry-run flag). Five concrete incident classes the
 * deprecation tracks:
 *
 * <ul>
 *   <li><b>Cross-options-class inconsistency confuses readers.</b>
 *       Engineers grep'ing for {@code .validateOnly(true)} across an
 *       AdminClient code base would miss the
 *       {@code UpdateFeaturesOptions} call sites because they used the
 *       odd-one-out {@code dryRun} name; the dry-run-vs-apply
 *       distinction was hidden behind a different keyword, and an
 *       audit "list every call site that runs an apply-mode admin
 *       operation" had a blind spot on the feature-update API.</li>
 *   <li><b>Refactor target named differently — invisible to
 *       grep-driven migration.</b> A team adopting "audit-only first,
 *       then apply" as a rollout policy would write a one-shot
 *       refactor that flips {@code .validateOnly(true)} call sites to
 *       {@code .validateOnly(false)} after sign-off; the
 *       {@code dryRun} call sites had to be migrated separately under
 *       a different name, doubling the migration coordination
 *       cost.</li>
 *   <li><b>Wire-equivalent — silent migration.</b> The new
 *       {@code validateOnly} method maps to the same protocol field
 *       on the {@code UpdateFeatures} RPC; the rename is purely a
 *       Java-API naming-consistency cleanup. There is no wire-level
 *       behavior change, so a caller that has not migrated keeps
 *       working at runtime; the deprecation tracks future code-base
 *       consistency, not a behavioral regression.</li>
 *   <li><b>Getter is part of the same deprecation.</b>
 *       {@code dryRun()} (the zero-arg getter) is also deprecated;
 *       callers that introspect the value for logging or audit (e.g.
 *       "log `dryRun=true` before issuing the RPC") must rename to
 *       {@code validateOnly()} or their introspection drifts from the
 *       fluent-builder method names the rest of the AdminClient
 *       surface uses.</li>
 *   <li><b>INVOKEDYNAMIC capture path.</b> A code base that uses
 *       Options configuration via a fluent-builder DSL (e.g. a
 *       generic {@code Consumer<UpdateFeaturesOptions>} parameter)
 *       can capture the setter as {@code options::dryRun} bound to
 *       that Consumer SAM. The user-class bytecode contains zero
 *       direct {@code INVOKEVIRTUAL} on the legacy method — only the
 *       {@code INVOKEDYNAMIC} + {@code LambdaMetafactory} bridge
 *       whose bsm-args contain a {@code REF_invokeVirtual} handle
 *       pointing at the legacy method. A name-only MethodInsnNode
 *       walk misses this case entirely.</li>
 * </ul>
 *
 * <h2>Descriptor discrimination — getter and setter share a name</h2>
 *
 * <p>The owner has two distinct methods named {@code dryRun}:
 *
 * <ul>
 *   <li>Setter:
 *       {@code (Z)Lorg/apache/kafka/clients/admin/UpdateFeaturesOptions;}
 *       — accepts a {@code boolean} and returns the Options instance
 *       for fluent chaining.</li>
 *   <li>Getter: {@code ()Z} — zero-arg, returns {@code boolean}.</li>
 * </ul>
 *
 * <p>Both are deprecated by KIP-919. The rule iterates over both
 * descriptors at each candidate call site.
 *
 * <h2>Method-reference capture path</h2>
 *
 * <p>{@code options::dryRun} bound to a SAM that takes a
 * {@code boolean} compiles to {@code INVOKEDYNAMIC} whose bsm-args
 * contain a {@code REF_invokeVirtual} handle pointing at the resolved
 * setter. {@code UpdateFeaturesOptions::dryRun} bound to a
 * {@code Predicate<UpdateFeaturesOptions>} captures the getter. The
 * rule's bsm-arg walk catches both cases by checking the handle's
 * {@code (owner, name, desc)} triple against the same filter used for
 * direct calls, iterated over both legacy descriptors.
 */
public final class AdminUpdateFeaturesOptionsDryRunDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.ADMIN_UPDATE_FEATURES_OPTIONS);
    private static final String METHOD_NAME = "dryRun";
    private static final Set<String> LEGACY_DESCS = Set.of(
            "(Z)Lorg/apache/kafka/clients/admin/UpdateFeaturesOptions;",
            "()Z");

    private final Severity severity;

    public AdminUpdateFeaturesOptionsDryRunDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_UPDATE_FEATURES_OPTIONS_DRY_RUN_DEPRECATED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi
                        && OWNERS.contains(mi.owner)
                        && METHOD_NAME.equals(mi.name)
                        && LEGACY_DESCS.contains(mi.desc)) {
                    out.add(violation(ctx, mn, insn));
                    continue;
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    for (String desc : LEGACY_DESCS) {
                        Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, desc);
                        if (h != null) {
                            out.add(violation(ctx, mn, insn));
                            break;
                        }
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.ADMIN_UPDATE_FEATURES_OPTIONS_DRY_RUN_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "UpdateFeaturesOptions.dryRun(boolean) or its zero-arg getter "
                        + "UpdateFeaturesOptions.dryRun() is reached here — either as a "
                        + "direct call or as an INVOKEDYNAMIC method-reference capture "
                        + "(e.g. `options::dryRun` bound to a fluent-builder Consumer "
                        + "SAM or `UpdateFeaturesOptions::dryRun` bound to a Predicate "
                        + "returning boolean). These methods are deprecated since "
                        + "Kafka 3.5 (KIP-919, June 2023) because UpdateFeaturesOptions "
                        + "was the only AdminClient *Options class that called the "
                        + "validate-without-apply mode `dryRun` instead of the "
                        + "canonical `validateOnly` used by every other Options class "
                        + "(AlterConfigsOptions.validateOnly, "
                        + "CreateTopicsOptions.validateOnly, "
                        + "DeleteRecordsOptions.validateOnly, ...). Five concrete "
                        + "failure modes follow: (1) cross-options-class inconsistency "
                        + "— engineers grep'ing for `.validateOnly(true)` across an "
                        + "AdminClient code base miss the UpdateFeaturesOptions call "
                        + "sites because they used the odd-one-out `dryRun` name; an "
                        + "audit \"list every call site that runs an apply-mode admin "
                        + "operation\" had a blind spot on the feature-update API; "
                        + "(2) refactor target named differently — a team adopting "
                        + "\"audit-only first, then apply\" as a rollout policy writing "
                        + "a one-shot refactor that flips `.validateOnly(true)` call "
                        + "sites to `.validateOnly(false)` after sign-off had to "
                        + "migrate the `dryRun` call sites separately under a different "
                        + "name, doubling the migration coordination cost; (3) "
                        + "wire-equivalent — the new validateOnly method maps to the "
                        + "same protocol field on the UpdateFeatures RPC, so the "
                        + "rename is purely a Java-API naming-consistency cleanup; "
                        + "there is no wire-level behavior change, so a caller that "
                        + "has not migrated keeps working at runtime, and the "
                        + "deprecation tracks future code-base consistency rather "
                        + "than a behavioral regression; (4) zero-arg getter is part "
                        + "of the same deprecation — callers that introspect the value "
                        + "for logging or audit (e.g. `log.info(\"dryRun=\" + "
                        + "options.dryRun())` before issuing the RPC) must rename to "
                        + "validateOnly() or their introspection drifts from the "
                        + "fluent-builder method names the rest of the AdminClient "
                        + "surface uses; (5) INVOKEDYNAMIC capture — a fluent-builder "
                        + "DSL (`Consumer<UpdateFeaturesOptions>` parameter) capturing "
                        + "the setter as `options::dryRun` emits zero direct "
                        + "INVOKEVIRTUAL on the legacy method and a name-only walk "
                        + "misses it; the bsm-args of the indy site contain a "
                        + "REF_invokeVirtual handle pointing at the legacy method. "
                        + "Migration: rename dryRun(boolean) call sites to "
                        + "validateOnly(boolean) and dryRun() call sites to "
                        + "validateOnly(). The new method is wire-equivalent (same "
                        + "internal field on the UpdateFeatures RPC), so the rename "
                        + "is purely a Java-API cleanup.");
    }
}
