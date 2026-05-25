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
 * Fires for every reach of the deprecated boolean-flavoured {@code FeatureUpdate}
 * surface — both the boolean constructor {@code FeatureUpdate(short maxVersionLevel,
 * boolean allowDowngrade)} and the boolean getter {@code allowDowngrade()} — whether
 * the call lands directly via {@code INVOKESPECIAL} / {@code INVOKEVIRTUAL} or
 * indirectly through an {@code INVOKEDYNAMIC} method-reference capture (e.g.
 * {@code FeatureUpdate::new} bound to a {@code BiFunction<Short, Boolean, FeatureUpdate>}
 * or {@code update::allowDowngrade} bound to a {@code Supplier<Boolean>}).
 *
 * <h2>Why the boolean overload is dangerous, not merely cosmetic</h2>
 *
 * <p>{@code Admin.updateFeatures(Map<String, FeatureUpdate>, ...)} drives the
 * cluster-wide finalized feature levels (metadata.version, kraft.version,
 * group.version, etc.). The legacy constructor's {@code boolean allowDowngrade}
 * collapses what Kafka's broker-side dispatcher actually distinguishes into a
 * single bit:
 *
 * <ul>
 *   <li><b>UPGRADE</b> — strictly raise the finalized level. Failure mode: if
 *       the current level is already at or above the requested level the call
 *       errors with {@code INVALID_UPDATE_VERSION}.</li>
 *   <li><b>SAFE_DOWNGRADE</b> — lower the finalized level, but only if the
 *       broker reports the feature implements the lossless-downgrade hook
 *       (e.g. metadata records can be re-serialized at the lower version).
 *       Brokers REFUSE the downgrade if any in-flight data would be lost.</li>
 *   <li><b>UNSAFE_DOWNGRADE</b> — lower the finalized level even when the
 *       broker reports the downgrade would drop or rewrite metadata records.
 *       This is the only path for emergency rollback when a higher level has
 *       wedged the cluster and operators are willing to accept the data
 *       loss / re-bootstrap risk.</li>
 * </ul>
 *
 * <p>The legacy boolean maps {@code false} to {@code UPGRADE} and
 * {@code true} to {@code SAFE_DOWNGRADE}. <b>There is no boolean expression
 * for {@code UNSAFE_DOWNGRADE}.</b> Code stuck on the legacy constructor
 * therefore cannot perform the emergency-rollback path at all — the call
 * silently degrades to a SAFE downgrade, which the brokers refuse with a
 * {@code FEATURE_UPDATE_FAILED} error when records cannot be losslessly
 * re-serialized. Operators see &laquo;downgrade rejected&raquo; logs and
 * conclude their cluster is unrecoverable, when in fact the new
 * {@code UNSAFE_DOWNGRADE} path is the documented escape hatch.
 *
 * <h2>The legacy getter has the same blind spot on the read side</h2>
 *
 * <p>{@code allowDowngrade()} returns {@code true} for both
 * {@code SAFE_DOWNGRADE} and {@code UNSAFE_DOWNGRADE} updates and
 * {@code false} for {@code UPGRADE} updates. Reconcilers, audit logs, and
 * change-request UIs built on the getter cannot tell a safe rollback from an
 * unsafe one — both look identical, which is the exact distinction operators
 * need to see before approving a downgrade in production.
 *
 * <h2>The replacement API</h2>
 *
 * <p>KIP-778 (Kafka 3.3) introduced the
 * {@link org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType}
 * enum and the constructor
 * {@code FeatureUpdate(short maxVersionLevel, FeatureUpdate.UpgradeType upgradeType)}
 * with the read-side getter {@code upgradeType()} returning the enum value.
 * The three-way enum is the on-wire shape; the boolean form is a one-way
 * encoding that loses information.
 *
 * <h2>Method-reference / constructor-reference capture path</h2>
 *
 * <p>{@code update::allowDowngrade} compiles to {@code INVOKEDYNAMIC} whose
 * bsm-args contain a {@code REF_invokeVirtual} handle pointing at
 * {@code FeatureUpdate.allowDowngrade()Z}. {@code FeatureUpdate::new} bound
 * to a {@code BiFunction<Short, Boolean, FeatureUpdate>} compiles to
 * {@code INVOKEDYNAMIC} whose bsm-args contain a
 * {@code REF_newInvokeSpecial} handle pointing at
 * {@code FeatureUpdate.<init>(SZ)V}. In both cases the user-class bytecode
 * contains zero direct {@code INVOKE*} on the deprecated members — the
 * call lives only in the {@code LambdaMetafactory}-synthesized bridge,
 * and a name-only MethodInsnNode walk would miss it.
 *
 * <h2>Descriptor discrimination is mandatory</h2>
 *
 * <p>{@code <init>} is overloaded on {@code FeatureUpdate}: the new
 * {@code FeatureUpdate(short, UpgradeType)} constructor and the legacy
 * {@code FeatureUpdate(short, boolean)} constructor share the {@code <init>}
 * name. Name-only matching would fire on the supported new constructor as
 * well, producing false positives that drive callers off the only correct
 * migration. The rule therefore matches on the {@code (owner, name, desc)}
 * triple: {@code (FeatureUpdate, "<init>", "(SZ)V")} for the boolean
 * constructor and {@code (FeatureUpdate, "allowDowngrade", "()Z")} for the
 * getter. The replacement {@code <init>} has descriptor
 * {@code (SLorg/apache/kafka/clients/admin/FeatureUpdate$UpgradeType;)V}
 * and is left untouched.
 */
public final class AdminFeatureUpdateAllowDowngradeDeprecatedRule implements Rule {

    private static final Set<String> OWNERS = Set.of(KafkaTypes.ADMIN_FEATURE_UPDATE);
    private static final String LEGACY_CTOR_NAME = "<init>";
    private static final String LEGACY_CTOR_DESC = "(SZ)V";
    private static final String LEGACY_GETTER_NAME = "allowDowngrade";
    private static final String LEGACY_GETTER_DESC = "()Z";

    private final Severity severity;

    public AdminFeatureUpdateAllowDowngradeDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_FEATURE_UPDATE_ALLOW_DOWNGRADE_DEPRECATED;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
            for (AbstractInsnNode insn : mn.instructions) {
                if (insn instanceof MethodInsnNode mi && OWNERS.contains(mi.owner)) {
                    if (LEGACY_CTOR_NAME.equals(mi.name) && LEGACY_CTOR_DESC.equals(mi.desc)) {
                        out.add(violation(ctx, mn, insn, "FeatureUpdate(short, boolean) constructor"));
                        continue;
                    }
                    if (LEGACY_GETTER_NAME.equals(mi.name) && LEGACY_GETTER_DESC.equals(mi.desc)) {
                        out.add(violation(ctx, mn, insn, "FeatureUpdate.allowDowngrade() getter"));
                        continue;
                    }
                }
                if (insn instanceof InvokeDynamicInsnNode indy) {
                    Handle ctorH = AsmUtil.indyTargetHandle(indy, OWNERS, LEGACY_CTOR_NAME, LEGACY_CTOR_DESC);
                    if (ctorH != null) {
                        out.add(violation(ctx, mn, insn, "FeatureUpdate(short, boolean) constructor"));
                        continue;
                    }
                    Handle getterH = AsmUtil.indyTargetHandle(indy, OWNERS, LEGACY_GETTER_NAME, LEGACY_GETTER_DESC);
                    if (getterH != null) {
                        out.add(violation(ctx, mn, insn, "FeatureUpdate.allowDowngrade() getter"));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn, String shape) {
        return new Violation(
                RuleId.ADMIN_FEATURE_UPDATE_ALLOW_DOWNGRADE_DEPRECATED, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                shape + " is reached here — either as a direct call or as an "
                        + "INVOKEDYNAMIC method-reference / constructor-reference capture "
                        + "(e.g. `update::allowDowngrade` bound to a Supplier<Boolean>, or "
                        + "`FeatureUpdate::new` bound to a BiFunction<Short, Boolean, FeatureUpdate>). "
                        + "This boolean surface is deprecated since Kafka 3.3 (KIP-778) because "
                        + "the boolean flag conflates THREE broker-side semantics into one bit. "
                        + "On the write side Admin.updateFeatures(...) actually distinguishes "
                        + "UPGRADE (raise the finalized level), SAFE_DOWNGRADE (lower it only "
                        + "if records can be losslessly re-serialized at the lower version), "
                        + "and UNSAFE_DOWNGRADE (lower it even when records would be dropped "
                        + "or rewritten — the only path for emergency rollback when a higher "
                        + "level has wedged the cluster). The legacy constructor maps "
                        + "false→UPGRADE and true→SAFE_DOWNGRADE; there is NO boolean "
                        + "expression for UNSAFE_DOWNGRADE, so callers stuck on this API "
                        + "cannot perform the documented emergency-rollback path — their "
                        + "downgrade silently degrades to SAFE and the brokers refuse with "
                        + "FEATURE_UPDATE_FAILED whenever records cannot be losslessly "
                        + "re-serialized. On the read side allowDowngrade() returns true for "
                        + "BOTH SAFE_DOWNGRADE and UNSAFE_DOWNGRADE, so reconcilers, audit "
                        + "logs, and change-request UIs cannot tell a safe rollback from an "
                        + "unsafe one — the exact distinction operators need before approving "
                        + "a downgrade. Migrate to `new FeatureUpdate(version, "
                        + "FeatureUpdate.UpgradeType.UPGRADE | SAFE_DOWNGRADE | "
                        + "UNSAFE_DOWNGRADE)` on the write side and `update.upgradeType()` on "
                        + "the read side; the three-way enum is the on-wire shape, the "
                        + "boolean is a one-way encoding that loses information.");
    }
}
