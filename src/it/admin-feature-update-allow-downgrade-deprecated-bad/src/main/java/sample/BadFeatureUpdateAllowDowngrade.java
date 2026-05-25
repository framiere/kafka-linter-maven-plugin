package sample;

import org.apache.kafka.clients.admin.FeatureUpdate;

import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * RULE: ADMIN_FEATURE_UPDATE_ALLOW_DOWNGRADE_DEPRECATED.
 *
 * <p>Exercises four shapes the rule must catch:
 *
 * <ol>
 *   <li>Direct {@code INVOKESPECIAL} on
 *       {@code FeatureUpdate.<init>(short, boolean)} — the deprecated
 *       boolean constructor whose flag conflates SAFE_DOWNGRADE with
 *       UNSAFE_DOWNGRADE.</li>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code FeatureUpdate.allowDowngrade()Z} — the deprecated getter
 *       that returns {@code true} for both safe and unsafe downgrades.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code update::allowDowngrade} into a {@code Supplier<Boolean>}.
 *       User-class bytecode contains ZERO {@code INVOKE*} targeting
 *       {@code allowDowngrade}; the call lives only in the
 *       {@code LambdaMetafactory}-synthesized bridge.</li>
 *   <li>{@code INVOKEDYNAMIC} constructor-ref capture
 *       {@code FeatureUpdate::new} into a
 *       {@code BiFunction<Short, Boolean, FeatureUpdate>}. The bsmArg is a
 *       {@code REF_newInvokeSpecial} handle pointing at the legacy
 *       {@code (SZ)V} constructor; descriptor discrimination matters
 *       because the new {@code (S, UpgradeType)V} constructor shares the
 *       {@code <init>} name.</li>
 * </ol>
 *
 * <h2>Why these are dangerous, not stylistic</h2>
 *
 * <p>{@code FeatureUpdate} drives the cluster-wide finalized feature
 * levels (metadata.version, kraft.version, group.version, etc.). The
 * legacy boolean cannot express UNSAFE_DOWNGRADE — the documented
 * emergency-rollback path for when a higher level has wedged the cluster
 * and operators are willing to accept record-loss / re-bootstrap risk.
 * Tooling stuck on the boolean API silently degrades the rollback to
 * SAFE, which brokers refuse with FEATURE_UPDATE_FAILED whenever records
 * cannot be losslessly re-serialized — operators see &laquo;downgrade
 * rejected&raquo; and conclude their cluster is unrecoverable when in
 * fact UNSAFE_DOWNGRADE is the documented escape hatch they cannot reach.
 *
 * <p>On the read side {@code allowDowngrade()} returns {@code true} for
 * BOTH SAFE_DOWNGRADE and UNSAFE_DOWNGRADE, so audit logs and
 * change-request UIs built on the getter cannot distinguish a safe
 * rollback from an unsafe one — the exact distinction operators need to
 * see before approving a downgrade in production.
 */
public final class BadFeatureUpdateAllowDowngrade {

    public FeatureUpdate buildLegacyBooleanCtor(short maxVersionLevel) {
        // FIRES — direct INVOKESPECIAL on FeatureUpdate.<init>(SZ)V. The
        // boolean true maps to SAFE_DOWNGRADE; there is no boolean
        // expression for UNSAFE_DOWNGRADE.
        return new FeatureUpdate(maxVersionLevel, true);
    }

    @SuppressWarnings("deprecation")
    public boolean readLegacyAllowDowngrade(FeatureUpdate update) {
        // FIRES — direct INVOKEVIRTUAL on FeatureUpdate.allowDowngrade()Z.
        // Returns true for both SAFE_DOWNGRADE and UNSAFE_DOWNGRADE; the
        // caller cannot tell which.
        return update.allowDowngrade();
    }

    @SuppressWarnings("deprecation")
    public Supplier<Boolean> capturedAllowDowngrade(FeatureUpdate update) {
        // FIRES — INVOKEDYNAMIC method-ref capture. SAM Supplier#get
        // returns Object; the bsm-arg handle is REF_invokeVirtual targeting
        // FeatureUpdate.allowDowngrade()Z. User-class bytecode contains
        // ZERO INVOKE* targeting allowDowngrade; the rule's bsm-arg walk
        // catches the handle's (owner, name, desc) triple.
        return update::allowDowngrade;
    }

    public BiFunction<Short, Boolean, FeatureUpdate> capturedLegacyCtor() {
        // FIRES — INVOKEDYNAMIC constructor-ref capture. SAM
        // BiFunction#apply takes (Object, Object) -> Object; the bsm-arg
        // handle is REF_newInvokeSpecial targeting FeatureUpdate.<init>(SZ)V.
        // The replacement constructor FeatureUpdate.<init>(SLorg/apache/
        // kafka/clients/admin/FeatureUpdate$UpgradeType;)V shares the
        // <init> name, so descriptor discrimination is mandatory — the
        // rule matches on the full (owner, name, desc) triple to avoid
        // firing on the supported new constructor.
        return FeatureUpdate::new;
    }
}
