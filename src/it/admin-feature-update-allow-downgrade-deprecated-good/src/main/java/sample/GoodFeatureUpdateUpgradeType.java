package sample;

import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType;

import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * RULE: ADMIN_FEATURE_UPDATE_ALLOW_DOWNGRADE_DEPRECATED — must NOT fire.
 *
 * <p>The four methods below exercise the supported KIP-778 surface
 * ({@link UpgradeType} enum + {@code upgradeType()} getter):
 *
 * <ol>
 *   <li>{@code new FeatureUpdate(version, UpgradeType.UPGRADE)} — direct
 *       {@code INVOKESPECIAL} on the supported
 *       {@code (S, UpgradeType)V} constructor.</li>
 *   <li>{@code update.upgradeType()} — direct {@code INVOKEVIRTUAL} on the
 *       supported getter returning the enum value, not a boolean.</li>
 *   <li>{@code update::upgradeType} method-ref capture into a
 *       {@code Supplier<UpgradeType>}. The bsm-arg handle is
 *       {@code REF_invokeVirtual} on
 *       {@code FeatureUpdate.upgradeType()Lorg/apache/kafka/clients/admin/FeatureUpdate$UpgradeType;}
 *       — name {@code upgradeType} is not in the LEGACY_NAMES set, so the
 *       walk rejects this site.</li>
 *   <li>{@code FeatureUpdate::new} constructor-ref bound to a
 *       {@code BiFunction<Short, UpgradeType, FeatureUpdate>}. The
 *       bsm-arg handle is {@code REF_newInvokeSpecial} on the supported
 *       {@code (S, UpgradeType)V} constructor — descriptor
 *       {@code (SLorg/apache/kafka/clients/admin/FeatureUpdate$UpgradeType;)V}
 *       is NOT {@code (SZ)V}, so the rule's descriptor filter rejects
 *       this site even though the name {@code <init>} matches.</li>
 * </ol>
 *
 * <h2>Why this is safe</h2>
 *
 * <p>{@link UpgradeType} is a three-value enum — {@code UPGRADE},
 * {@code SAFE_DOWNGRADE}, {@code UNSAFE_DOWNGRADE} — that maps 1:1 to
 * the broker-side dispatcher. Operators can express the documented
 * emergency-rollback path ({@code UNSAFE_DOWNGRADE}) that the legacy
 * boolean cannot reach; audit logs and change-request UIs can
 * distinguish a safe rollback from an unsafe one on the read side. The
 * rule's descriptor discrimination ensures the supported
 * {@code (S, UpgradeType)V} constructor and the supported
 * {@code upgradeType()} getter never fire even though they share the
 * {@code <init>} name with the deprecated boolean constructor.
 */
public final class GoodFeatureUpdateUpgradeType {

    public FeatureUpdate buildUpgrade(short maxVersionLevel) {
        // DOES NOT FIRE — supported (S, UpgradeType)V constructor. The
        // rule's descriptor filter on <init> matches only (SZ)V.
        return new FeatureUpdate(maxVersionLevel, UpgradeType.UPGRADE);
    }

    public FeatureUpdate buildUnsafeDowngrade(short maxVersionLevel) {
        // DOES NOT FIRE — same supported constructor, requesting the
        // emergency-rollback path that the legacy boolean cannot express.
        return new FeatureUpdate(maxVersionLevel, UpgradeType.UNSAFE_DOWNGRADE);
    }

    public UpgradeType readUpgradeType(FeatureUpdate update) {
        // DOES NOT FIRE — supported upgradeType() getter. Returns the
        // three-way enum, not a boolean — callers can distinguish
        // SAFE_DOWNGRADE from UNSAFE_DOWNGRADE.
        return update.upgradeType();
    }

    public Supplier<UpgradeType> capturedUpgradeType(FeatureUpdate update) {
        // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture targeting the
        // SUPPORTED getter. The bsmArg handle's name is "upgradeType",
        // not in the rule's LEGACY_NAMES set, so the walk rejects it.
        return update::upgradeType;
    }

    public BiFunction<Short, UpgradeType, FeatureUpdate> capturedSupportedCtor() {
        // DOES NOT FIRE — INVOKEDYNAMIC constructor-ref capture. The
        // bsmArg handle name is <init> (which IS in LEGACY_NAMES) BUT
        // the descriptor is (SLorg/apache/kafka/clients/admin/
        // FeatureUpdate$UpgradeType;)V, not (SZ)V — the descriptor
        // filter rejects the site. This is precisely why descriptor
        // discrimination is mandatory: the new constructor shares the
        // <init> name with the legacy boolean one, and a name-only
        // match would false-positive on the supported migration target.
        return FeatureUpdate::new;
    }
}
