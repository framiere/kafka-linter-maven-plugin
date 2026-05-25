package sample;

import org.apache.kafka.clients.admin.UpdateFeaturesOptions;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * RULE: ADMIN_UPDATE_FEATURES_OPTIONS_DRY_RUN_DEPRECATED — must NOT fire
 * on any of the four call sites below.
 *
 * <p>All four shapes target the KIP-919 migration replacement
 * {@code UpdateFeaturesOptions.validateOnly(...)} / {@code .validateOnly()}.
 * The owner is identical to the legacy call ({@code UpdateFeaturesOptions}),
 * but the method name differs — so the rule's
 * {@code (OWNER + METHOD_NAME = "dryRun" + DESC)} filter rejects every
 * site here. This proves the rule does not over-fire on the new API.
 *
 * <h2>Why these four shapes specifically</h2>
 *
 * <p>The shapes mirror the BAD fixture one-for-one so that the GOOD
 * fixture exercises the exact same bytecode paths the rule walks:
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code UpdateFeaturesOptions.validateOnly(boolean)} — same shape
 *       as the legacy setter call, only the method name differs.</li>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code UpdateFeaturesOptions.validateOnly()} — same shape as
 *       the legacy getter call.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code options::validateOnly} bound to
 *       {@code Function<Boolean, UpdateFeaturesOptions>}. The bsm-args
 *       hold a {@code REF_invokeVirtual} handle whose desc is
 *       {@code (Z)Lorg/apache/kafka/clients/admin/UpdateFeaturesOptions;}
 *       — DESC matches the legacy setter, but NAME does not.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code UpdateFeaturesOptions::validateOnly} bound to
 *       {@code Predicate<UpdateFeaturesOptions>}. The bsm-args hold a
 *       {@code REF_invokeVirtual} handle whose desc is {@code ()Z} —
 *       DESC matches the legacy getter, but NAME does not.</li>
 * </ol>
 *
 * <p>Shapes 3 and 4 are the most important non-firing cases: an
 * over-eager rule that only checked owner + desc (skipping NAME) would
 * fire on these. A rule that only checked owner + name (skipping DESC)
 * would not. The dedicated rule checks (OWNER + NAME + DESC) on both
 * the direct and indy paths and so passes here.
 */
public final class GoodUpdateFeaturesOptionsDryRun {

    public UpdateFeaturesOptions directSetter() {
        UpdateFeaturesOptions options = new UpdateFeaturesOptions();
        return options.validateOnly(true);
    }

    public boolean directGetter() {
        UpdateFeaturesOptions options = new UpdateFeaturesOptions();
        return options.validateOnly();
    }

    public Function<Boolean, UpdateFeaturesOptions> capturedSetter() {
        UpdateFeaturesOptions options = new UpdateFeaturesOptions();
        return options::validateOnly;
    }

    public Predicate<UpdateFeaturesOptions> capturedGetter() {
        return UpdateFeaturesOptions::validateOnly;
    }
}
