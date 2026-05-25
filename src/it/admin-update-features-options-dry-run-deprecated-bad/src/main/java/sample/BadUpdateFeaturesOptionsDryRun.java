package sample;

import org.apache.kafka.clients.admin.UpdateFeaturesOptions;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * RULE: ADMIN_UPDATE_FEATURES_OPTIONS_DRY_RUN_DEPRECATED — must fire
 * on the four call sites below.
 *
 * <p>The four methods exercise the four distinct bytecode shapes the
 * rule is required to catch — direct {@code INVOKEVIRTUAL} on the
 * setter and the getter, plus {@code INVOKEDYNAMIC} method-ref
 * captures targeting each of those two methods:
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code UpdateFeaturesOptions.dryRun(boolean)}. The descriptor
 *       at the call site is
 *       {@code (Z)Lorg/apache/kafka/clients/admin/UpdateFeaturesOptions;}
 *       — the legacy setter; returns the Options instance for fluent
 *       chaining.</li>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code UpdateFeaturesOptions.dryRun()}. The descriptor at the
 *       call site is {@code ()Z} — the zero-arg getter; same
 *       deprecation as the setter because the introspection name
 *       drifts from the canonical {@code validateOnly} used by every
 *       other AdminClient *Options class.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code options::dryRun} bound to a
 *       {@code Function<Boolean, UpdateFeaturesOptions>}. The
 *       user-class bytecode at this site contains ZERO direct
 *       {@code INVOKEVIRTUAL} on the legacy setter — only the
 *       {@code INVOKEDYNAMIC} + {@code LambdaMetafactory} bridge whose
 *       bsm-args contain a {@code REF_invokeVirtual} handle pointing
 *       at the legacy setter, with desc
 *       {@code (Z)Lorg/apache/kafka/clients/admin/UpdateFeaturesOptions;}.</li>
 *   <li>{@code INVOKEDYNAMIC} method-ref capture
 *       {@code UpdateFeaturesOptions::dryRun} bound to a
 *       {@code Predicate<UpdateFeaturesOptions>}. The bsm-args contain
 *       a {@code REF_invokeVirtual} handle pointing at the legacy
 *       getter, with desc {@code ()Z}.</li>
 * </ol>
 *
 * <h2>Why these methods are deprecated (KIP-919 summary)</h2>
 *
 * <p>KIP-919 (Kafka 3.5, June 2023) renamed
 * {@code UpdateFeaturesOptions.dryRun} to
 * {@code UpdateFeaturesOptions.validateOnly} so that the
 * validate-without-apply mode matches the canonical naming convention
 * used by every other AdminClient {@code *Options} class
 * ({@code AlterConfigsOptions.validateOnly},
 * {@code CreateTopicsOptions.validateOnly},
 * {@code DeleteRecordsOptions.validateOnly}, ...). The rename is
 * purely a Java-API naming-consistency cleanup; the new method maps
 * to the same protocol field on the {@code UpdateFeatures} RPC, so
 * there is no wire-level behavior change.
 */
public final class BadUpdateFeaturesOptionsDryRun {

    @SuppressWarnings("deprecation")
    public UpdateFeaturesOptions directSetter() {
        // MUST FIRE — direct INVOKEVIRTUAL on
        // UpdateFeaturesOptions.dryRun(boolean). The descriptor at the
        // call site is
        // (Z)Lorg/apache/kafka/clients/admin/UpdateFeaturesOptions; —
        // the legacy setter.
        UpdateFeaturesOptions options = new UpdateFeaturesOptions();
        return options.dryRun(true);
    }

    @SuppressWarnings("deprecation")
    public boolean directGetter() {
        // MUST FIRE — direct INVOKEVIRTUAL on
        // UpdateFeaturesOptions.dryRun(). The descriptor at the call
        // site is ()Z — the legacy getter; same deprecation as the
        // setter because the introspection name drifts from the
        // canonical `validateOnly` used by every other AdminClient
        // *Options class.
        UpdateFeaturesOptions options = new UpdateFeaturesOptions();
        return options.dryRun();
    }

    @SuppressWarnings("deprecation")
    public Function<Boolean, UpdateFeaturesOptions> capturedSetter() {
        // MUST FIRE — INVOKEDYNAMIC method-ref capture
        // `options::dryRun` bound to a
        // Function<Boolean, UpdateFeaturesOptions>. The receiver
        // `options` is captured; the SAM apply signature
        // (boolean -> UpdateFeaturesOptions) erases to match the
        // legacy setter. The user-class bytecode contains ZERO direct
        // INVOKEVIRTUAL on the legacy method — only the INVOKEDYNAMIC
        // bridge whose bsm-args hold a REF_invokeVirtual handle whose
        // desc is (Z)Lorg/apache/kafka/clients/admin/UpdateFeaturesOptions;.
        UpdateFeaturesOptions options = new UpdateFeaturesOptions();
        return options::dryRun;
    }

    @SuppressWarnings("deprecation")
    public Predicate<UpdateFeaturesOptions> capturedGetter() {
        // MUST FIRE — INVOKEDYNAMIC method-ref capture
        // `UpdateFeaturesOptions::dryRun` bound to a
        // Predicate<UpdateFeaturesOptions>. Unbound — the receiver is
        // the SAM's first (and only) argument. The bsm-args hold a
        // REF_invokeVirtual handle whose desc is ()Z — the legacy
        // getter.
        return UpdateFeaturesOptions::dryRun;
    }
}
