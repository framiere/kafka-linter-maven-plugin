package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: ADMIN_INCREMENTAL_ALTER_CONFIGS_NO_OPTIONS — must
 * fire EXACTLY 8 times across this class (one per method
 * below).
 *
 * <p>Exercises the unsafe overload descriptor of {@link
 * Admin#incrementalAlterConfigs(Map)} — the 1-arg form
 * taking only a {@code Map<ConfigResource,
 * Collection<AlterConfigOp>>}. Each method exercises one
 * capture shape — direct {@code INVOKEINTERFACE} or
 * {@code INVOKEDYNAMIC} method-reference capture.
 */
public final class BadAdminIncrementalAlterConfigsNoOptions {

    /** 1-arg SAM whose erased descriptor matches
     *  {@code Admin.incrementalAlterConfigs(Map)
     *  AlterConfigsResult} when bound to an Admin
     *  receiver. */
    @FunctionalInterface
    interface IncrementalConfigFactory {
        AlterConfigsResult apply(Map<ConfigResource, Collection<AlterConfigOp>> ops);
    }

    private static Map<ConfigResource, Collection<AlterConfigOp>> ops() {
        return Map.of(
                new ConfigResource(ConfigResource.Type.TOPIC, "orders"),
                List.of(new AlterConfigOp(new ConfigEntry("retention.ms", "604800000"), AlterConfigOp.OpType.SET)));
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overload =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  Admin.incrementalAlterConfigs(Map). */
    public AlterConfigsResult alterA(Admin admin) {
        return admin.incrementalAlterConfigs(ops());
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the same
     *  overload with inline map binding. */
    public AlterConfigsResult alterB(Admin admin) {
        Map<ConfigResource, Collection<AlterConfigOp>> o = ops();
        return admin.incrementalAlterConfigs(o);
    }

    /** MUST FIRE — direct INVOKEINTERFACE with intermediate
     *  local-variable binding. */
    public AlterConfigsResult alterC(Admin admin) {
        AlterConfigsResult r = admin.incrementalAlterConfigs(ops());
        return r;
    }

    /** MUST FIRE — direct INVOKEINTERFACE inside a static
     *  helper. */
    public static AlterConfigsResult alterD(Admin admin) {
        return admin.incrementalAlterConfigs(ops());
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code admin::incrementalAlterConfigs} as
     *  a bound-receiver method reference. Indy implMethod
     *  handle descriptor matches
     *  Admin.incrementalAlterConfigs(Map) AlterConfigsResult
     *  — the 1-arg overload. */
    public Function<Map<ConfigResource, Collection<AlterConfigOp>>, AlterConfigsResult> mapFactory(Admin admin) {
        return admin::incrementalAlterConfigs;
    }

    /** MUST FIRE — same method reference bound to a custom
     *  1-arg SAM whose erased descriptor matches the unsafe
     *  overload. */
    public IncrementalConfigFactory customFactory(Admin admin) {
        return admin::incrementalAlterConfigs;
    }

    /** MUST FIRE — local SAM binding via bound-receiver
     *  method reference, applied inside the same method. */
    public AlterConfigsResult useLocalFactory(Admin admin) {
        IncrementalConfigFactory factory = admin::incrementalAlterConfigs;
        return factory.apply(ops());
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code a.incrementalAlterConfigs(ops())} per element.
     *  The synthetic {@code lambda$alterAll$0} carries a
     *  direct INVOKEINTERFACE on the 1-arg overload. */
    public Stream<AlterConfigsResult> alterAll(List<Admin> admins) {
        return admins.stream().map(a -> a.incrementalAlterConfigs(ops()));
    }

    public static void main(String[] args) {
        new BadAdminIncrementalAlterConfigsNoOptions();
    }
}
