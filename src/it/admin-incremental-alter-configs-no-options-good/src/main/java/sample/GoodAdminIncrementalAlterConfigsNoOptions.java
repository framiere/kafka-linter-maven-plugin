package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call passes an {@link
 * AlterConfigsOptions} so the descriptor contains
 * {@code org/apache/kafka/clients/admin/AlterConfigsOptions}
 * and the predicate rejects it.
 */
public final class GoodAdminIncrementalAlterConfigsNoOptions {

    @FunctionalInterface
    interface OptionsIncrementalConfigFactory {
        AlterConfigsResult apply(Map<ConfigResource, Collection<AlterConfigOp>> ops, AlterConfigsOptions options);
    }

    private static Map<ConfigResource, Collection<AlterConfigOp>> ops() {
        return Map.of(
                new ConfigResource(ConfigResource.Type.TOPIC, "orders"),
                List.of(new AlterConfigOp(new ConfigEntry("retention.ms", "604800000"), AlterConfigOp.OpType.SET)));
    }

    private static AlterConfigsOptions opts() {
        return new AlterConfigsOptions().timeoutMs(120_000);
    }

    public AlterConfigsResult alterA(Admin admin) {
        return admin.incrementalAlterConfigs(ops(), opts());
    }

    public AlterConfigsResult alterB(Admin admin) {
        Map<ConfigResource, Collection<AlterConfigOp>> o = ops();
        return admin.incrementalAlterConfigs(o, opts());
    }

    public AlterConfigsResult alterC(Admin admin) {
        AlterConfigsResult r = admin.incrementalAlterConfigs(ops(), opts());
        return r;
    }

    public static AlterConfigsResult alterD(Admin admin) {
        return admin.incrementalAlterConfigs(ops(), opts());
    }

    public BiFunction<Map<ConfigResource, Collection<AlterConfigOp>>, AlterConfigsOptions, AlterConfigsResult> mapFactory(Admin admin) {
        return admin::incrementalAlterConfigs;
    }

    public OptionsIncrementalConfigFactory customFactory(Admin admin) {
        return admin::incrementalAlterConfigs;
    }

    public AlterConfigsResult useLocalFactory(Admin admin) {
        OptionsIncrementalConfigFactory factory = admin::incrementalAlterConfigs;
        return factory.apply(ops(), opts());
    }

    public Stream<AlterConfigsResult> alterAll(List<Admin> admins) {
        return admins.stream().map(a -> a.incrementalAlterConfigs(ops(), opts()));
    }

    public static void main(String[] args) {
        new GoodAdminIncrementalAlterConfigsNoOptions();
    }
}
