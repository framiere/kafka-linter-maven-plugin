package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ElectLeadersOptions;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call passes an {@link
 * ElectLeadersOptions} so the descriptor contains
 * {@code org/apache/kafka/clients/admin/ElectLeadersOptions}
 * and the predicate rejects it.
 */
public final class GoodAdminElectLeadersNoOptions {

    /** 3-arg SAM whose erased descriptor matches the safe
     *  {@code Admin.electLeaders(ElectionType, Set,
     *  ElectLeadersOptions) ElectLeadersResult} overload. */
    @FunctionalInterface
    interface OptionsElectLeadersTri {
        ElectLeadersResult apply(ElectionType type, Set<TopicPartition> partitions, ElectLeadersOptions options);
    }

    /** Alternate 3-arg SAM with the same erased descriptor —
     *  exercises a second indy capture shape. */
    @FunctionalInterface
    interface OptionsElectLeadersFactory {
        ElectLeadersResult apply(ElectionType type, Set<TopicPartition> partitions, ElectLeadersOptions options);
    }

    private static Set<TopicPartition> partitions() {
        return Set.of(new TopicPartition("orders", 0));
    }

    private static ElectLeadersOptions opts() {
        return new ElectLeadersOptions().timeoutMs(120_000);
    }

    public ElectLeadersResult electA(Admin admin) {
        return admin.electLeaders(ElectionType.PREFERRED, partitions(), opts());
    }

    public ElectLeadersResult electB(Admin admin) {
        return admin.electLeaders(ElectionType.UNCLEAN, partitions(), opts());
    }

    public ElectLeadersResult electC(Admin admin) {
        ElectLeadersResult r = admin.electLeaders(ElectionType.PREFERRED, partitions(), opts());
        return r;
    }

    public static ElectLeadersResult electD(Admin admin) {
        return admin.electLeaders(ElectionType.PREFERRED, partitions(), opts());
    }

    public OptionsElectLeadersTri triFactory(Admin admin) {
        return admin::electLeaders;
    }

    public OptionsElectLeadersFactory customFactory(Admin admin) {
        return admin::electLeaders;
    }

    public ElectLeadersResult useLocalFactory(Admin admin) {
        OptionsElectLeadersFactory factory = admin::electLeaders;
        return factory.apply(ElectionType.PREFERRED, partitions(), opts());
    }

    public Stream<ElectLeadersResult> electAll(List<Admin> admins) {
        return admins.stream().map(a -> a.electLeaders(ElectionType.PREFERRED, partitions(), opts()));
    }

    public static void main(String[] args) {
        new GoodAdminElectLeadersNoOptions();
    }
}
