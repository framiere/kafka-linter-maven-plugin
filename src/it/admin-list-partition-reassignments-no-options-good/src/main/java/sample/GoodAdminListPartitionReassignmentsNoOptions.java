package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call passes a
 * {@link ListPartitionReassignmentsOptions} so the
 * descriptor contains
 * {@code org/apache/kafka/clients/admin/ListPartitionReassignmentsOptions}
 * and the predicate rejects it.
 */
public final class GoodAdminListPartitionReassignmentsNoOptions {

    @FunctionalInterface
    interface OptionsReassignFactory {
        ListPartitionReassignmentsResult apply(Set<TopicPartition> partitions,
                                               ListPartitionReassignmentsOptions options);
    }

    private static ListPartitionReassignmentsOptions opts() {
        return new ListPartitionReassignmentsOptions().timeoutMs(120_000);
    }

    public ListPartitionReassignmentsResult listA(Admin admin) {
        return admin.listPartitionReassignments(opts());
    }

    public ListPartitionReassignmentsResult listB(Admin admin) {
        Set<TopicPartition> tps = Set.of(new TopicPartition("orders", 0));
        return admin.listPartitionReassignments(tps, opts());
    }

    public ListPartitionReassignmentsResult listC(Admin admin) {
        return admin.listPartitionReassignments(Optional.empty(), opts());
    }

    public static ListPartitionReassignmentsResult listD(Admin admin) {
        return admin.listPartitionReassignments(opts());
    }

    public Function<ListPartitionReassignmentsOptions, ListPartitionReassignmentsResult> supplierFactory(Admin admin) {
        return admin::listPartitionReassignments;
    }

    public BiFunction<Set<TopicPartition>, ListPartitionReassignmentsOptions, ListPartitionReassignmentsResult> setFactory(Admin admin) {
        return admin::listPartitionReassignments;
    }

    public ListPartitionReassignmentsResult useLocalFactory(Admin admin) {
        OptionsReassignFactory factory = admin::listPartitionReassignments;
        return factory.apply(Set.of(new TopicPartition("orders", 1)), opts());
    }

    public Stream<ListPartitionReassignmentsResult> listAll(List<Admin> admins) {
        return admins.stream().map(a -> a.listPartitionReassignments(opts()));
    }

    public static void main(String[] args) {
        new GoodAdminListPartitionReassignmentsNoOptions();
    }
}
