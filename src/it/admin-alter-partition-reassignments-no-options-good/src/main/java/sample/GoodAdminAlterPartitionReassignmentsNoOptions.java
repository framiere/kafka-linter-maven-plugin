package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call passes an
 * {@link AlterPartitionReassignmentsOptions} so the
 * descriptor contains
 * {@code org/apache/kafka/clients/admin/AlterPartitionReassignmentsOptions}
 * and the predicate rejects it.
 */
public final class GoodAdminAlterPartitionReassignmentsNoOptions {

    @FunctionalInterface
    interface OptionsReassignFactory {
        AlterPartitionReassignmentsResult apply(
                Map<TopicPartition, Optional<NewPartitionReassignment>> plan,
                AlterPartitionReassignmentsOptions options);
    }

    private static Map<TopicPartition, Optional<NewPartitionReassignment>> plan() {
        return Map.of(new TopicPartition("orders", 0),
                Optional.of(new NewPartitionReassignment(List.of(1, 2, 3))));
    }

    private static AlterPartitionReassignmentsOptions opts() {
        return new AlterPartitionReassignmentsOptions().timeoutMs(120_000);
    }

    public AlterPartitionReassignmentsResult alterA(Admin admin) {
        return admin.alterPartitionReassignments(plan(), opts());
    }

    public AlterPartitionReassignmentsResult alterB(Admin admin) {
        Map<TopicPartition, Optional<NewPartitionReassignment>> cancel =
                Map.of(new TopicPartition("orders", 0), Optional.empty());
        return admin.alterPartitionReassignments(cancel, opts());
    }

    public AlterPartitionReassignmentsResult alterC(Admin admin) {
        AlterPartitionReassignmentsResult r =
                admin.alterPartitionReassignments(plan(), opts());
        return r;
    }

    public static AlterPartitionReassignmentsResult alterD(Admin admin) {
        return admin.alterPartitionReassignments(plan(), opts());
    }

    public BiFunction<Map<TopicPartition, Optional<NewPartitionReassignment>>, AlterPartitionReassignmentsOptions, AlterPartitionReassignmentsResult> mapFactory(Admin admin) {
        return admin::alterPartitionReassignments;
    }

    public OptionsReassignFactory customFactory(Admin admin) {
        return admin::alterPartitionReassignments;
    }

    public AlterPartitionReassignmentsResult useLocalFactory(Admin admin) {
        OptionsReassignFactory factory = admin::alterPartitionReassignments;
        return factory.apply(plan(), opts());
    }

    public Stream<AlterPartitionReassignmentsResult> alterAll(List<Admin> admins) {
        return admins.stream().map(a -> a.alterPartitionReassignments(plan(), opts()));
    }

    public static void main(String[] args) {
        new GoodAdminAlterPartitionReassignmentsNoOptions();
    }
}
