package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: ADMIN_ALTER_PARTITION_REASSIGNMENTS_NO_OPTIONS —
 * must fire EXACTLY 8 times across this class (one per
 * method below).
 *
 * <p>Exercises the unsafe overload descriptor of {@link
 * Admin#alterPartitionReassignments(Map)} — the 1-arg form
 * taking only a {@code Map<TopicPartition,
 * Optional<NewPartitionReassignment>>}. Each method
 * exercises one capture shape — direct {@code
 * INVOKEINTERFACE} or {@code INVOKEDYNAMIC}
 * method-reference capture.
 */
public final class BadAdminAlterPartitionReassignmentsNoOptions {

    /** 1-arg SAM whose erased descriptor matches
     *  {@code Admin.alterPartitionReassignments(Map)
     *  AlterPartitionReassignmentsResult} when bound to an
     *  Admin receiver. */
    @FunctionalInterface
    interface MapReassignFactory {
        AlterPartitionReassignmentsResult apply(
                Map<TopicPartition, Optional<NewPartitionReassignment>> plan);
    }

    private static Map<TopicPartition, Optional<NewPartitionReassignment>> plan() {
        return Map.of(new TopicPartition("orders", 0),
                Optional.of(new NewPartitionReassignment(List.of(1, 2, 3))));
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overload =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  Admin.alterPartitionReassignments(Map). */
    public AlterPartitionReassignmentsResult alterA(Admin admin) {
        return admin.alterPartitionReassignments(plan());
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  Admin.alterPartitionReassignments(Map) with an
     *  empty-Optional cancel plan. */
    public AlterPartitionReassignmentsResult alterB(Admin admin) {
        Map<TopicPartition, Optional<NewPartitionReassignment>> cancel =
                Map.of(new TopicPartition("orders", 0), Optional.empty());
        return admin.alterPartitionReassignments(cancel);
    }

    /** MUST FIRE — direct INVOKEINTERFACE with intermediate
     *  local-variable binding. */
    public AlterPartitionReassignmentsResult alterC(Admin admin) {
        AlterPartitionReassignmentsResult r = admin.alterPartitionReassignments(plan());
        return r;
    }

    /** MUST FIRE — direct INVOKEINTERFACE inside a static
     *  helper. */
    public static AlterPartitionReassignmentsResult alterD(Admin admin) {
        return admin.alterPartitionReassignments(plan());
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code admin::alterPartitionReassignments}
     *  as a bound-receiver method reference. Indy
     *  implMethod handle descriptor matches
     *  Admin.alterPartitionReassignments(Map)
     *  AlterPartitionReassignmentsResult — the 1-arg
     *  overload. */
    public Function<Map<TopicPartition, Optional<NewPartitionReassignment>>, AlterPartitionReassignmentsResult> mapFactory(Admin admin) {
        return admin::alterPartitionReassignments;
    }

    /** MUST FIRE — same method reference bound to a custom
     *  1-arg SAM whose erased descriptor matches the unsafe
     *  overload. */
    public MapReassignFactory customFactory(Admin admin) {
        return admin::alterPartitionReassignments;
    }

    /** MUST FIRE — local SAM binding via bound-receiver
     *  method reference, applied inside the same method. */
    public AlterPartitionReassignmentsResult useLocalFactory(Admin admin) {
        MapReassignFactory factory = admin::alterPartitionReassignments;
        return factory.apply(plan());
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code a.alterPartitionReassignments(plan())} per
     *  element. The synthetic {@code lambda$alterAll$0}
     *  carries a direct INVOKEINTERFACE on the 1-arg
     *  overload. */
    public Stream<AlterPartitionReassignmentsResult> alterAll(List<Admin> admins) {
        return admins.stream().map(a -> a.alterPartitionReassignments(plan()));
    }

    public static void main(String[] args) {
        new BadAdminAlterPartitionReassignmentsNoOptions();
    }
}
