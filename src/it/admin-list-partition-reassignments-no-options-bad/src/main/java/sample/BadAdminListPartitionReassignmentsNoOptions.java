package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * RULE: ADMIN_LIST_PARTITION_REASSIGNMENTS_NO_OPTIONS —
 * must fire EXACTLY 8 times across this class (one per
 * method below).
 *
 * <p>Exercises the unsafe overload descriptors of
 * {@link Admin#listPartitionReassignments()} — the
 * 0-arg form and the 1-arg form taking only a {@code
 * Set<TopicPartition>}. Each method exercises one capture
 * shape — direct {@code INVOKEINTERFACE} or {@code
 * INVOKEDYNAMIC} method-reference capture.
 */
public final class BadAdminListPartitionReassignmentsNoOptions {

    /** 1-arg SAM whose erased descriptor matches
     *  {@code Admin.listPartitionReassignments(Set)
     *  ListPartitionReassignmentsResult} when bound to an
     *  Admin receiver. */
    @FunctionalInterface
    interface SetReassignFactory {
        ListPartitionReassignmentsResult apply(Set<TopicPartition> partitions);
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overloads =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  Admin.listPartitionReassignments() (0-arg). */
    public ListPartitionReassignmentsResult listA(Admin admin) {
        return admin.listPartitionReassignments();
    }

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  Admin.listPartitionReassignments(Set<TopicPartition>)
     *  — set-only, no Options. */
    public ListPartitionReassignmentsResult listB(Admin admin) {
        Set<TopicPartition> tps = Set.of(new TopicPartition("orders", 0));
        return admin.listPartitionReassignments(tps);
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the 0-arg
     *  overload with different surrounding context. */
    public ListPartitionReassignmentsResult listC(Admin admin) {
        ListPartitionReassignmentsResult r = admin.listPartitionReassignments();
        return r;
    }

    /** MUST FIRE — direct INVOKEINTERFACE inside a static
     *  helper. */
    public static ListPartitionReassignmentsResult listD(Admin admin) {
        return admin.listPartitionReassignments();
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code admin::listPartitionReassignments}
     *  as a bound-receiver method reference. Indy
     *  implMethod handle descriptor matches
     *  Admin.listPartitionReassignments()
     *  ListPartitionReassignmentsResult — the 0-arg overload. */
    public Supplier<ListPartitionReassignmentsResult> supplierFactory(Admin admin) {
        return admin::listPartitionReassignments;
    }

    /** MUST FIRE — {@code admin::listPartitionReassignments}
     *  bound to a Function<Set, Result>; indy handle
     *  descriptor matches the 1-arg Set-only overload. */
    public Function<Set<TopicPartition>, ListPartitionReassignmentsResult> setFactory(Admin admin) {
        return admin::listPartitionReassignments;
    }

    /** MUST FIRE — local SAM binding via bound-receiver
     *  method reference, applied inside the same method. */
    public ListPartitionReassignmentsResult useLocalFactory(Admin admin) {
        SetReassignFactory factory = admin::listPartitionReassignments;
        return factory.apply(Set.of(new TopicPartition("orders", 1)));
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code a.listPartitionReassignments()} per element.
     *  The synthetic {@code lambda$listAll$0} carries a
     *  direct INVOKEINTERFACE on the 0-arg overload. */
    public Stream<ListPartitionReassignmentsResult> listAll(List<Admin> admins) {
        return admins.stream().map(a -> a.listPartitionReassignments());
    }

    public static void main(String[] args) {
        new BadAdminListPartitionReassignmentsNoOptions();
    }
}
