package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * RULE: ADMIN_ELECT_LEADERS_NO_OPTIONS — must fire EXACTLY 8
 * times across this class (one per method below).
 *
 * <p>Exercises the unsafe overload descriptor of {@link
 * Admin#electLeaders(ElectionType, Set)} — the 2-arg form
 * taking only an {@code ElectionType} and a {@code Set<
 * TopicPartition>}. Each method exercises one capture shape
 * — direct {@code INVOKEINTERFACE} or {@code INVOKEDYNAMIC}
 * method-reference capture.
 */
public final class BadAdminElectLeadersNoOptions {

    /** 2-arg SAM whose erased descriptor matches
     *  {@code Admin.electLeaders(ElectionType, Set)
     *  ElectLeadersResult} when bound to an Admin
     *  receiver. */
    @FunctionalInterface
    interface ElectLeadersFactory {
        ElectLeadersResult apply(ElectionType type, Set<TopicPartition> partitions);
    }

    private static Set<TopicPartition> partitions() {
        return Set.of(new TopicPartition("orders", 0));
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overload =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  Admin.electLeaders(ElectionType, Set). */
    public ElectLeadersResult electA(Admin admin) {
        return admin.electLeaders(ElectionType.PREFERRED, partitions());
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the UNCLEAN
     *  variant. */
    public ElectLeadersResult electB(Admin admin) {
        return admin.electLeaders(ElectionType.UNCLEAN, partitions());
    }

    /** MUST FIRE — direct INVOKEINTERFACE with intermediate
     *  local-variable binding. */
    public ElectLeadersResult electC(Admin admin) {
        ElectLeadersResult r = admin.electLeaders(ElectionType.PREFERRED, partitions());
        return r;
    }

    /** MUST FIRE — direct INVOKEINTERFACE inside a static
     *  helper. */
    public static ElectLeadersResult electD(Admin admin) {
        return admin.electLeaders(ElectionType.PREFERRED, partitions());
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code admin::electLeaders} as a
     *  bound-receiver method reference. Indy implMethod
     *  handle descriptor matches Admin.electLeaders(
     *  ElectionType, Set) ElectLeadersResult — the 2-arg
     *  overload. */
    public BiFunction<ElectionType, Set<TopicPartition>, ElectLeadersResult> biFactory(Admin admin) {
        return admin::electLeaders;
    }

    /** MUST FIRE — same method reference bound to a custom
     *  2-arg SAM whose erased descriptor matches the unsafe
     *  overload. */
    public ElectLeadersFactory customFactory(Admin admin) {
        return admin::electLeaders;
    }

    /** MUST FIRE — local SAM binding via bound-receiver
     *  method reference, applied inside the same method. */
    public ElectLeadersResult useLocalFactory(Admin admin) {
        ElectLeadersFactory factory = admin::electLeaders;
        return factory.apply(ElectionType.PREFERRED, partitions());
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code a.electLeaders(PREFERRED, partitions())} per
     *  element. The synthetic {@code lambda$electAll$0}
     *  carries a direct INVOKEINTERFACE on the 2-arg
     *  overload. */
    public Stream<ElectLeadersResult> electAll(List<Admin> admins) {
        return admins.stream().map(a -> a.electLeaders(ElectionType.PREFERRED, partitions()));
    }

    public static void main(String[] args) {
        new BadAdminElectLeadersNoOptions();
    }
}
