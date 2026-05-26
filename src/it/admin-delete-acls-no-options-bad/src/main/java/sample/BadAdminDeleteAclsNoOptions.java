package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: ADMIN_DELETE_ACLS_NO_OPTIONS — must fire EXACTLY 8
 * times across this class (one per method below).
 *
 * <p>Exercises the unsafe overload descriptor of {@link
 * Admin#deleteAcls(Collection)} — the 1-arg form taking only
 * a {@code Collection<AclBindingFilter>}. Each method
 * exercises one capture shape — direct {@code
 * INVOKEINTERFACE} or {@code INVOKEDYNAMIC}
 * method-reference capture.
 */
public final class BadAdminDeleteAclsNoOptions {

    /** 1-arg SAM whose erased descriptor matches
     *  {@code Admin.deleteAcls(Collection) DeleteAclsResult}
     *  when bound to an Admin receiver. */
    @FunctionalInterface
    interface AclDeleteFactory {
        DeleteAclsResult apply(Collection<AclBindingFilter> filters);
    }

    private static Collection<AclBindingFilter> filters() {
        return List.of(new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.TOPIC, "orders", PatternType.LITERAL),
                new AccessControlEntryFilter("User:svc", "*", AclOperation.READ, AclPermissionType.ALLOW)));
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overload =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  Admin.deleteAcls(Collection<AclBindingFilter>). */
    public DeleteAclsResult deleteA(Admin admin) {
        return admin.deleteAcls(filters());
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the same
     *  overload with an inline filter list. */
    public DeleteAclsResult deleteB(Admin admin) {
        Collection<AclBindingFilter> f = filters();
        return admin.deleteAcls(f);
    }

    /** MUST FIRE — direct INVOKEINTERFACE with intermediate
     *  local-variable binding. */
    public DeleteAclsResult deleteC(Admin admin) {
        DeleteAclsResult r = admin.deleteAcls(filters());
        return r;
    }

    /** MUST FIRE — direct INVOKEINTERFACE inside a static
     *  helper. */
    public static DeleteAclsResult deleteD(Admin admin) {
        return admin.deleteAcls(filters());
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code admin::deleteAcls} as a
     *  bound-receiver method reference. Indy implMethod
     *  handle descriptor matches Admin.deleteAcls(Collection)
     *  DeleteAclsResult — the 1-arg overload. */
    public Function<Collection<AclBindingFilter>, DeleteAclsResult> collectionFactory(Admin admin) {
        return admin::deleteAcls;
    }

    /** MUST FIRE — same method reference bound to a custom
     *  1-arg SAM whose erased descriptor matches the unsafe
     *  overload. */
    public AclDeleteFactory customFactory(Admin admin) {
        return admin::deleteAcls;
    }

    /** MUST FIRE — local SAM binding via bound-receiver
     *  method reference, applied inside the same method. */
    public DeleteAclsResult useLocalFactory(Admin admin) {
        AclDeleteFactory factory = admin::deleteAcls;
        return factory.apply(filters());
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code a.deleteAcls(filters())} per element. The
     *  synthetic {@code lambda$deleteAll$0} carries a direct
     *  INVOKEINTERFACE on the 1-arg overload. */
    public Stream<DeleteAclsResult> deleteAll(List<Admin> admins) {
        return admins.stream().map(a -> a.deleteAcls(filters()));
    }

    public static void main(String[] args) {
        new BadAdminDeleteAclsNoOptions();
    }
}
