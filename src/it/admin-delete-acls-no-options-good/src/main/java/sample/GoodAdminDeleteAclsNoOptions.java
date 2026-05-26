package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteAclsOptions;
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
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call passes a {@link
 * DeleteAclsOptions} so the descriptor contains
 * {@code org/apache/kafka/clients/admin/DeleteAclsOptions}
 * and the predicate rejects it.
 */
public final class GoodAdminDeleteAclsNoOptions {

    @FunctionalInterface
    interface OptionsAclDeleteFactory {
        DeleteAclsResult apply(Collection<AclBindingFilter> filters, DeleteAclsOptions options);
    }

    private static Collection<AclBindingFilter> filters() {
        return List.of(new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.TOPIC, "orders", PatternType.LITERAL),
                new AccessControlEntryFilter("User:svc", "*", AclOperation.READ, AclPermissionType.ALLOW)));
    }

    private static DeleteAclsOptions opts() {
        return new DeleteAclsOptions().timeoutMs(120_000);
    }

    public DeleteAclsResult deleteA(Admin admin) {
        return admin.deleteAcls(filters(), opts());
    }

    public DeleteAclsResult deleteB(Admin admin) {
        Collection<AclBindingFilter> f = filters();
        return admin.deleteAcls(f, opts());
    }

    public DeleteAclsResult deleteC(Admin admin) {
        DeleteAclsResult r = admin.deleteAcls(filters(), opts());
        return r;
    }

    public static DeleteAclsResult deleteD(Admin admin) {
        return admin.deleteAcls(filters(), opts());
    }

    public BiFunction<Collection<AclBindingFilter>, DeleteAclsOptions, DeleteAclsResult> collectionFactory(Admin admin) {
        return admin::deleteAcls;
    }

    public OptionsAclDeleteFactory customFactory(Admin admin) {
        return admin::deleteAcls;
    }

    public DeleteAclsResult useLocalFactory(Admin admin) {
        OptionsAclDeleteFactory factory = admin::deleteAcls;
        return factory.apply(filters(), opts());
    }

    public Stream<DeleteAclsResult> deleteAll(List<Admin> admins) {
        return admins.stream().map(a -> a.deleteAcls(filters(), opts()));
    }

    public static void main(String[] args) {
        new GoodAdminDeleteAclsNoOptions();
    }
}
