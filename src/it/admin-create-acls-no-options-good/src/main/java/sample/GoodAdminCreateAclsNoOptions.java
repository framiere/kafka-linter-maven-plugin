package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateAclsOptions;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * MUST NOT FIRE — every call passes a {@link
 * CreateAclsOptions} so the descriptor contains
 * {@code org/apache/kafka/clients/admin/CreateAclsOptions}
 * and the predicate rejects it.
 */
public final class GoodAdminCreateAclsNoOptions {

    @FunctionalInterface
    interface OptionsAclCreateFactory {
        CreateAclsResult apply(Collection<AclBinding> bindings, CreateAclsOptions options);
    }

    private static Collection<AclBinding> bindings() {
        return List.of(new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL),
                new AccessControlEntry("User:svc", "*", AclOperation.READ, AclPermissionType.ALLOW)));
    }

    private static CreateAclsOptions opts() {
        return new CreateAclsOptions().timeoutMs(120_000);
    }

    public CreateAclsResult createA(Admin admin) {
        return admin.createAcls(bindings(), opts());
    }

    public CreateAclsResult createB(Admin admin) {
        Collection<AclBinding> b = bindings();
        return admin.createAcls(b, opts());
    }

    public CreateAclsResult createC(Admin admin) {
        CreateAclsResult r = admin.createAcls(bindings(), opts());
        return r;
    }

    public static CreateAclsResult createD(Admin admin) {
        return admin.createAcls(bindings(), opts());
    }

    public BiFunction<Collection<AclBinding>, CreateAclsOptions, CreateAclsResult> collectionFactory(Admin admin) {
        return admin::createAcls;
    }

    public OptionsAclCreateFactory customFactory(Admin admin) {
        return admin::createAcls;
    }

    public CreateAclsResult useLocalFactory(Admin admin) {
        OptionsAclCreateFactory factory = admin::createAcls;
        return factory.apply(bindings(), opts());
    }

    public Stream<CreateAclsResult> createAll(List<Admin> admins) {
        return admins.stream().map(a -> a.createAcls(bindings(), opts()));
    }

    public static void main(String[] args) {
        new GoodAdminCreateAclsNoOptions();
    }
}
