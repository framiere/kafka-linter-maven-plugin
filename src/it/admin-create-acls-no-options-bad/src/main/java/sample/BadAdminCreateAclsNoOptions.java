package sample;

import org.apache.kafka.clients.admin.Admin;
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
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * RULE: ADMIN_CREATE_ACLS_NO_OPTIONS — must fire EXACTLY 8
 * times across this class (one per method below).
 *
 * <p>Exercises the unsafe overload descriptor of {@link
 * Admin#createAcls(Collection)} — the 1-arg form taking
 * only a {@code Collection<AclBinding>}. Each method
 * exercises one capture shape — direct {@code
 * INVOKEINTERFACE} or {@code INVOKEDYNAMIC}
 * method-reference capture.
 */
public final class BadAdminCreateAclsNoOptions {

    /** 1-arg SAM whose erased descriptor matches
     *  {@code Admin.createAcls(Collection) CreateAclsResult}
     *  when bound to an Admin receiver. */
    @FunctionalInterface
    interface AclCreateFactory {
        CreateAclsResult apply(Collection<AclBinding> bindings);
    }

    private static Collection<AclBinding> bindings() {
        return List.of(new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL),
                new AccessControlEntry("User:svc", "*", AclOperation.READ, AclPermissionType.ALLOW)));
    }

    // ===== Direct INVOKEINTERFACE on the unsafe overload =====

    /** MUST FIRE — direct INVOKEINTERFACE on
     *  Admin.createAcls(Collection<AclBinding>). */
    public CreateAclsResult createA(Admin admin) {
        return admin.createAcls(bindings());
    }

    /** MUST FIRE — direct INVOKEINTERFACE on the same
     *  overload with an inline binding list. */
    public CreateAclsResult createB(Admin admin) {
        Collection<AclBinding> b = bindings();
        return admin.createAcls(b);
    }

    /** MUST FIRE — direct INVOKEINTERFACE with intermediate
     *  local-variable binding. */
    public CreateAclsResult createC(Admin admin) {
        CreateAclsResult r = admin.createAcls(bindings());
        return r;
    }

    /** MUST FIRE — direct INVOKEINTERFACE inside a static
     *  helper. */
    public static CreateAclsResult createD(Admin admin) {
        return admin.createAcls(bindings());
    }

    // ===== INVOKEDYNAMIC method-reference captures =====

    /** MUST FIRE — {@code admin::createAcls} as a
     *  bound-receiver method reference. Indy implMethod
     *  handle descriptor matches Admin.createAcls(Collection)
     *  CreateAclsResult — the 1-arg overload. */
    public Function<Collection<AclBinding>, CreateAclsResult> collectionFactory(Admin admin) {
        return admin::createAcls;
    }

    /** MUST FIRE — same method reference bound to a custom
     *  1-arg SAM whose erased descriptor matches the unsafe
     *  overload. */
    public AclCreateFactory customFactory(Admin admin) {
        return admin::createAcls;
    }

    /** MUST FIRE — local SAM binding via bound-receiver
     *  method reference, applied inside the same method. */
    public CreateAclsResult useLocalFactory(Admin admin) {
        AclCreateFactory factory = admin::createAcls;
        return factory.apply(bindings());
    }

    /** MUST FIRE — explicit lambda body that invokes
     *  {@code a.createAcls(bindings())} per element. The
     *  synthetic {@code lambda$createAll$0} carries a direct
     *  INVOKEINTERFACE on the 1-arg overload. */
    public Stream<CreateAclsResult> createAll(List<Admin> admins) {
        return admins.stream().map(a -> a.createAcls(bindings()));
    }

    public static void main(String[] args) {
        new BadAdminCreateAclsNoOptions();
    }
}
