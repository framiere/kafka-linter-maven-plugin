package sample;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateAclsOptions;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

/**
 * RULE: ADMIN_CREATE_ACLS_NO_OPTIONS.
 *
 * Fires when {@code Admin.createAcls(Collection<AclBinding>)} is called
 * without a {@code CreateAclsOptions} argument. ACL writes are SECURITY
 * MUTATIONS — under default ~30 s timeout, large batches against a busy
 * controller may have some bindings persisted and others not. The aggregated
 * {@code CreateAclsResult.all()} future reports the FIRST failure and masks
 * the per-binding success/fail state.
 *
 * <p>Failure mode: a tenant-onboarding tool grants read/write/describe to a
 * new principal across 30 topics (90 AclBindings, 3 per topic). Dev passes in
 * 2 s. In production with 50K existing ACLs and a contended controller, the
 * call hits ~30 s at binding 67/90; the catch block logs 'ACL creation
 * failed: TimeoutException' and retries the FULL batch. The controller
 * already persisted bindings 1-67; the retry adds 67 duplicates plus the
 * remaining 23. The audit log shows 90 created instead of 90 expected,
 * confusing the compliance review six months later. With
 * {@code timeoutMs(120_000)} AND per-binding inspection via
 * {@code result.values()}, the caller knows exactly which bindings
 * persisted.
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code (Ljava/util/Collection;)Lorg/apache/kafka/clients/admin/CreateAclsResult;}
 *   <li>With options: {@code (Ljava/util/Collection;Lorg/apache/kafka/clients/admin/CreateAclsOptions;)Lorg/apache/kafka/clients/admin/CreateAclsResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/CreateAclsOptions;}
 * in the descriptor.
 */
public final class BadAdminCreateAclsNoOptions {

    private static List<AclBinding> bindings() {
        ResourcePattern paymentsTopic = new ResourcePattern(
                ResourceType.TOPIC, "payments.events", PatternType.LITERAL);
        AccessControlEntry read = new AccessControlEntry(
                "User:audit-publisher", "*", AclOperation.READ, AclPermissionType.ALLOW);
        AccessControlEntry write = new AccessControlEntry(
                "User:audit-publisher", "*", AclOperation.WRITE, AclPermissionType.ALLOW);
        return List.of(new AclBinding(paymentsTopic, read), new AclBinding(paymentsTopic, write));
    }

    /** Anti-pattern: no CreateAclsOptions on Admin (INVOKEINTERFACE) — FIRES. */
    public CreateAclsResult adminNoOptions(Admin admin) {
        return admin.createAcls(bindings()); // FIRES — no options
    }

    /** Anti-pattern: no CreateAclsOptions on AdminClient (INVOKEVIRTUAL) — FIRES. */
    public CreateAclsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.createAcls(bindings()); // FIRES — no options
        }
    }

    /** Control: CreateAclsOptions with generous timeout — must NOT fire. */
    public CreateAclsResult adminWithOptions(Admin admin) {
        CreateAclsOptions opts = new CreateAclsOptions().timeoutMs(120_000);
        return admin.createAcls(bindings(), opts);
    }
}
