package sample;

import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeAclsOptions;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.ResourcePatternFilter;

/**
 * RULE: ADMIN_DESCRIBE_ACLS_NO_OPTIONS.
 *
 * Fires when {@code Admin.describeAcls(AclBindingFilter)} is called without
 * a {@code DescribeAclsOptions} argument. Inherits the default ~30 s
 * AdminClient timeout, which compounds two scaling factors:
 *
 * <ol>
 *   <li>ACL store size — multi-tenant clusters often have 50K+ bindings.
 *   <li>Filter breadth — {@code AclBindingFilter.ANY} matches every binding;
 *       narrow filters short-circuit at the broker.
 * </ol>
 *
 * <p>Failure mode: a compliance script exports every ACL to CSV via
 * {@code admin.describeAcls(AclBindingFilter.ANY).values().get()}. Staging
 * (200 bindings) completes in 800 ms. Production (45K bindings) hits the
 * 30 s timeout; the script reports 'ACL audit failed' and the security
 * review is delayed two days. Fix: {@code timeoutMs(120_000)} AND iterate
 * principal-by-principal rather than scanning the whole store in one call.
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code (Lorg/apache/kafka/common/acl/AclBindingFilter;)Lorg/apache/kafka/clients/admin/DescribeAclsResult;}
 *   <li>With options: {@code (Lorg/apache/kafka/common/acl/AclBindingFilter;Lorg/apache/kafka/clients/admin/DescribeAclsOptions;)Lorg/apache/kafka/clients/admin/DescribeAclsResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/DescribeAclsOptions;}
 * in the descriptor.
 */
public final class BadAdminDescribeAclsNoOptions {

    private static AclBindingFilter principalFilter() {
        return new AclBindingFilter(
                ResourcePatternFilter.ANY,
                new AccessControlEntryFilter(
                        "User:audit-publisher", null, AclOperation.ANY, AclPermissionType.ANY));
    }

    /** Anti-pattern: no DescribeAclsOptions on Admin (INVOKEINTERFACE) — FIRES. */
    public DescribeAclsResult adminNoOptions(Admin admin) {
        return admin.describeAcls(principalFilter()); // FIRES — no options
    }

    /** Anti-pattern: no DescribeAclsOptions on AdminClient (INVOKEVIRTUAL) — FIRES. */
    public DescribeAclsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.describeAcls(principalFilter()); // FIRES — no options
        }
    }

    /** Control: DescribeAclsOptions with generous timeout — must NOT fire. */
    public DescribeAclsResult adminWithOptions(Admin admin) {
        DescribeAclsOptions opts = new DescribeAclsOptions().timeoutMs(120_000);
        return admin.describeAcls(principalFilter(), opts);
    }
}
