package sample;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteAclsOptions;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

/**
 * RULE: ADMIN_DELETE_ACLS_NO_OPTIONS.
 *
 * Fires when {@code Admin.deleteAcls(Collection<AclBindingFilter>)} is called
 * without a {@code DeleteAclsOptions} argument. ACL deletion is IRREVERSIBLE
 * (Kafka has no restore-deleted-ACL API). Each filter can match ZERO, ONE,
 * or MANY bindings — {@code AclBindingFilter.ANY} matches every binding in
 * the cluster. Under the default ~30 s timeout, a broad-filter call against
 * a busy controller can be partially applied, and the aggregated
 * {@code DeleteAclsResult.all()} future masks per-filter outcomes.
 *
 * <p>Failure mode: an operator's cleanup script means to delete legacy ACLs
 * for {@code User:legacy-app-*}. A review-time edit substitutes
 * {@code AclBindingFilter.ANY} (matches everything). The dev test only has 3
 * ACLs and passes. Production has 50K bindings; the 30 s timeout fires at
 * binding 18000; the operator restarts the script assuming timeout meant
 * 'nothing happened'; a second {@code ANY} filter is issued; result: 36000
 * bindings gone, every consumer/producer hit
 * {@code TOPIC_AUTHORIZATION_FAILED}, 4-hour incident to restore from a
 * 6-hour-old backup (losing 6 hours of legitimate ACL changes).
 *
 * <p>Bytecode descriptors:
 * <ul>
 *   <li>No-options: {@code (Ljava/util/Collection;)Lorg/apache/kafka/clients/admin/DeleteAclsResult;}
 *   <li>With options: {@code (Ljava/util/Collection;Lorg/apache/kafka/clients/admin/DeleteAclsOptions;)Lorg/apache/kafka/clients/admin/DeleteAclsResult;}
 * </ul>
 * The rule detects the absence of {@code Lorg/apache/kafka/clients/admin/DeleteAclsOptions;}
 * in the descriptor.
 */
public final class BadAdminDeleteAclsNoOptions {

    private static List<AclBindingFilter> filters() {
        ResourcePatternFilter legacyTopic = new ResourcePatternFilter(
                ResourceType.TOPIC, "legacy-app-events", PatternType.LITERAL);
        AccessControlEntryFilter legacyPrincipal = new AccessControlEntryFilter(
                "User:legacy-app-1", "*", AclOperation.ANY, AclPermissionType.ANY);
        return List.of(new AclBindingFilter(legacyTopic, legacyPrincipal));
    }

    /** Anti-pattern: no DeleteAclsOptions on Admin (INVOKEINTERFACE) — FIRES. */
    public DeleteAclsResult adminNoOptions(Admin admin) {
        return admin.deleteAcls(filters()); // FIRES — no options
    }

    /** Anti-pattern: no DeleteAclsOptions on AdminClient (INVOKEVIRTUAL) — FIRES. */
    public DeleteAclsResult adminClientNoOptions(Properties props) {
        try (AdminClient ac = (AdminClient) Admin.create(props)) {
            return ac.deleteAcls(filters()); // FIRES — no options
        }
    }

    /** Control: DeleteAclsOptions with generous timeout — must NOT fire. */
    public DeleteAclsResult adminWithOptions(Admin admin) {
        DeleteAclsOptions opts = new DeleteAclsOptions().timeoutMs(120_000);
        return admin.deleteAcls(filters(), opts);
    }
}
