package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterUserScramCredentialsOptions;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialAlteration;
import org.apache.kafka.clients.admin.UserScramCredentialDeletion;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;

import java.util.List;

/**
 * RULE: ADMIN_ALTER_USER_SCRAM_CREDENTIALS_NO_OPTIONS.
 *
 * Fires on any call to {@code Admin.alterUserScramCredentials(List<UserScramCredentialAlteration>)}
 * whose descriptor lacks {@code AlterUserScramCredentialsOptions}. The no-options
 * form inherits the AdminClient default {@code request.timeout.ms} (~30 s) and
 * offers no caller-visible knob. The options class has NO validateOnly /
 * NO dry-run — credential rotation is destructive, ordered, and not transactional.
 * On partial-failure mid-batch some users are rotated/deleted and others not,
 * leaving the cluster's authentication state half-mutated.
 *
 * Mix of receiver types deliberately exercises both INVOKEINTERFACE (against
 * {@code Admin}) and INVOKEVIRTUAL (against {@code AdminClient}).
 */
public final class BadAdminAlterUserScramCredentialsNoOptions {

    private final Admin admin;

    public BadAdminAlterUserScramCredentialsNoOptions(Admin admin) {
        this.admin = admin;
    }

    /** Anti-pattern: no-options call on the Admin interface — INVOKEINTERFACE alterUserScramCredentials(List). */
    public void rotateAll(List<UserScramCredentialAlteration> alterations) {
        this.admin.alterUserScramCredentials(alterations); // FIRES — no AlterUserScramCredentialsOptions
    }

    /** Anti-pattern: parameter typed as the abstract AdminClient — INVOKEVIRTUAL alterUserScramCredentials(List). */
    public static void rotateAllConcrete(AdminClient a, List<UserScramCredentialAlteration> alterations) {
        a.alterUserScramCredentials(alterations); // FIRES — no AlterUserScramCredentialsOptions
    }

    /** Anti-pattern: inline-built upsertion + deletion batch — still no options. */
    public void rotateAlice(byte[] saltedPasswordBytes) {
        UserScramCredentialUpsertion newPass = new UserScramCredentialUpsertion(
                "alice", new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 4096), saltedPasswordBytes);
        UserScramCredentialDeletion deleteSha256 = new UserScramCredentialDeletion(
                "alice", ScramMechanism.SCRAM_SHA_256);
        this.admin.alterUserScramCredentials(List.of(newPass, deleteSha256)); // FIRES — no AlterUserScramCredentialsOptions
    }

    /** Control: explicit AlterUserScramCredentialsOptions with long timeout — must NOT fire. */
    public void rotateAllWithOptions(List<UserScramCredentialAlteration> alterations) {
        this.admin.alterUserScramCredentials(alterations,
                new AlterUserScramCredentialsOptions().timeoutMs(120_000));
    }
}
