package io.conductor.kafkalinter.rules.security;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Project-scoped rule. Fires when {@code aws.access.key.id} or
 * {@code aws.secret.access.key} is set in a .properties file to a
 * non-empty literal value (i.e. a value that does NOT contain a
 * {@code ${...}} ConfigProvider placeholder).
 *
 * <p>Long-lived IAM credentials in version control are the single largest
 * source of AWS-account-takeover incidents. The Confluent S3 / Kinesis /
 * DynamoDB connectors accept these keys as fallback when no IAM role
 * chain is available, but the correct path is the EC2/EKS/ECS IAM
 * instance-role chain (auto-rotated via STS) or a KIP-297 ConfigProvider
 * ({@code ${file:...}}, {@code ${vault:...}}, {@code ${env:...}}).
 *
 * <p>Emits one violation per offending key per file (so a file that sets
 * both {@code aws.access.key.id} and {@code aws.secret.access.key} to
 * literals produces two violations).
 */
public final class CredAwsCredentialLiteralRule implements ProjectScopedRule {

    private final Severity severity;

    public CredAwsCredentialLiteralRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CRED_AWS_CREDENTIAL_LITERAL;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            checkKey(out, ctx, e.getKey(), p, KafkaTypes.AWS_ACCESS_KEY_ID_KEY,
                    "aws.access.key.id");
            checkKey(out, ctx, e.getKey(), p, KafkaTypes.AWS_SECRET_ACCESS_KEY_KEY,
                    "aws.secret.access.key");
        }
        return out;
    }

    private void checkKey(List<Violation> out, ProjectContext ctx, Path file,
                          Properties p, String key, String displayKey) {
        String raw = p.getProperty(key);
        if (raw == null) return;
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) return;
        if (trimmed.contains("${")) return;

        out.add(new Violation(
                RuleId.CRED_AWS_CREDENTIAL_LITERAL, severity,
                ctx.relativize(file), "key:" + displayKey, 0,
                displayKey + " is set to a literal value — long-lived IAM "
                        + "credential shipped in a .properties file. Long-lived "
                        + "AWS access keys in version control are the single "
                        + "largest source of AWS-account-takeover incidents: "
                        + "(1) GitHub Secret Scanning recognises `AKIA[0-9A-Z]"
                        + "{16}` as an access-key-ID format and may auto-"
                        + "quarantine the key within minutes; (2) gitleaks / "
                        + "trufflehog block PR merges on the same pattern; (3) "
                        + "the credential is baked into every container image "
                        + "layer the .properties file ships in (any ECR pull "
                        + "extracts it); (4) the credential is readable by "
                        + "anyone with `kubectl exec` on the connect-worker "
                        + "pod; (5) once leaked, automated scrapers weaponise "
                        + "the key within MINUTES (crypto-mining instances, "
                        + "S3 exfiltration, VPC pivots); (6) rotation requires "
                        + "redeploying every consumer of the credential — "
                        + "hours-to-days of operational work that gets "
                        + "skipped. Fix (preferred): remove the key from "
                        + "this .properties file and rely on the EC2/EKS/ECS "
                        + "IAM role chain (DefaultAWSCredentialsProviderChain "
                        + "auto-resolves it at startup, STS auto-rotates "
                        + "every ~6 hours, no operator action). Fix "
                        + "(fallback, for environments without instance "
                        + "roles): inject via a KIP-297 ConfigProvider, e.g. "
                        + "`${file:/run/secrets/aws:access-key}`, "
                        + "`${vault:secret/data/aws#access_key}`, or "
                        + "`${env:AWS_ACCESS_KEY_ID}`. The .properties file "
                        + "then contains a template, not a credential. "
                        + "Placeholder syntax (containing `${`) is explicitly "
                        + "accepted by this rule; only non-empty non-"
                        + "placeholder literals are flagged."));
    }
}
