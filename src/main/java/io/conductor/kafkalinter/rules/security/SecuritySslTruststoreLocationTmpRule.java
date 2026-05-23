package io.conductor.kafkalinter.rules.security;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * Project-scoped rule. Fires when {@code ssl.truststore.location} (or
 * the Spring Boot equivalent) points into a world-writable scratch
 * directory: {@code /tmp}, {@code /var/tmp}, or {@code /dev/shm}.
 *
 * <p>The truststore is the client's CA bundle — the root-of-trust for
 * deciding whether a broker certificate is genuine. An attacker who
 * can WRITE to the truststore file replaces the CA bundle with their
 * own self-signed CA; the next TLS handshake validates an attacker-
 * controlled broker certificate against the attacker's CA, succeeds,
 * and the client streams credentials + records to the wrong endpoint.
 * Full MITM with no cert-validation error to trip on.
 *
 * <p>WARNING severity. Severity escalates to ERROR via pom-level
 * override on multi-tenant infrastructure.
 */
public final class SecuritySslTruststoreLocationTmpRule implements ProjectScopedRule {

    private static final String PLAIN_KEY = "ssl.truststore.location";
    private static final String SPRING_KEY = "spring.kafka.properties.ssl.truststore.location";

    private final Severity severity;

    public SecuritySslTruststoreLocationTmpRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SECURITY_SSL_TRUSTSTORE_LOCATION_TMP;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            checkKey(out, ctx, e.getKey(), p, PLAIN_KEY);
            checkKey(out, ctx, e.getKey(), p, SPRING_KEY);
        }
        return out;
    }

    private void checkKey(List<Violation> out, ProjectContext ctx, Path file,
                          Properties p, String key) {
        String value = p.getProperty(key);
        if (value == null) return;
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        if (normalized.isEmpty()) return;
        if (!isTmpPath(normalized)) return;
        out.add(new Violation(
                RuleId.SECURITY_SSL_TRUSTSTORE_LOCATION_TMP, severity,
                ctx.relativize(file), "key:" + key, 0,
                key + "=" + value.trim() + " — the Kafka client's "
                        + "truststore (the CA bundle the client uses to "
                        + "validate broker certificates during the TLS "
                        + "handshake) is stored in a world-writable "
                        + "shared scratch directory. Unlike the keystore "
                        + "(which leaks the *client's* identity if read), "
                        + "the truststore decides *which broker certs the "
                        + "client trusts* — overwriting it is a complete "
                        + "TLS bypass. The threat model is qualitatively "
                        + "worse than the keystore-in-tmp case. The "
                        + "Linux default mode on `/tmp` is `1777` "
                        + "(drwxrwxrwt): every other process on the host "
                        + "with write permission to the directory can "
                        + "create new files OR truncate-and-rewrite "
                        + "existing files owned by other users (the "
                        + "sticky bit only prevents `unlink` of someone "
                        + "else's file — not `open(..., O_WRONLY | "
                        + "O_TRUNC)` on the same path if the existing "
                        + "file is `chmod 644` or `chmod 666`). Specific "
                        + "failure modes: (1) **Full MITM via CA-bundle "
                        + "replacement.** An attacker who can write to "
                        + "`/tmp/kafka.truststore.p12` replaces it with "
                        + "a truststore containing their own self-signed "
                        + "CA. The next TLS handshake the Kafka client "
                        + "performs (broker reconnect, planned cert "
                        + "rotation, JVM restart, dynamic broker discovery "
                        + "via DNS update) loads the malicious truststore "
                        + "and validates whatever broker cert the attacker "
                        + "presents — the attacker's CA signed it, so the "
                        + "chain validates. The attacker now sits as a "
                        + "TLS-terminating proxy between the client and "
                        + "the real broker (or runs a fake broker "
                        + "entirely): they see every plaintext produce "
                        + "request, every plaintext fetch response, every "
                        + "SASL credential the client sends (PLAIN "
                        + "passwords, SCRAM client-first messages with "
                        + "the username, OAUTHBEARER access tokens). They "
                        + "replay the captured SASL credentials against "
                        + "the real broker from another network position "
                        + "to maintain access independently of the MITM "
                        + "proxy. (2) **No TLS-error symptom.** Cert "
                        + "validation PASSES because the truststore is "
                        + "the attacker's. The client logs no SSL "
                        + "handshake error, no hostname-verification "
                        + "failure, no cert-chain rejection. The broker "
                        + "doesn't see the connection at all (it goes to "
                        + "the MITM proxy). The network layer sees a "
                        + "successful TCP+TLS connection. The only "
                        + "detection signal is anomaly detection on the "
                        + "broker side (broker not seeing the expected "
                        + "client) or the network layer (connection going "
                        + "to an unexpected destination IP) — and most "
                        + "ops teams don't alert on either. (3) **DNS "
                        + "hijack amplification.** Even with the "
                        + "compromised truststore, the attacker still "
                        + "needs to route the client's connection to "
                        + "their MITM proxy. This is normally HARD — the "
                        + "client connects to `bootstrap.servers`, a "
                        + "specific DNS name. With a /tmp-writable "
                        + "truststore, the threat is amplified by ANY "
                        + "co-located process that can also modify "
                        + "`/etc/hosts` (CAP_NET_ADMIN, container "
                        + "escape, kubelet bug) or that controls a "
                        + "DNS resolver the JVM uses (`-Dsun.net.spi."
                        + "nameservice.provider` shenanigans, "
                        + "`getaddrinfo()` shimming via LD_PRELOAD in "
                        + "shared-pod scenarios). The DNS hijack alone "
                        + "would normally trip cert validation; combined "
                        + "with the truststore overwrite, it doesn't. "
                        + "(4) **Metadata leakage even with `chmod 644`.** "
                        + "The truststore CONTENT is public (it contains "
                        + "only CA certs, no private material) so `chmod "
                        + "644` is intentionally safe for read access. "
                        + "The threat is WRITE access. The standard "
                        + "`umask 0022` produces files with mode `644` "
                        + "in `/tmp`, which means the OWNER can write — "
                        + "and on a host where the JVM runs as a "
                        + "shared service-account user, every other "
                        + "process running as the same user can rewrite "
                        + "the file. On container hosts where the "
                        + "container runs as UID 1000 (a common default) "
                        + "and the host has another container also "
                        + "running as UID 1000, both share write access "
                        + "to the same UID-owned /tmp files. (5) **Tmpfs "
                        + "persistence across pod restarts.** On a "
                        + "Kubernetes node with `EmptyDir { medium: "
                        + "Memory }` or a host-mounted tmpfs at `/tmp`, "
                        + "an attacker who briefly compromises the "
                        + "Kafka pod and rewrites the truststore "
                        + "achieves persistence across the legitimate "
                        + "pod's restart: the next pod scheduled to the "
                        + "node sees the malicious truststore on its "
                        + "tmpfs and loads it. The cert-rotation "
                        + "pipeline (init container fetching from "
                        + "Vault, writing to `/tmp`) overwrites the "
                        + "malicious truststore with the next planned "
                        + "rotation — but the window between compromise "
                        + "and rotation can be days. Specific scenarios "
                        + "where this rule fires: (i) **Apache Kafka "
                        + "quickstart copy-paste.** The official "
                        + "quickstart shows `ssl.truststore.location="
                        + "/tmp/kafka.client.truststore.jks` next to "
                        + "the keystore example. The pair propagates "
                        + "into team templates together. (ii) **Init "
                        + "container fetches CA bundle from Vault and "
                        + "drops it in `/tmp/`.** A Vault Agent sidecar "
                        + "or init container fetches the corporate CA "
                        + "bundle from `vault read pki/ca_bundle` and "
                        + "writes it as a PKCS12 to `/tmp/kafka."
                        + "truststore.p12`. The fix is to write into a "
                        + "per-pod EmptyDir at `/var/run/secrets/kafka/` "
                        + "instead. (iii) **Bind-mounted host "
                        + "truststore.** A team mounts the host's CA "
                        + "bundle (`/etc/ssl/certs/ca-bundle.crt`) "
                        + "into the container at `/tmp/ca-bundle.crt` "
                        + "for convenience; the path propagates into "
                        + "production configs. (iv) **Dev shell script "
                        + "survival.** `local-dev.sh` exports "
                        + "`KAFKA_TRUSTSTORE=/tmp/dev-truststore.jks`; "
                        + "the script gets templated into a Helm chart; "
                        + "production runs with the dev path. (v) "
                        + "**Spring Boot YAML.** "
                        + "`spring.kafka.properties.ssl.truststore."
                        + "location=/tmp/truststore.p12` in a "
                        + "tutorial-derived `application.yml` reaches "
                        + "prod via a Helm chart. (vi) **CI-runner "
                        + "test config leak.** Test suites use "
                        + "`/tmp/test-truststore.jks` for the test "
                        + "Kafka container; the path leaks into a "
                        + "production config via a shared "
                        + "`application-test.yml` that overrides "
                        + "`application.yml`. Fix: move the truststore "
                        + "to a path with proper file-system "
                        + "permissions and ownership where ONLY a "
                        + "privileged user can write: (1) For "
                        + "Kubernetes: mount a Secret (or ConfigMap "
                        + "for public CA bundles) at "
                        + "`/var/run/secrets/kafka/truststore.p12` "
                        + "with `defaultMode: 0444` (world-readable, "
                        + "no-one-writable — the Secret is "
                        + "immutable on mount). (2) For bare-metal: "
                        + "write the truststore to "
                        + "`/etc/kafka/ssl/truststore.p12` owned by "
                        + "`root:root` with `chmod 644` — the JVM "
                        + "reads it, no non-root process can rewrite "
                        + "it. (3) For Vault Agent: use the Agent's "
                        + "templating to write to "
                        + "`/run/vault/kafka/truststore.p12` in a "
                        + "tmpfs mount that is per-pod and owned by "
                        + "the service account, not the world-"
                        + "writable `/tmp`. (4) For PEM-based "
                        + "workflows: prefer `ssl.truststore.type="
                        + "PEM` (KIP-651, Kafka 2.7+) and point at a "
                        + "ConfigMap-mounted CA bundle. Sibling rules: "
                        + "[[security-ssl-keystore-location-tmp]] is "
                        + "the keystore equivalent (private-key "
                        + "leak — attacker becomes the client; this "
                        + "rule's attack is MITM — attacker "
                        + "intercepts the client); "
                        + "[[security-ssl-endpoint-identification-"
                        + "algorithm-empty]] catches hostname-"
                        + "verification disabling that compounds the "
                        + "truststore-overwrite attack; "
                        + "[[security-ssl-protocol-legacy]] catches "
                        + "legacy-TLS-version pinning."));
    }

    private static boolean isTmpPath(String lowerNormalized) {
        return lowerNormalized.startsWith("/tmp/")
                || lowerNormalized.equals("/tmp")
                || lowerNormalized.startsWith("/var/tmp/")
                || lowerNormalized.equals("/var/tmp")
                || lowerNormalized.startsWith("/dev/shm/")
                || lowerNormalized.equals("/dev/shm");
    }
}
