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
 * Project-scoped rule. Fires when {@code ssl.keystore.location} (or
 * the Spring Boot equivalent) points into a world-readable scratch
 * directory: {@code /tmp}, {@code /var/tmp}, or {@code /dev/shm}.
 *
 * <p>Storing the keystore — which contains the client's private key
 * and cert chain — in a shared-tmpfs directory exposes it to any
 * other process on the host (sidecar, init container, debug shell,
 * log forwarder, attacker with code-exec). On a JVM with arbitrary-
 * code-execution potential (Java agents, JNI, log4j-style gadgets),
 * an authenticated Kafka client becomes 'whoever can land on this
 * box'.
 */
public final class SecuritySslKeystoreLocationTmpRule implements ProjectScopedRule {

    private static final String PLAIN_KEY = "ssl.keystore.location";
    private static final String SPRING_KEY = "spring.kafka.properties.ssl.keystore.location";

    private final Severity severity;

    public SecuritySslKeystoreLocationTmpRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SECURITY_SSL_KEYSTORE_LOCATION_TMP;
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
                RuleId.SECURITY_SSL_KEYSTORE_LOCATION_TMP, severity,
                ctx.relativize(file), "key:" + key, 0,
                key + "=" + value.trim() + " — the Kafka client's "
                        + "keystore file (containing the private key + "
                        + "client cert chain used for mTLS to the broker) "
                        + "is stored in a world-readable/world-writable "
                        + "shared scratch directory. The Linux default "
                        + "mode on `/tmp` is `1777` (drwxrwxrwt): the "
                        + "sticky bit prevents non-owners from `unlink`-"
                        + "ing other users' files, but does NOT prevent "
                        + "reads, writes, listings, or stats. Every "
                        + "other process on the host — sidecar "
                        + "container, init container, debug shell, log "
                        + "forwarder, attacker with code-exec via a "
                        + "Java agent / JNI / log4j-style gadget — sees "
                        + "the file. Specific failure modes: (1) "
                        + "**Private-key exfiltration.** A non-Kafka "
                        + "process on the same host reads the keystore "
                        + "file (the JVM holding the file open does NOT "
                        + "make it unreadable to other processes; Unix "
                        + "file locks are advisory, not mandatory), "
                        + "extracts the PrivateKeyEntry via "
                        + "`KeyStore.getKey(alias, password)` if the "
                        + "keystore password is also leaked (and on "
                        + "JVMs that write heap dumps to `/tmp/"
                        + "java_pid<n>.hprof`, the password often is — "
                        + "see CVE-2014-3577 and friends for the long "
                        + "history of password-in-heap bugs), and uses "
                        + "the key to authenticate to the broker as "
                        + "this client. Every ACL the legitimate "
                        + "client has, the attacker now has. (2) "
                        + "**Keystore replacement / persistence.** An "
                        + "attacker who briefly gains code-exec on the "
                        + "host (a single RCE in an unrelated process, "
                        + "a JVM deserialization bug, a CI runner that "
                        + "shares the same tmpfs) replaces `/tmp/"
                        + "kafka.keystore.jks` with their own keystore "
                        + "containing a cert that the broker's CA bundle "
                        + "trusts (or that bypasses CA validation via "
                        + "[[security-ssl-endpoint-identification-"
                        + "algorithm-empty]]). The next time the client "
                        + "reloads SSL config (broker restart, planned "
                        + "cert rotation, JVM restart), it loads the "
                        + "attacker's keystore and authenticates as "
                        + "whatever identity the attacker chose. (3) "
                        + "**Metadata leakage even with `chmod 600`.** "
                        + "Even if the keystore file itself is `chmod "
                        + "600 kafka.keystore.jks`, the surrounding "
                        + "`/tmp` directory still leaks (a) the "
                        + "*existence* of the file (an attacker doing "
                        + "`ls -la /tmp/` knows mTLS is in use and "
                        + "what filename to target); (b) every "
                        + "read/write *timestamp* (correlatable with "
                        + "broker-side auth events to fingerprint the "
                        + "client); (c) the file's *size* in bytes "
                        + "(revealing roughly how many certs are "
                        + "bundled — single cert vs full chain vs "
                        + "intermediate CA list). (4) **Container-"
                        + "tmpfs leftover certs.** On container hosts "
                        + "where `/tmp` is a tmpfs shared across pod "
                        + "restarts on the same node, the keystore "
                        + "persists across pod restarts in a way that "
                        + "operators don't expect. After a pod is "
                        + "killed, the next pod on the same node sees "
                        + "leftover certs from the previous tenant — "
                        + "and a leftover keystore is indistinguishable "
                        + "from the current one. Container debugging "
                        + "(`kubectl debug` / ephemeral debug "
                        + "containers / `crictl exec`) further "
                        + "complicates the threat model because the "
                        + "debug container shares the tmpfs. (5) "
                        + "**Co-located heap-dump leak.** The same "
                        + "`/tmp` is commonly the JVM's default "
                        + "`-Djava.io.tmpdir` (it is on every JDK 8+ "
                        + "default config). A `kill -3` or "
                        + "`HeapDumpOnOutOfMemoryError` writes a heap "
                        + "dump next to the keystore. The heap dump "
                        + "contains the loaded keystore's in-memory "
                        + "representation INCLUDING the unencrypted "
                        + "private key bytes (the JCA `KeyStore` "
                        + "object holds the decrypted PrivateKey in a "
                        + "live field) and the keystore password "
                        + "string (held in the SslContext / "
                        + "SslEngineFactory). An on-call engineer who "
                        + "copies the heap dump to their laptop for "
                        + "analysis just exfiltrated the keystore. "
                        + "(6) **CI/build-server keystore leakage.** "
                        + "If the IT tests use `/tmp/test-keystore.jks` "
                        + "and the CI runner is multi-tenant (shared "
                        + "across teams or branches), keystores from "
                        + "one test run leak into the next. This is the "
                        + "common vector for staging-credential theft "
                        + "via a public PR build. Specific scenarios "
                        + "where this rule fires: (i) **Dev/staging "
                        + "tutorial copy-paste.** Apache Kafka's SSL "
                        + "quickstart and many Confluent tutorials use "
                        + "`ssl.keystore.location=/tmp/kafka."
                        + "keystore.jks` because `/tmp` always exists "
                        + "and is writable without root; the value "
                        + "propagates into production. (ii) **Init "
                        + "container that writes to a tmpfs EmptyDir "
                        + "mounted at `/tmp`.** A Kubernetes init "
                        + "container fetches a cert from Vault, writes "
                        + "it to `/tmp/keystore.jks`, then the app "
                        + "container reads it from there. The fix is "
                        + "to mount a per-pod EmptyDir at "
                        + "`/var/run/secrets/kafka/` instead of "
                        + "overloading `/tmp`. (iii) **Local dev "
                        + "launch script.** A `dev-run.sh` exports "
                        + "`KAFKA_KEYSTORE=/tmp/keystore.jks` for "
                        + "convenience; the script gets templated into "
                        + "a Helm chart for staging; the value reaches "
                        + "prod. (iv) **Spring Boot "
                        + "`application-local.yml` checked into "
                        + "version control.** A team accidentally "
                        + "checks in their local config with "
                        + "`spring.kafka.properties.ssl.keystore."
                        + "location=/tmp/dev-keystore.jks`; the "
                        + "default Spring profile inherits and "
                        + "production runs with the dev path. (v) "
                        + "**Java agent / APM tooling.** A Java agent "
                        + "(Datadog, New Relic, AppDynamics) running "
                        + "as a sidecar in the JVM has full read "
                        + "access to `/tmp` and can be tricked into "
                        + "shipping the keystore off-host as part of "
                        + "a 'log collection' feature. Fix: move the "
                        + "keystore to a path with proper file-system "
                        + "permissions and ownership: (1) For "
                        + "Kubernetes: mount a Secret of type "
                        + "`kubernetes.io/tls` (or a generic Secret "
                        + "with the keystore as a data key) at "
                        + "`/var/run/secrets/kafka/keystore.p12` with "
                        + "`defaultMode: 0400` and a "
                        + "non-root container user. (2) For bare-"
                        + "metal: write the keystore to "
                        + "`/etc/kafka/ssl/keystore.p12` owned by the "
                        + "service-account user with `chmod 600`. (3) "
                        + "For Vault-driven PKI: use Vault Agent's "
                        + "templating to write into a tmpfs mount at "
                        + "`/run/vault/kafka/` owned by the service "
                        + "account, not `/tmp`. (4) If the init "
                        + "container needs scratch space, mount an "
                        + "explicit EmptyDir at a non-conventional "
                        + "path (e.g. `/var/cache/init-cert/`) and "
                        + "scope its permissions tightly. Sibling "
                        + "rules: [[security-ssl-truststore-location-"
                        + "tmp]] is the truststore equivalent (an "
                        + "attacker overwriting the truststore "
                        + "achieves full MITM rather than client "
                        + "impersonation, often worse); "
                        + "[[security-ssl-keystore-type-jks]] catches "
                        + "the proprietary-format choice; "
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
