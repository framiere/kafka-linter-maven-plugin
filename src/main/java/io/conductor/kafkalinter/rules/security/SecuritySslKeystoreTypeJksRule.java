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
 * Project-scoped rule. Fires when a Kafka client properties file sets
 * {@code ssl.keystore.type=JKS} (case-insensitive). PKCS12 (RFC 7292)
 * is the modern, cross-tool alternative; JDK 9+ uses PKCS12 as the
 * default keystore type.
 *
 * <p>INFO severity by default — JKS still works at runtime; the issue
 * is a maintainability tax (every certificate rotation requires a
 * `keytool` round-trip because cert-manager, Vault PKI, ACM, OpenShift
 * serving-cert all emit PKCS12 natively).
 */
public final class SecuritySslKeystoreTypeJksRule implements ProjectScopedRule {

    private static final String PLAIN_KEY = "ssl.keystore.type";
    private static final String SPRING_KEY = "spring.kafka.properties.ssl.keystore.type";

    private final Severity severity;

    public SecuritySslKeystoreTypeJksRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SECURITY_SSL_KEYSTORE_TYPE_JKS;
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
        if (!"jks".equals(normalized)) return;
        out.add(new Violation(
                RuleId.SECURITY_SSL_KEYSTORE_TYPE_JKS, severity,
                ctx.relativize(file), "key:" + key, 0,
                key + "=" + value.trim() + " — `JKS` (Java KeyStore) is "
                        + "the proprietary Java-only keystore format that "
                        + "Sun introduced with JDK 1.2 in 1998. It is "
                        + "still supported by every modern JDK, but as "
                        + "of JDK 9 (September 2017, JEP 229) the default "
                        + "keystore type is `PKCS12` — and JDK 17's "
                        + "`<JAVA_HOME>/conf/security/java.security` "
                        + "explicitly notes that `JKS` is 'a proprietary "
                        + "format, and a migration to PKCS12 is "
                        + "recommended.' This rule is NOT about a "
                        + "runtime vulnerability — the Kafka client will "
                        + "open and use a `JKS` keystore exactly as well "
                        + "as a `PKCS12` one — but about the "
                        + "maintainability cost that `JKS` imposes on "
                        + "every certificate rotation in the system. "
                        + "Format comparison: (a) **`PKCS12`** is "
                        + "RFC-7292 (Public-Key Cryptography Standard "
                        + "#12, ratified by RSA Laboratories in 1997 "
                        + "and adopted as IETF RFC 7292 in 2014). It "
                        + "encodes the keystore as a sequence of ASN.1 "
                        + "DER `SafeBag` objects (one bag per "
                        + "private-key-and-cert-chain or one per "
                        + "trusted-cert), wrapped in HMAC-SHA256 "
                        + "integrity protection and PBES2/AES "
                        + "encryption for the private-key bags. Every "
                        + "cryptographic tool on the planet — OpenSSL, "
                        + "Windows certutil, macOS Keychain, Linux p11-"
                        + "kit, OpenShift's `oc create secret tls`, "
                        + "AWS ACM's `export-certificate`, Vault PKI's "
                        + "`vault read pki/issue/...`, cert-manager's "
                        + "`kubernetes.io/tls` Secret format, Bitnami "
                        + "Helm charts — produce, consume, or natively "
                        + "transcode to PKCS12. (b) **`JKS`** is "
                        + "Sun-defined, unstandardised, and uses a "
                        + "1990s-era custom binary format with SHA-1 "
                        + "for the integrity MAC (modernised to SHA-256 "
                        + "in JDK 8u301+ but still proprietary). The "
                        + "ONLY tools that read and write JKS are Java "
                        + "tools — `keytool`, `kafka-storage.sh`, "
                        + "third-party Java keystore manipulation "
                        + "libraries. No OpenSSL workflow, no "
                        + "Kubernetes-native workflow, no "
                        + "certificate-manager workflow produces JKS "
                        + "directly. (c) **`JCEKS`** (Java "
                        + "Cryptography Extension KeyStore) — "
                        + "proprietary, slightly-stronger-encryption "
                        + "variant of JKS; same portability problems "
                        + "for `JCEKS`-typed keystores. (d) **`BKS`** "
                        + "/ **`UBER`** — Bouncy Castle's keystore "
                        + "formats; same issue. Specific impacts of "
                        + "JKS: (1) **Cert-manager friction in "
                        + "Kubernetes.** A `Certificate` resource on "
                        + "cert-manager produces a Kubernetes "
                        + "`Secret` of type `kubernetes.io/tls` "
                        + "containing PEM-encoded `tls.crt`, "
                        + "`tls.key`, and (optionally) a PKCS12 "
                        + "bundle via the `keystores.pkcs12` field "
                        + "(cert-manager 1.5+, Aug 2021). There is "
                        + "no `keystores.jks` field — to use a JKS "
                        + "keystore in a Kafka client pod, the team "
                        + "either (i) runs an init container that "
                        + "`keytool -importkeystore -srcstoretype "
                        + "PKCS12 -deststoretype JKS` on every pod "
                        + "startup, adding ~3 seconds to pod "
                        + "ready-time and one extra failure mode, or "
                        + "(ii) builds a custom controller that "
                        + "watches the cert-manager Secret and writes "
                        + "out a JKS, adding an operational dependency. "
                        + "Switching the Kafka client to "
                        + "`ssl.keystore.type=PKCS12` eliminates both. "
                        + "(2) **Vault PKI rotation.** HashiCorp Vault's "
                        + "PKI secrets engine issues certificates as "
                        + "PEM (`vault read pki/issue/<role>`). The "
                        + "agent-injector sidecar templates these into "
                        + "the pod filesystem as PEM files. Loading a "
                        + "PEM cert+key directly via "
                        + "`ssl.keystore.type=PEM` (KIP-651, Kafka 2.7, "
                        + "December 2020) is the modern Kafka path; "
                        + "PKCS12 is the second-best alternative; JKS "
                        + "requires a `keytool` conversion step on every "
                        + "rotation. (3) **AWS ACM Private CA export.** "
                        + "ACM Private CA's `export-certificate` API "
                        + "returns PEM and PKCS12. JKS conversion is the "
                        + "team's responsibility — typically scripted as "
                        + "a Lambda or a cron job, which becomes the "
                        + "team's homegrown 'cert rotation pipeline' "
                        + "with all the bugs that entails. (4) **Windows "
                        + "/ macOS interop.** A team on a heterogeneous "
                        + "fleet (Java services on Linux, .NET services "
                        + "on Windows, mobile clients on macOS) cannot "
                        + "share a single PKI bundle — the Windows and "
                        + "macOS sides read PKCS12 natively and need a "
                        + "separate JKS-only path for the Java side. "
                        + "(5) **SHA-1 integrity MAC on old JKS files.** "
                        + "A JKS file created with JDK 8u300 or older "
                        + "(2021 and earlier) uses SHA-1 for the keystore "
                        + "integrity MAC; modern FIPS 140-3 mode rejects "
                        + "this and the keystore fails to load. The fix "
                        + "is to convert to PKCS12 or to re-create the "
                        + "JKS on a modern JDK, but the former is the "
                        + "correct long-term answer. (6) **`keytool` "
                        + "deprecation warning.** Since JDK 9, `keytool "
                        + "-genkeypair -keystore foo.jks` prints "
                        + "'Warning: The JKS keystore uses a proprietary "
                        + "format. It is recommended to migrate to "
                        + "PKCS12 which is an industry standard format "
                        + "using \"keytool -importkeystore -srcstoretype "
                        + "JKS -deststoretype PKCS12\".' The warning "
                        + "appears on every interactive keystore "
                        + "operation and is the source of the migration "
                        + "advice that this rule encodes. Specific "
                        + "scenarios where this rule fires: (i) "
                        + "**Long-lived on-prem deployment.** A team "
                        + "set up Kafka clients in 2017 with "
                        + "`ssl.keystore.type=JKS` because that was the "
                        + "JDK 8 default; the property has propagated "
                        + "to every new service through Helm chart "
                        + "templates ever since. Removing the line "
                        + "lets the JDK pick PKCS12 by default on "
                        + "JDK 11+. (ii) **Apache Kafka quickstart "
                        + "copy-paste.** The 2016-era Apache Kafka SSL "
                        + "quickstart used `keytool -keystore "
                        + "server.keystore.jks` everywhere; teams "
                        + "copying from that doc set "
                        + "`ssl.keystore.type=JKS` explicitly even "
                        + "when their tooling produces PKCS12 "
                        + "natively. (iii) **Confluent Platform "
                        + "tutorial.** Older Confluent Platform docs "
                        + "(pre-7.x, 2022) used JKS examples; "
                        + "post-7.0 docs use PKCS12. (iv) **Strimzi "
                        + "`KafkaUser` client cert.** Strimzi produces "
                        + "a PKCS12 Secret for client certs; teams "
                        + "transcode to JKS for legacy app "
                        + "compatibility — but the legacy app is "
                        + "usually fine with PKCS12 too. (v) **Spring "
                        + "Boot `application.yml` template.** Many "
                        + "Spring Boot tutorials set "
                        + "`spring.kafka.properties.ssl.keystore.type"
                        + "=JKS` because that was the default in "
                        + "Spring Boot 2.x with JDK 8. Spring Boot 3.x "
                        + "on JDK 17+ defaults to PKCS12 — the "
                        + "explicit JKS value is now overriding a "
                        + "better default. (vi) **FIPS 140-3 "
                        + "compliance project.** A bank or fed-related "
                        + "deployment needs to run on a FIPS-enabled "
                        + "JDK; the legacy JKS keystores load with "
                        + "warnings (SHA-1 MAC) or fail outright "
                        + "(modern FIPS mode); the migration to "
                        + "PKCS12 is a prerequisite. Fix: two-step "
                        + "migration. (1) Convert the keystore on "
                        + "disk: `keytool -importkeystore "
                        + "-srcstoretype JKS -srckeystore "
                        + "client.keystore.jks -deststoretype "
                        + "PKCS12 -destkeystore client.keystore.p12 "
                        + "-srcstorepass <pw> -deststorepass <pw>` "
                        + "(or, on JDK 8u301+, `keytool "
                        + "-importkeystore -srckeystore "
                        + "client.keystore.jks -destkeystore "
                        + "client.keystore.p12` — JDK auto-detects "
                        + "source type and uses PKCS12 as the "
                        + "destination default). (2) Update the "
                        + "property file: either delete the "
                        + "`ssl.keystore.type` line entirely (lets "
                        + "the JDK 9+ default of PKCS12 kick in and "
                        + "matches the file extension) or explicitly "
                        + "set `ssl.keystore.type=PKCS12`. Do the same "
                        + "for `ssl.truststore.type` if it is set to "
                        + "JKS — the truststore migration has no key "
                        + "material to worry about and is purely a "
                        + "format change. For PEM-native workflows "
                        + "(Vault, cert-manager), prefer "
                        + "`ssl.keystore.type=PEM` and point "
                        + "`ssl.keystore.location` at the PEM bundle "
                        + "directly (KIP-651, Kafka 2.7+). Sibling "
                        + "rules: [[security-ssl-keystore-location-tmp]] "
                        + "catches keystores stored in `/tmp` "
                        + "(non-persistent), [[security-ssl-truststore-"
                        + "location-tmp]] is the truststore equivalent, "
                        + "[[security-ssl-protocol-legacy]] catches "
                        + "legacy TLS version pinning, "
                        + "[[security-sasl-mechanism-plain]] catches "
                        + "PLAIN authentication choice."));
    }
}
