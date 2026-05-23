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
 * {@code sasl.mechanism=PLAIN} (case-insensitive), with or without
 * {@code security.protocol=SASL_SSL}.
 *
 * <p>SASL/PLAIN (RFC 4616) sends the username and password as a
 * single base64-encoded token on the first authentication frame. The
 * broker receives the cleartext password and compares it locally —
 * there is no challenge, no salt, no hash. Even over TLS the broker
 * is in possession of every client's plaintext password (a key-
 * management headache that the SCRAM-SHA-* family eliminates).
 * Without TLS underneath, anyone with a packet capture on the path
 * has the credentials.
 *
 * <p>WARNING severity. There are legitimate uses of PLAIN-over-TLS
 * (Confluent Cloud API keys, managed services where SCRAM is not
 * an option), so the rule is a 'have you considered SCRAM?' nudge
 * rather than a hard ERROR. The detail message notes whether the
 * accompanying {@code security.protocol} makes the situation acute
 * (cleartext credentials on the wire) or merely sub-optimal
 * (broker still sees plaintext).
 */
public final class SecuritySaslMechanismPlainRule implements ProjectScopedRule {

    private static final String PLAIN_MECH_KEY = "sasl.mechanism";
    private static final String SPRING_MECH_KEY = "spring.kafka.properties.sasl.mechanism";
    private static final String PLAIN_PROTO_KEY = "security.protocol";
    private static final String SPRING_PROTO_KEY = "spring.kafka.properties.security.protocol";

    private final Severity severity;

    public SecuritySaslMechanismPlainRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SECURITY_SASL_MECHANISM_PLAIN;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            checkPair(out, ctx, e.getKey(), p, PLAIN_MECH_KEY, PLAIN_PROTO_KEY);
            checkPair(out, ctx, e.getKey(), p, SPRING_MECH_KEY, SPRING_PROTO_KEY);
        }
        return out;
    }

    private void checkPair(List<Violation> out, ProjectContext ctx, Path file,
                           Properties p, String mechKey, String protoKey) {
        String mech = p.getProperty(mechKey);
        if (mech == null) return;
        if (!"plain".equals(mech.trim().toLowerCase(Locale.ROOT))) return;
        String proto = p.getProperty(protoKey);
        String normalizedProto = proto == null ? "" : proto.trim().toUpperCase(Locale.ROOT);
        boolean overTls = "SASL_SSL".equals(normalizedProto);
        String protoStateClause = proto == null
                ? "no `" + protoKey + "` is set (client defaults to PLAINTEXT — the SASL handshake runs over a non-TLS socket)"
                : "`" + protoKey + "=" + proto.trim() + "`";
        String wirelineClause = overTls
                ? "the credentials are wrapped in a TLS tunnel so the wire is encrypted, BUT the BROKER still receives every client's plaintext password — a key-management liability that survives every TLS rotation."
                : "the username + base64-encoded password are emitted on the wire as the first authentication frame, fully recoverable from a `tcpdump` on any host with a network tap on the broker's listener (sidecar, K8s CNI in promiscuous mode, BPF probe, broker DNS man-in-the-middle).";
        out.add(new Violation(
                RuleId.SECURITY_SASL_MECHANISM_PLAIN, severity,
                ctx.relativize(file), "key:" + mechKey, 0,
                mechKey + "=" + mech.trim() + " and " + protoStateClause
                        + " — SASL/PLAIN (RFC 4616, March 2006) wraps the "
                        + "username and password into a single base64-"
                        + "encoded token sent on the first authentication "
                        + "frame; the broker compares the cleartext "
                        + "password against its local store. There is no "
                        + "challenge, no salt, no hash on the wire, no "
                        + "client-side proof-of-knowledge — the broker "
                        + "literally receives `\\0username\\0password` (NUL-"
                        + "separated, per the RFC). For this file: "
                        + wirelineClause + " The SCRAM-SHA-* family "
                        + "(KIP-84 / Apache Kafka 0.10.2, February 2017) "
                        + "solves both halves of the problem: the broker "
                        + "stores only `SaltedPassword = Hi(Normalize("
                        + "password), salt, iterations)`, not the password "
                        + "itself; the handshake is a multi-round "
                        + "challenge-response (RFC 5802) where the client "
                        + "proves knowledge of the password without "
                        + "transmitting it, the broker proves it has the "
                        + "salted hash without revealing it. A compromised "
                        + "broker disk leaks `SaltedPassword` values which "
                        + "are useless against the brokers themselves (the "
                        + "client still needs the original password for "
                        + "the handshake) and prohibitively expensive to "
                        + "brute-force on a per-credential basis (the "
                        + "default 4096 PBKDF2 iterations are ~5ms per "
                        + "guess on commodity CPUs). SCRAM-SHA-512 in "
                        + "particular has been the default-recommended "
                        + "mechanism since Apache Kafka 2.0 (July 2018). "
                        + "Specific impacts of PLAIN: (a) **Broker-disk "
                        + "compromise → org-wide password leak.** With "
                        + "PLAIN, the broker's user database stores "
                        + "cleartext (or trivially-reversible, depending "
                        + "on the storage backend — JAAS file with "
                        + "`org.apache.kafka.common.security.plain.Plain"
                        + "LoginModule` stores raw passwords; LDAP-backed "
                        + "stores commonly use SSHA which is brute-"
                        + "forceable in hours for short passwords). A "
                        + "single broker-disk exfiltration (insider, "
                        + "leaked backup, RCE) yields every service's "
                        + "password in cleartext. Because Kafka service "
                        + "accounts are commonly shared with the same "
                        + "credentials in adjacent systems (Zookeeper, "
                        + "Schema Registry, Kafka Connect, the "
                        + "application's database), the blast radius is "
                        + "every system in the credential's trust "
                        + "boundary. (b) **Per-client password rotation "
                        + "is the org's responsibility.** PLAIN passwords "
                        + "cannot be rotated server-side without "
                        + "coordinated client-side restart; SCRAM "
                        + "supports independent server-side iteration "
                        + "increase + dual-credential active-active "
                        + "rotation. (c) **Compliance failures.** "
                        + "PCI-DSS 4.0 requires that 'strong "
                        + "cryptography' protect both data-in-transit "
                        + "AND data-at-rest authentication credentials; "
                        + "auditors flag PLAIN as failing the at-rest "
                        + "half regardless of TLS configuration. FedRAMP "
                        + "Moderate baseline IA-5(1)(c) requires "
                        + "cryptographic storage of authenticator "
                        + "secrets — PLAIN explicitly fails this. (d) "
                        + "**Replay-attack window in non-TLS configs.** "
                        + "With `security.protocol=SASL_PLAINTEXT`, the "
                        + "captured PLAIN auth frame can be replayed "
                        + "verbatim by an attacker to authenticate to "
                        + "any broker — no nonce, no timestamp, no "
                        + "binding to the TCP session. SCRAM's "
                        + "challenge-response naturally prevents this. "
                        + "Specific scenarios where this rule fires: "
                        + "(1) **Confluent Cloud API key copy-paste.** "
                        + "Confluent Cloud uses SASL_SSL + PLAIN by "
                        + "default because the API key/secret model "
                        + "maps cleanly to PLAIN; this is the legitimate "
                        + "case where PLAIN-over-TLS is acceptable, but "
                        + "the rule still fires as a nudge that "
                        + "Confluent Cloud also supports OAUTHBEARER "
                        + "(KIP-768) and SCRAM-SHA-512 as alternatives "
                        + "with stronger semantics. (2) **MSK + IAM "
                        + "fallback to SASL_PLAINTEXT.** AWS MSK "
                        + "supports IAM, SCRAM, mTLS, and unauthenticated "
                        + "listeners; a misconfigured cluster falls "
                        + "back to a SASL_PLAINTEXT + PLAIN listener "
                        + "for 'debug access' that someone forgot to "
                        + "decommission. (3) **On-prem broker with "
                        + "SCRAM available but PLAIN configured.** The "
                        + "broker supports both mechanisms via "
                        + "`sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,"
                        + "SCRAM-SHA-512` but the client config picked "
                        + "PLAIN for simplicity (no salt management on "
                        + "the client side); SCRAM doesn't need salt "
                        + "management either, the client just sends the "
                        + "password and the SASL state machine handles "
                        + "the rest. (4) **Strimzi `KafkaUser` with "
                        + "`authentication.type=scram-sha-512` but the "
                        + "Streams app's .properties still says "
                        + "`sasl.mechanism=PLAIN`.** The KafkaUser CRD "
                        + "configures the broker side; the client side "
                        + "must independently agree. Mismatched config "
                        + "fails authentication, the developer changes "
                        + "the client to PLAIN to 'match the easier "
                        + "default' rather than fixing the KafkaUser. "
                        + "(5) **Helm chart with PLAIN as the "
                        + "default-easy template.** Many Kafka client "
                        + "Helm charts ship with PLAIN as the default "
                        + "to minimize per-environment setup; in "
                        + "production this default propagates "
                        + "everywhere. (6) **Java SDK example "
                        + "snippets.** Apache Kafka's official "
                        + "client-quickstart examples historically used "
                        + "PLAIN; the SCRAM examples were added later "
                        + "and live deeper in the docs. Copy-paste "
                        + "from quickstart → production. Fix: change "
                        + "to `sasl.mechanism=SCRAM-SHA-512` (preferred) "
                        + "or `sasl.mechanism=SCRAM-SHA-256`; update the "
                        + "JAAS config from `PlainLoginModule` to "
                        + "`ScramLoginModule`; provision the SCRAM "
                        + "credential on the broker side "
                        + "(`kafka-configs.sh --alter --add-config "
                        + "'SCRAM-SHA-512=[iterations=4096,password=..."
                        + "]' --entity-type users --entity-name "
                        + "<user>`); restart the client. If you are on "
                        + "Confluent Cloud and stuck with PLAIN, prefer "
                        + "OAUTHBEARER (KIP-768) with a corporate IdP; "
                        + "if neither is available, ensure "
                        + "`security.protocol=SASL_SSL` is explicit and "
                        + "rotate the API key at least every 90 days. "
                        + "Sibling rules: "
                        + "[[security-protocol-plaintext-remote]] is the "
                        + "broker-side equivalent (no TLS to a managed "
                        + "cluster); "
                        + "[[security-sasl-oauthbearer-token-endpoint-"
                        + "http]] is the OAUTHBEARER token endpoint "
                        + "cleartext leak; "
                        + "[[security-ssl-protocol-legacy]] is the "
                        + "deprecated-TLS-version pinning."));
    }
}
