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
import java.util.Set;

/**
 * Project-scoped rule. Fires when a Kafka client properties file pins
 * {@code ssl.protocol} (or the Spring Boot equivalent) to a legacy or
 * broken TLS version: {@code TLSv1}, {@code TLSv1.1}, {@code SSLv2},
 * {@code SSLv3}, or the historical {@code SSLv2Hello} pseudo-protocol.
 *
 * <p>The Kafka client forwards {@code ssl.protocol} verbatim to the
 * JSSE {@code SSLContext.getInstance(name)} factory. JDK 8u291+, JDK
 * 11.0.11+ and JDK 17+ disable TLSv1 and TLSv1.1 by default via
 * {@code jdk.tls.disabledAlgorithms}; SSLv2/v3 are removed entirely.
 * Explicitly pinning one of these values either fails at startup
 * ({@code NoSuchAlgorithmException}) or — worse, on a JDK with the
 * disabled-list overridden — ships a vulnerable client.
 *
 * <p>ERROR severity. RFC 8996 (March 2021) formally deprecated TLS 1.0
 * and 1.1; PCI-DSS 4.0, FedRAMP, and most enterprise security
 * baselines forbid both for any data-in-transit protection.
 */
public final class SecuritySslProtocolLegacyRule implements ProjectScopedRule {

    private static final String PLAIN_KEY = "ssl.protocol";
    private static final String SPRING_KEY = "spring.kafka.properties.ssl.protocol";

    private static final Set<String> LEGACY_VALUES = Set.of(
            "tlsv1",
            "tlsv1.0",
            "tls",
            "tlsv1.1",
            "sslv2",
            "sslv3",
            "sslv2hello",
            "ssl",
            "ssl_tls");

    private final Severity severity;

    public SecuritySslProtocolLegacyRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SECURITY_SSL_PROTOCOL_LEGACY;
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
        if (!LEGACY_VALUES.contains(normalized)) return;
        out.add(new Violation(
                RuleId.SECURITY_SSL_PROTOCOL_LEGACY, severity,
                ctx.relativize(file), "key:" + key, 0,
                key + "=" + value.trim() + " — pins the Kafka client's "
                        + "TLS handshake to a legacy or broken version. "
                        + "The accepted modern values are `TLSv1.2` and "
                        + "`TLSv1.3`; every other value either is a "
                        + "deprecated/insecure protocol or a JDK alias "
                        + "that the SSLContext factory will resolve to "
                        + "one. Specific problem with each legacy value: "
                        + "(a) **`TLSv1` / `TLSv1.0`** — RFC 8996 (March "
                        + "2021) formally deprecated TLS 1.0; it is "
                        + "vulnerable to BEAST (CVE-2011-3389), the "
                        + "POODLE downgrade variant, and Lucky-13 timing "
                        + "attacks; JDK 8u291+ / JDK 11.0.11+ / JDK 17+ "
                        + "disable it by default in `jdk.tls."
                        + "disabledAlgorithms` in `<JAVA_HOME>/conf/"
                        + "security/java.security`. (b) **`TLSv1.1`** — "
                        + "also deprecated by RFC 8996; vulnerable to "
                        + "the same downgrade/oracle attacks; same JDK "
                        + "disabled-list defaults. (c) **`SSLv3`** — "
                        + "vulnerable to POODLE (CVE-2014-3566), "
                        + "completely removed from modern JDKs; "
                        + "specifying it triggers an immediate "
                        + "`NoSuchAlgorithmException` on JDK 11+. (d) "
                        + "**`SSLv2`** / **`SSLv2Hello`** — vulnerable "
                        + "to the DROWN attack (CVE-2016-0800); removed "
                        + "from every JDK shipped after 2015; "
                        + "specifying it fails immediately. (e) "
                        + "**`TLS`** / **`SSL`** / **`SSL_TLS`** — JDK "
                        + "aliases that resolve to 'the JDK's default "
                        + "SSLContext' — which is `TLSv1.3` on JDK 11+ "
                        + "and `TLSv1.2` on JDK 8. The aliases worked "
                        + "in JDK 7/8 as 'whatever TLS version the JDK "
                        + "supports', so legacy configs were copy-"
                        + "pasted with `ssl.protocol=TLS`. The aliases "
                        + "still resolve in modern JDKs but pin the "
                        + "version to whatever JDK-internal default is "
                        + "first registered in `SunJSSE` — typically "
                        + "the correct modern version, BUT the alias "
                        + "hides the intent: someone reading the config "
                        + "cannot tell whether the client is on TLS 1.2 "
                        + "or TLS 1.3 without inspecting the JDK build, "
                        + "and a JDK upgrade silently changes the "
                        + "negotiated version (a compliance auditor's "
                        + "nightmare). Always pin to the exact version. "
                        + "Two failure shapes for the legacy values: "
                        + "(1) **Startup crash on a hardened JDK.** "
                        + "`org.apache.kafka.common.errors."
                        + "SslAuthenticationException: SSL handshake "
                        + "failed` or `java.security."
                        + "NoSuchAlgorithmException: TLSv1.1 SSLContext "
                        + "not available` at "
                        + "`SSLContext.getInstance(\"TLSv1.1\")` — the "
                        + "JDK refuses to construct the context because "
                        + "the algorithm is in `jdk.tls."
                        + "disabledAlgorithms`. Caught in QA but "
                        + "expensive to diagnose because the error "
                        + "surfaces as a generic SSL handshake failure "
                        + "rather than 'you pinned a forbidden TLS "
                        + "version'. (2) **Silent downgrade on an "
                        + "unhardened JDK.** A JDK with `jdk.tls."
                        + "disabledAlgorithms` overridden (commonly by "
                        + "a security-team-supplied `java.security` "
                        + "patch that 're-enables TLS 1.0 for one "
                        + "legacy partner') — the client connects to "
                        + "the broker with the explicitly-pinned legacy "
                        + "version, all subsequent traffic uses the "
                        + "vulnerable cipher suites, and the cluster is "
                        + "exposed to whatever attack matches the "
                        + "version. The pinning hides the issue from "
                        + "future security audits because the property "
                        + "file looks deliberate. Specific scenarios "
                        + "where this rule fires: (i) **legacy on-prem "
                        + "broker compatibility** — operator runs "
                        + "Apache Kafka 0.10 / 0.11 on an old broker "
                        + "fleet that does not support TLS 1.2, sets "
                        + "`ssl.protocol=TLSv1.1` to force "
                        + "compatibility; the BROKER is the bug — "
                        + "upgrade it. (ii) **copy-paste from 2014-era "
                        + "Stack Overflow** — pre-Kafka-1.0 docs and "
                        + "blog posts widely showed "
                        + "`ssl.protocol=TLSv1.2` (correct) or "
                        + "`ssl.protocol=TLS` (which used to mean "
                        + "'whatever TLS version'); modern copy-paste "
                        + "preserves the obsolete spelling. (iii) "
                        + "**JDK upgrade exposes the pinning** — a "
                        + "team upgrades from JDK 8u201 (which "
                        + "permitted TLSv1.1) to JDK 11.0.11 (which "
                        + "disables it); the Kafka client starts "
                        + "crashing on every connection; the team "
                        + "rolls back the JDK upgrade instead of "
                        + "removing the legacy pinning, and stays on "
                        + "the old JDK indefinitely. (iv) **wildcard "
                        + "`spring.kafka.properties.ssl.protocol=TLS`** "
                        + "— Spring Boot configs templated from a "
                        + "Bitnami / Helm chart often have `TLS` as a "
                        + "default; the value gets forwarded to every "
                        + "Spring-managed producer/consumer/admin and "
                        + "hides the negotiated version. (v) "
                        + "**compliance-audit failure** — PCI-DSS 4.0 "
                        + "(March 2025) and FedRAMP both require "
                        + "TLS 1.2 minimum (and recommend TLS 1.3); an "
                        + "auditor running `grep ssl.protocol` across "
                        + "config files finds the pinned legacy "
                        + "version and flags the entire data-flow as "
                        + "out of compliance. (vi) **FIPS 140-2/3 "
                        + "mode** — a FIPS-mode JDK refuses to "
                        + "instantiate any SSLContext below TLS 1.2; "
                        + "the application fails to start in the "
                        + "regulated environment. Fix: DELETE the "
                        + "`ssl.protocol` line entirely. The Kafka "
                        + "client falls back to the JDK default, which "
                        + "is `TLSv1.3` on JDK 11+ and `TLSv1.2` on "
                        + "JDK 8 — both are correct for every modern "
                        + "Kafka broker. If you genuinely need to pin "
                        + "a specific version (some regulated "
                        + "environments require an explicit "
                        + "negotiation floor), use `ssl.protocol="
                        + "TLSv1.3` and pair it with "
                        + "`ssl.enabled.protocols=TLSv1.3` to lock the "
                        + "list as well. If you need to support a "
                        + "legacy broker, fix the broker — modern "
                        + "Kafka (since 1.0, Nov 2017) supports "
                        + "TLS 1.2; modern Kafka 3.0+ supports "
                        + "TLS 1.3. Sibling rule "
                        + "[[security-ssl-enabled-protocols-legacy]] "
                        + "catches the list-form (`ssl.enabled."
                        + "protocols=TLSv1,TLSv1.1,TLSv1.2`); sibling "
                        + "rule [[security-sasl-mechanism-plain]] "
                        + "catches PLAIN over non-SSL; sibling rule "
                        + "[[security-protocol-plaintext-remote]] "
                        + "catches cleartext to a managed cluster."));
    }
}
