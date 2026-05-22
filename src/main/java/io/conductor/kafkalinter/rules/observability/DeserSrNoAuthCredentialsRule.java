package io.conductor.kafkalinter.rules.observability;

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
 * Project-scoped rule. Fires when a properties file sets
 * {@code schema.registry.url=https://...} (Schema Registry over TLS) but does
 * NOT configure any client-side authentication credential. TLS encrypts the
 * bytes on the wire but does not authenticate the client — without
 * {@code basic.auth.credentials.source}, {@code bearer.auth.credentials.source},
 * or {@code schema.registry.ssl.keystore.location} (mTLS), the registry either
 * crashes with {@code 401 Unauthorized} on first send OR (worse) accepts
 * anonymous read/write, leaking schemas and allowing arbitrary subject writes.
 *
 * <p>Detects both the plain key {@code schema.registry.url} and the Spring Boot
 * prefixed {@code spring.kafka.properties.schema.registry.url}. Emits at most
 * one violation per file (does not double-count plain + Spring matches).
 *
 * <p>The HTTP-URL case is covered by {@code SCHEMA_REGISTRY_URL_HTTP} (a
 * separate concern); this rule only fires on {@code https://} URLs.
 */
public final class DeserSrNoAuthCredentialsRule implements ProjectScopedRule {

    private static final String SR_URL_PLAIN = "schema.registry.url";
    private static final String SR_URL_SPRING = "spring.kafka.properties.schema.registry.url";

    // Plain (unprefixed) auth-evidence keys.
    private static final List<String> AUTH_KEYS_PLAIN = List.of(
            "basic.auth.credentials.source",
            "basic.auth.user.info",
            "bearer.auth.credentials.source",
            "bearer.auth.token",
            "schema.registry.ssl.keystore.location");

    // Spring Boot prefixed variants.
    private static final List<String> AUTH_KEYS_SPRING = List.of(
            "spring.kafka.properties.basic.auth.credentials.source",
            "spring.kafka.properties.basic.auth.user.info",
            "spring.kafka.properties.bearer.auth.credentials.source",
            "spring.kafka.properties.bearer.auth.token",
            "spring.kafka.properties.schema.registry.ssl.keystore.location");

    private final Severity severity;

    public DeserSrNoAuthCredentialsRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.DESER_SR_NO_AUTH_CREDENTIALS;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Path file = e.getKey();
            Properties p = e.getValue();

            String plainUrl = trimOrNull(p.getProperty(SR_URL_PLAIN));
            String springUrl = trimOrNull(p.getProperty(SR_URL_SPRING));

            String url;
            String urlKey;
            if (isHttps(plainUrl)) {
                url = plainUrl;
                urlKey = SR_URL_PLAIN;
            } else if (isHttps(springUrl)) {
                url = springUrl;
                urlKey = SR_URL_SPRING;
            } else {
                continue;
            }

            if (hasAnyAuthEvidence(p)) continue;

            out.add(new Violation(
                    RuleId.DESER_SR_NO_AUTH_CREDENTIALS, severity,
                    ctx.relativize(file), "key:" + urlKey, 0,
                    urlKey + "=" + url + " — Schema Registry over TLS, but NO client-side "
                            + "authentication is configured (no `basic.auth.credentials.source` / "
                            + "`basic.auth.user.info`, no `bearer.auth.credentials.source` / "
                            + "`bearer.auth.token`, no `schema.registry.ssl.keystore.location` for "
                            + "mTLS). TLS encrypts the bytes on the wire but does NOT authenticate "
                            + "the client. Confluent's `RestService` will issue every request with "
                            + "no `Authorization` header and no client certificate. Two failure "
                            + "modes follow: (a) **fail-loud 401 crashloop** — Confluent Cloud and "
                            + "any auth-required SR rejects the request with `RestClientException: "
                            + "Unauthorized; error code: 401` deep inside the deserializer, the "
                            + "consumer crashes on first poll, restarts, crashes again, until "
                            + "someone reads the stack trace; OR (b) **silent anonymous access** "
                            + "— on-prem SR with `authentication.method=NONE` (the default!) "
                            + "accepts the request, so the producer/consumer/Streams app runs "
                            + "fine, but anyone with network reachability to the SR endpoint can "
                            + "now read every schema (which often encodes internal model names, "
                            + "field labels, business-domain detail) and, if write is also "
                            + "unauthenticated, register new schemas under existing subjects — "
                            + "corrupting schema evolution for every producer/consumer in the "
                            + "ecosystem. The half-migrated case ([[schema-registry-url-http]] "
                            + "flipped from http:// to https:// without adding auth in the same "
                            + "migration) is the worst-of-both: appears secure on cursory audit "
                            + "(`https://`!) but is anonymous. Fix: pair the HTTPS URL with auth. "
                            + "For basic auth: `basic.auth.credentials.source=USER_INFO` + "
                            + "`basic.auth.user.info=${SR_USER_INFO}` (env-injected, NOT a "
                            + "literal — see [[cred-basic-auth-user-info-literal]]). For OAuth: "
                            + "`bearer.auth.credentials.source=OAUTHBEARER` + the OAuth client-"
                            + "credentials config. For mTLS: "
                            + "`schema.registry.ssl.keystore.location=/path/to/client.p12` (with "
                            + "matching `schema.registry.ssl.keystore.password=${KEYSTORE_PASS}` "
                            + "env-injected). Sibling rules: "
                            + "CRED_BASIC_AUTH_USER_INFO_LITERAL (catches hardcoded user:password "
                            + "literals), CRED_SR_BEARER_AUTH_TOKEN_LITERAL (catches hardcoded "
                            + "bearer JWT literals), SCHEMA_REGISTRY_URL_HTTP (catches the "
                            + "plain-HTTP URL, an orthogonal failure)."));
        }
        return out;
    }

    private static boolean hasAnyAuthEvidence(Properties p) {
        for (String k : AUTH_KEYS_PLAIN) {
            if (trimOrNull(p.getProperty(k)) != null) return true;
        }
        for (String k : AUTH_KEYS_SPRING) {
            if (trimOrNull(p.getProperty(k)) != null) return true;
        }
        return false;
    }

    private static boolean isHttps(String url) {
        return url != null && url.toLowerCase(Locale.ROOT).startsWith("https://");
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
