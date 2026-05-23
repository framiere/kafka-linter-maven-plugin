package io.conductor.kafkalinter.rules.security;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Project-scoped rule. Fires when a Kafka client properties file sets
 * {@code sasl.oauthbearer.token.endpoint.url} (or the Spring Boot
 * equivalent) to an {@code http://} URL — meaning the OAuth2
 * client-credentials request (carrying the client_secret) and the
 * issued JWT bearer token both traverse the network in cleartext.
 *
 * <p>KIP-768 (Apache Kafka 3.1, January 2022) introduced the
 * OAUTHBEARER login callback handler that performs an
 * RFC-6749 §4.4 client-credentials POST to this endpoint to exchange
 * a {@code client_id}/{@code client_secret} for a JWT. The endpoint
 * URL MUST be {@code https://} for any production deployment.
 *
 * <p>ERROR severity because the misconfiguration is a CVE-grade leak
 * of long-lived credentials and short-lived tokens, with no scenario
 * where {@code http://} is correct in production.
 */
public final class SecuritySaslOauthbearerTokenEndpointHttpRule implements ProjectScopedRule {

    private static final String PLAIN_KEY =
            "sasl.oauthbearer.token.endpoint.url";
    private static final String SPRING_KEY =
            "spring.kafka.properties.sasl.oauthbearer.token.endpoint.url";
    private static final String HTTP_PREFIX = "http://";

    private final Severity severity;

    public SecuritySaslOauthbearerTokenEndpointHttpRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SECURITY_SASL_OAUTHBEARER_TOKEN_ENDPOINT_HTTP;
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
        String trimmed = value.trim();
        if (trimmed.isEmpty()) return;
        if (!trimmed.toLowerCase().startsWith(HTTP_PREFIX)) return;
        out.add(new Violation(
                RuleId.SECURITY_SASL_OAUTHBEARER_TOKEN_ENDPOINT_HTTP, severity,
                ctx.relativize(file), "key:" + key, 0,
                key + "=" + trimmed + " — the OAuth2 token endpoint URL "
                        + "is configured with `http://` (cleartext) instead "
                        + "of `https://`. KIP-768 (Apache Kafka 3.1, January "
                        + "2022) introduced the OAUTHBEARER login callback "
                        + "(`OAuthBearerLoginCallbackHandler`) that performs "
                        + "an RFC-6749 §4.4 OAuth2 client-credentials POST "
                        + "to this URL to exchange a long-lived "
                        + "`client_id`/`client_secret` pair for a short-"
                        + "lived JWT bearer token; the JWT is then used by "
                        + "the Kafka client as the `Authorization: Bearer "
                        + "<jwt>` credential in every subsequent SASL/"
                        + "OAUTHBEARER handshake to the brokers. With "
                        + "`http://`, the ENTIRE client-credentials flow "
                        + "runs in cleartext: (1) the POST request body "
                        + "carries `grant_type=client_credentials&client_id="
                        + "<id>&client_secret=<secret>&scope=<scope>` (or "
                        + "the equivalent `Authorization: Basic <base64("
                        + "client_id:client_secret)>` header) — both forms "
                        + "are fully recoverable from a `tcpdump`, a "
                        + "compromised L7 proxy log, an HTTP-aware IDS, an "
                        + "APM agent that captures request bodies, or a "
                        + "service-mesh sidecar that logs HTTP traffic to "
                        + "stdout; (2) the response body carries the JWT "
                        + "as a JSON `access_token` field — the JWT is "
                        + "self-contained, includes the `sub` (subject) "
                        + "and `aud` (audience) claims that the brokers "
                        + "verify, and is valid for its full lifetime "
                        + "(typically 1-24 hours) regardless of where it "
                        + "was intercepted. Two distinct leaks: (a) "
                        + "**client_secret leak — permanent compromise.** "
                        + "The client_secret is a LONG-LIVED credential "
                        + "(machine-to-machine credentials are typically "
                        + "rotated annually or — in practice — never). "
                        + "An attacker who captures it can mint NEW JWTs "
                        + "forever by calling the same token endpoint with "
                        + "the same client_id/client_secret pair (this "
                        + "time over HTTPS — the IdP doesn't care which "
                        + "transport the client used). The compromise "
                        + "extends to EVERY service in the OAuth2 trust "
                        + "boundary that accepts tokens issued for this "
                        + "client_id, not just Kafka. (b) **JWT leak — "
                        + "immediate broker authentication.** The captured "
                        + "JWT directly authenticates to the brokers; the "
                        + "attacker can produce, consume, create topics, "
                        + "delete topics, list consumer groups, etc. as "
                        + "that client identity — bounded only by the "
                        + "broker-side ACLs on that subject. On a multi-"
                        + "tenant cluster this is full account takeover "
                        + "from any on-path observer. Specific scenarios "
                        + "where the misconfiguration leaks: (1) **dev/"
                        + "test config leak into prod** — a developer "
                        + "sets `http://localhost:8080/realms/dev/protocol/"
                        + "openid-connect/token` while working against a "
                        + "local Keycloak; the same .properties file gets "
                        + "templated into the prod Helm chart and ships "
                        + "to prod where the URL points at a "
                        + "real-but-cleartext IdP; CVE-grade leak, never "
                        + "noticed because the broker handshake still "
                        + "works. (2) **in-cluster IdP behind a service-"
                        + "mesh sidecar** — operator says 'the sidecar "
                        + "encrypts pod-to-pod, http:// inside the cluster "
                        + "is fine' — but the sidecar's L7 access logs "
                        + "(commonly enabled by default) record the full "
                        + "HTTP request including the body, so the "
                        + "client_secret ends up in the central logging "
                        + "platform (ELK / Loki / Datadog Logs), readable "
                        + "by every engineer with logs access. (3) "
                        + "**APM agent capture** — modern APM agents "
                        + "(Datadog, New Relic, Elastic APM, Dynatrace) "
                        + "by default capture HTTP request bodies for "
                        + "outbound HTTP calls when the URL doesn't start "
                        + "with `https://`; the client_secret and the JWT "
                        + "both end up in the APM SaaS dashboards, "
                        + "readable by the APM tenant admins and "
                        + "exported in trace dumps. (4) **public WiFi / "
                        + "hotel network from a developer laptop** — a "
                        + "Java IDE running a Kafka client with this "
                        + "config on a coffee-shop WiFi advertises the "
                        + "client_secret to every other device on the "
                        + "captive portal. (5) **transparent egress "
                        + "proxy with HTTP-aware logging** — corporate "
                        + "egress proxies log full request URIs for "
                        + "`http://` and only the CONNECT verb for "
                        + "`https://`; the client_secret traveling over "
                        + "`http://` ends up in the proxy access log "
                        + "indexed alongside every other intra-corp "
                        + "request. (6) **load-balancer health checks** "
                        + "— some operators put the IdP behind an L7 "
                        + "load balancer that strips TLS at the edge "
                        + "and forwards `http://` internally; the "
                        + "client thinks it called https because the "
                        + "outer URL is https, but the operator copy-"
                        + "pasted the INTERNAL load-balancer URL into "
                        + "the Kafka client config — every "
                        + "client-credentials request goes plaintext on "
                        + "the internal LAN. Common bug shapes: (a) "
                        + "**Strimzi + Keycloak with HTTP service** — "
                        + "Strimzi's `KafkaUser.spec.authentication.type="
                        + "oauth` documentation example uses `http://"
                        + "keycloak:8080/realms/...` for the internal "
                        + "K8s Service URL; operators copy-paste without "
                        + "noticing the cluster needs an HTTPS-fronted "
                        + "Keycloak. (b) **Confluent Cloud + Okta** — "
                        + "Confluent Cloud's OAUTHBEARER docs correctly "
                        + "show `https://{okta-domain}/oauth2/default/v1/"
                        + "token`, but customers sometimes use Okta's "
                        + "legacy `/api/v1/authn` HTTP endpoint as a "
                        + "shortcut during testing and forget to change "
                        + "it. (c) **Spring Boot autoconfig + custom "
                        + "OAUTHBEARER login handler** — Spring Kafka "
                        + "does not validate that "
                        + "`spring.kafka.properties.sasl.oauthbearer."
                        + "token.endpoint.url` starts with `https://`; "
                        + "the Spring Boot value gets forwarded verbatim "
                        + "to the Kafka client. (d) **KRaft + Zitadel/"
                        + "Authentik/Ory Hydra self-hosted IdPs** — "
                        + "operators of self-hosted IdPs (Zitadel, "
                        + "Authentik, Ory Hydra, Dex, FusionAuth) "
                        + "commonly run them on `:8080` with a "
                        + "'we'll add TLS later' comment in the README; "
                        + "the Kafka client config locks in the "
                        + "cleartext URL and the migration never "
                        + "happens. (e) **terraform/ansible templating "
                        + "leak** — the IdP URL is templated from a "
                        + "variable that resolves to `http://` in the "
                        + "dev environment and never gets switched to "
                        + "`https://` in the prod environment because "
                        + "the prod variable was forgotten in the var-"
                        + "file. Fix: change the URL to `https://...` "
                        + "with a properly-issued TLS certificate (the "
                        + "Kafka client validates the cert chain against "
                        + "the JVM's default truststore unless "
                        + "`ssl.truststore.location` is overridden). For "
                        + "an in-cluster IdP behind a service mesh, "
                        + "front it with an HTTPS gateway (mTLS-"
                        + "terminating Envoy/Istio/Linkerd ingress, or a "
                        + "dedicated NGINX with cert-manager-issued "
                        + "certs). For development against a local "
                        + "Keycloak/Zitadel, run the IdP with a self-"
                        + "signed cert and add it to the JVM truststore "
                        + "via `cacerts` — NEVER fall back to "
                        + "`http://` because the dev config will "
                        + "eventually leak into prod. Sibling rules: "
                        + "[[security-protocol-plaintext-remote]] is the "
                        + "broker-side equivalent (cleartext Kafka "
                        + "wire-protocol to a managed cluster); "
                        + "[[security-ssl-protocol-legacy]] catches "
                        + "TLS 1.0/1.1 explicit configuration; "
                        + "[[security-sasl-mechanism-plain]] catches "
                        + "PLAIN-without-SSL credential leak."));
    }
}
