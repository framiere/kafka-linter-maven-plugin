# KAFKA_CLIENTS_CVE_SASL_OAUTHBEARER

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: CVE-2025-27817 — your kafka-clients can be told to read any file on disk.

## TL;DR

`kafka-clients` versions `3.1.0` through `3.9.0` are vulnerable to **CVE-2025-27817**: a SSRF / arbitrary-file-read flaw in the SASL OAUTHBEARER configuration parsing. If client configuration can be influenced by an untrusted party (Kafka Connect, multi-tenant SaaS, JMX-driven reconfig), the attacker can exfiltrate filesystem contents and environment variables through error logs, or pivot HTTP requests. Fixed in **3.9.1** and **4.0.0**.

## The setup

You ship a Kafka Connect cluster, a streaming SaaS where customers supply connection parameters, or any service where part of `Properties` is filled in from a config that crosses a trust boundary. Even within a single tenant, if your config files are stored anywhere a low-privileged process can write, this becomes interesting.

## What's actually happening

The OAUTHBEARER SASL mechanism accepts two URL-shaped properties:

- `sasl.oauthbearer.token.endpoint.url`
- `sasl.oauthbearer.jwks.endpoint.url`

In affected versions, neither is validated against an allow-list. The client will dereference these URLs — including `file://` and `http://internal-metadata-service/`. On failure the contents may be embedded in an error log line. Result: an attacker who can set those properties can ask the client to read `/etc/passwd`, `/proc/self/environ`, an EC2 IMDS endpoint, or any internal HTTP service the client process can reach.

The fix (Kafka 3.9.1 / 4.0.0) introduces `-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls`:

- In **3.9.1**, the default still allows all URLs (backward compatibility) — so the upgrade alone isn't enough; you must set the system property to an allow-list.
- In **4.0.0+**, the default is an empty list — you must explicitly enumerate trusted URLs or OAUTHBEARER fails.

## Why this is subtle

- Most teams never set these properties explicitly — they think "we don't use OAUTHBEARER, doesn't apply." But the **CVE** is about a property that an *attacker* can inject through whichever surface allows config edits. Kafka Connect's REST API, in particular, has historically accepted arbitrary connector config — that's the canonical attack surface, mirroring CVE-2023-25194.
- The patch in 3.9.1 is "permissive by default for compatibility". A team that bumps to 3.9.1 and ticks the box is still vulnerable until they set the allow-list system property. The linter must flag this — version alone isn't the full fix.
- BOM-managed versions in Spring Boot 3.4.x and Quarkus 3.20 LTS were on `kafka-clients` 3.7–3.8 well into the post-CVE window. A team that bumped Boot's patch version but not the line saw no warning.

## Operational impact

- **Filesystem read** — anything the client process user can read. Kubernetes service account tokens, mounted secrets, cloud creds at `~/.aws/credentials`.
- **Internal SSRF** — `http://169.254.169.254/latest/meta-data/iam/security-credentials/...` on AWS, similar metadata endpoints on GCP / Azure.
- **Pivot from REST API access to env-var dump** in Kafka Connect: a user with permission to create connectors can read the entire process environment, often containing other systems' credentials.

## How to fix

```xml
<!-- BAD: client in the vulnerable range -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.7.1</version>   <!-- vulnerable -->
</dependency>

<!-- GOOD: 3.9.1+ or 4.x -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.9.2</version>
</dependency>
```

If you have to stay on 3.9.x, also set:

```
-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=https://my-idp.example.com/token,https://my-idp.example.com/jwks
```

For 4.x, the same system property is required to enable OAUTHBEARER at all.

## When this might be a false positive

- A producer/consumer with no external config surface (no Connect, no admin REST, no MBean-driven config reload), no OAUTHBEARER in use, and bytecode that doesn't construct admin clients from untrusted input. Even then, the upgrade is cheap — keep the rule on.

## Detection strategy

- Resolved `kafka-clients` version range `[3.1.0, 3.9.1)`.
- Also report whether the project has Kafka Connect modules (`io.confluent:kafka-connect-*`, `org.apache.kafka:connect-*`) — bumps the urgency.
- Optionally check JVM args / launcher scripts in the project for `-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls` — out of scope for a pom-only rule, mention in the violation message.

## References

- [Apache Kafka CVE list — CVE-2025-27817](https://kafka.apache.org/cve-list)
- [NVD entry for CVE-2025-27817](https://nvd.nist.gov/vuln/detail/CVE-2025-27817)
- [KIP-768 — Extend SASL/OAUTHBEARER with support for OIDC](https://cwiki.apache.org/confluence/display/KAFKA/KIP-768)
