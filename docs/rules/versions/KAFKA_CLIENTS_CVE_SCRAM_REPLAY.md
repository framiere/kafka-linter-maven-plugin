# KAFKA_CLIENTS_CVE_SCRAM_REPLAY

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: CVE-2024-56128 — SCRAM without TLS is a replay attack waiting to happen.

## TL;DR

`kafka-clients` and brokers in the affected range mishandle SCRAM challenge nonces in a way that allows authentication replay when the connection is not also TLS-protected. **CVE-2024-56128** does not require an upgrade in the strict sense — it requires turning on `SSL` for the listener — but versions before the fix make the failure-mode worse.

## The setup

A team picked SASL/SCRAM (typical for "we want auth but the corporate PKI is a nightmare") and a `security.protocol=SASL_PLAINTEXT` listener (typical for "it's an internal network"). On an internal network with a hostile actor on the wire — a compromised peer pod, a SPAN port tap, a malicious sidecar — the SCRAM exchange can be captured and replayed within the challenge-nonce reuse window.

## What's actually happening

SCRAM (SASL Challenge-Response Authentication Mechanism, RFC 5802) relies on per-exchange nonces to prevent replay. In the affected Kafka range, the client-side nonce handling allowed two distinct authentication attempts in close succession to reuse derived material, enabling a man-in-the-middle to replay the captured exchange and impersonate the client.

The fix tightens nonce handling but the **real** mitigation is to use `SASL_SSL` instead of `SASL_PLAINTEXT`. The CVE exists precisely because the latter is a common configuration.

## Why this is subtle

- Teams perceive SCRAM as "the strong one" and assume TLS is optional with it. The CVE proves otherwise.
- "Internal network" assumptions break down in shared Kubernetes namespaces, multi-tenant VPCs, and zero-trust environments.
- The fix is largely defensive; the CVE-issuing party (Apache) is sending a signal: **stop using SASL_PLAINTEXT**.

## Operational impact

- **Authentication impersonation** — an attacker who can capture one SCRAM exchange can establish an authenticated session as that user.
- **Cluster-wide pivot** — once authenticated, the attacker has whatever ACLs the impersonated user has. Service accounts often have broad rights.

## How to fix

Two layers:

1. Upgrade `kafka-clients` to a patched line (3.9.1+ / 4.x).
2. Switch `security.protocol` from `SASL_PLAINTEXT` to `SASL_SSL` everywhere. This is the real fix.

```properties
# BAD
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512

# GOOD
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
ssl.truststore.location=/etc/kafka/ssl/truststore.jks
ssl.truststore.password=...
```

## When this might be a false positive

- The deployment uses `SASL_SSL` or `SSL` everywhere (TLS prevents the replay).
- A truly isolated network with cryptographically authenticated peers (rare; almost always overestimated).

## Detection strategy

- Two signals together:
  - Resolved `kafka-clients` version `< 3.9.1`.
  - Config file or bytecode showing `security.protocol=SASL_PLAINTEXT` plus `sasl.mechanism=SCRAM-*`.
- If only the version is in range but the protocol is `SASL_SSL`, downgrade to informational.

## References

- [Apache CVE list — CVE-2024-56128](https://kafka.apache.org/cve-list)
- [NVD entry for CVE-2024-56128](https://nvd.nist.gov/vuln/detail/CVE-2024-56128)
- [RFC 5802 — SCRAM](https://datatracker.ietf.org/doc/html/rfc5802)
