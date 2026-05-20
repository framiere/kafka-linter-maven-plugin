# SECURITY_PROTOCOL_PLAINTEXT_REMOTE

**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: config-file
**Tagline**: Plaintext to a hostname you don't own is a credential leak in a green box.

## TL;DR

The linter flags `security.protocol=PLAINTEXT` (or unset, which defaults to PLAINTEXT) when `bootstrap.servers` points to a non-local hostname — likely a managed or remote cluster requiring TLS + auth.

## What's happening (the mechanism)

`security.protocol` selects the wire-level security layer between the client and broker:

- `PLAINTEXT` (default): no TLS, no auth — anyone on the network sees the bytes.
- `SSL`: TLS, no auth (mutual TLS optional).
- `SASL_PLAINTEXT`: SASL auth over plaintext (auth visible on the wire — almost never right outside trusted networks).
- `SASL_SSL`: SASL over TLS — the standard for cloud-managed Kafka.

Managed Kafka services (Confluent Cloud, AWS MSK with SASL/SCRAM, Azure Event Hubs Kafka facade, Aiven) all require `SASL_SSL` with credentials. Connecting with the default PLAINTEXT either:
- Fails the TCP handshake fast (best case).
- Or silently sends credentials in the SASL plain mechanism over an unencrypted channel.

## Operational impact

- Authentication leak if the broker is misconfigured to accept PLAINTEXT on a public listener.
- Silent failure mode in dev → prod transitions when local Docker uses PLAINTEXT but prod is SASL_SSL.
- Compliance violation (PCI, HIPAA, GDPR) when traffic is unencrypted on a public network.

## How to fix

```properties
# BAD — managed cluster, no security config
bootstrap.servers=pkc-XXXXX.us-east-1.aws.confluent.cloud:9092

# GOOD
bootstrap.servers=pkc-XXXXX.us-east-1.aws.confluent.cloud:9092
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="<API-KEY>" password="<API-SECRET>";
```

For mTLS managed clusters: `security.protocol=SSL` plus `ssl.keystore.location`, `ssl.truststore.location`.

## When this might be a false positive

- Local dev clusters (`localhost`, `127.0.0.1`, `kafka.svc.cluster.local`).
- Internal-only VPC clusters where network-layer encryption is provided (rare in 2026).

## Detection strategy

- Config: `bootstrap.servers` hostname does NOT resolve to a private/loopback range or known internal suffix AND `security.protocol` is unset or `PLAINTEXT`. CONTEXT (cannot prove without resolving DNS).
- Heuristic suffixes that imply managed: `*.aws.confluent.cloud`, `*.azure.confluent.cloud`, `*.servicebus.windows.net`, `*.amazonaws.com`, `*.aivencloud.com`, `*.cloud.redpanda.com`.
- Heuristic prefixes that imply local: `localhost`, `127.0.0.1`, `0.0.0.0`, `kafka.svc.cluster.local`, `*.local`.

## References

- Apache Kafka — Security: https://kafka.apache.org/documentation/#security
- Apache Kafka — `security.protocol`: https://kafka.apache.org/documentation/#producerconfigs_security.protocol
- Confluent Cloud client config: https://docs.confluent.io/cloud/current/client-apps/config-client.html
