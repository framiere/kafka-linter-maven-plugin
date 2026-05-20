# HEADERS_SENSITIVE_KEYS

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode
**Tagline**: Kafka headers are plaintext — don't put your password in them.

## TL;DR

The linter flags `RecordHeader` / `Headers.add` calls whose header key matches sensitive patterns (`password`, `token`, `secret`, `api-key`, `authorization`). Kafka record headers are not encrypted by default and are visible on the wire and in `kafka-console-consumer` output.

## What's happening (the mechanism)

`org.apache.kafka.common.header.Headers` are plain `byte[]` key/value pairs attached to each `ProducerRecord`. They:
- Flow through the broker unmodified.
- Are visible to any consumer with `read` ACL on the topic.
- Are printed verbatim by `kafka-console-consumer --property print.headers=true`.
- Appear in broker `kafka.request.logger` logs at TRACE level.
- Often end up in S3 / archival tooling (Kafka Connect, MirrorMaker2) verbatim.

Putting credentials in headers is a recurring leak vector — the team adds a "convenient" `authorization` header for downstream pickup, and now every consumer (legitimate or not) sees the bearer token.

## Operational impact

- Credential leak to every consumer in the cluster.
- Credential leak to archival storage (S3, GCS) where it persists indefinitely.
- Audit / SIEM tools that capture broker logs see the secret.

## How to fix

```java
// BAD
record.headers().add("authorization", bearerToken.getBytes(UTF_8));
record.headers().add("password", password.getBytes(UTF_8));

// GOOD — secrets travel out of band (mTLS / SASL / OAuth at the connection layer)
// Use headers for non-secret metadata only:
record.headers().add("trace-id", traceId.getBytes(UTF_8));
record.headers().add("source-system", "orders-api".getBytes(UTF_8));
```

If you must pass an auth token through Kafka (e.g., signing service tokens for downstream), encrypt at the application layer and rotate aggressively.

## When this might be a false positive

- Header keys that contain "token" but refer to non-secret idempotency tokens (`idempotency-token`, `trace-token`).
- Encrypted payload headers where the value is already ciphertext.

## Detection strategy

- Bytecode: `Headers.add(String, byte[])` / `new RecordHeader(String, byte[])` where the first arg is an `LDC` whose string contains case-insensitive: `password`, `secret`, `apikey`, `api_key`, `api-key`, `token`, `authorization`, `bearer`, `credential`. MEDIUM.
- Allow-list: `idempotency-token`, `trace-token`, `csrf-token`.

## References

- Apache Kafka — Record headers (KIP-82): https://cwiki.apache.org/confluence/display/KAFKA/KIP-82+-+Add+Record+Headers
- OWASP — Secrets Management Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
