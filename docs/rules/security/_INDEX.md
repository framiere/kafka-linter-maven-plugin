# security rules

Build-time anti-pattern checks for Kafka client and Schema Registry security configuration — transport protocol, TLS handshake, SASL mechanism, credential hygiene, and deserialization safety. Rules scan both bytecode (`Properties` literals, `ProducerConfig` / `ConsumerConfig` map puts) and property sources (`application.properties`, `application.yml`, Connect connector configs). Targets Apache Kafka 3.x and 4.x.

## Status

This category currently has **23 rules registered in the runtime** (`src/main/java/io/conductor/kafkalinter/RuleId.java`) and **1 per-rule standalone doc file** (`DESER_JSON_TYPE_INFO_NO_ALLOWLIST.md`). The remaining 22 rules carry their full didactic prose inline in `RuleId.message(...)` and that prose is printed verbatim into the violation message. An operator hitting one of these rules in the Maven log will see the full explanation without needing to open a doc file.

The standalone doc backlog is tracked under the doc-tree-vs-registry drift section of [`../_CATALOG.md`](../_CATALOG.md#known-drift-doc-tree-vs-registry).

## Rule families

The 23 rules cluster into the following families. Each family targets a specific attack surface or operational footgun.

### Transport in cleartext

- **`SECURITY_PROTOCOL_PLAINTEXT`** — `security.protocol=PLAINTEXT` ships data, keys, and credentials over the wire in cleartext. The only legitimate value in production is `SSL` or `SASL_SSL`.
- **`SECURITY_PROTOCOL_SASL_PLAINTEXT`** — `SASL_PLAINTEXT` authenticates the client but does not encrypt the data path. SASL credentials and record payloads travel cleartext; an on-path observer recovers both.
- **`SCHEMA_REGISTRY_URL_HTTP`** — `schema.registry.url=http://...` defeats every guarantee provided by the registry's bearer-auth / basic-auth chain. Use `https://`.
- **`SECURITY_SASL_OAUTHBEARER_TOKEN_ENDPOINT_HTTP`** — OAuth token endpoint over HTTP is a credential-leaking misconfiguration; tokens travel cleartext on every refresh.

### Weak TLS

- **`SECURITY_SSL_PROTOCOL_LEGACY`**, **`SECURITY_SSL_ENABLED_PROTOCOLS_LEGACY`**, **`SECURITY_SSL_CIPHER_SUITES_LEGACY`** — TLS 1.0 / 1.1 / SSLv3 / RC4 / 3DES / NULL ciphers; legacy primitives that have CVEs or no defence-in-depth. Production posture is `TLSv1.2`+, modern AEAD suites only.
- **`SSL_ENDPOINT_IDENTIFICATION_DISABLED`** — `ssl.endpoint.identification.algorithm=` (empty) disables hostname verification; the client accepts any certificate signed by the truststore CAs, regardless of whether the broker DNS name matches the cert SAN. Renders TLS-MITM-resistant only against attackers outside the CA chain.

### SASL mechanism

- **`SECURITY_SASL_MECHANISM_PLAIN`** — `SASL/PLAIN` sends the password to the broker on every authentication; broker logs may contain credential material. Prefer `SCRAM-SHA-512` or `OAUTHBEARER`.

### Keystore / truststore hygiene

- **`SECURITY_SSL_KEYSTORE_TYPE_JKS`** — JKS keystores are vulnerable to brute-force password recovery (CVE-2017-1000487). Use PKCS12.
- **`SECURITY_SSL_KEYSTORE_LOCATION_TMP`**, **`SECURITY_SSL_TRUSTSTORE_LOCATION_TMP`** — keystores in `/tmp` are world-readable on most filesystems and disappear across reboots. Move to a deployment-managed secret-mount path.

### Credential literals in code

- **`CRED_SASL_JAAS_LITERAL`** — `sasl.jaas.config="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"...\" password=\"...\";"` literal in code or config-file. Use a config-provider (`config.providers=secrets`, `${secrets:/path:key}`) instead.
- **`CRED_BASIC_AUTH_USER_INFO_LITERAL`** — `basic.auth.user.info=user:pass` literal for Schema Registry auth.
- **`CRED_SR_BEARER_AUTH_TOKEN_LITERAL`** — `bearer.auth.token=<jwt>` literal.
- **`CRED_SSL_KEYSTORE_PASSWORD_LITERAL`** — `ssl.keystore.password=<pwd>` literal.
- **`CRED_AWS_CREDENTIAL_LITERAL`** — AWS access key / secret literals (Connect connectors, MSK IAM auth).

### Deserialization safety

- **`DESER_JSON_TYPE_INFO_NO_ALLOWLIST`** — JsonDeserializer with `JsonTypeInfo` polymorphism but no allow-list. Untrusted type names instantiate arbitrary classes (CVE-2017-7525, CVE-2017-15095, CVE-2017-17485 family). See [`DESER_JSON_TYPE_INFO_NO_ALLOWLIST.md`](DESER_JSON_TYPE_INFO_NO_ALLOWLIST.md) — the only fully written security rule today.
- **`DESER_SR_NO_AUTH_CREDENTIALS`** — Schema Registry client configured without auth on a registry that requires it; production deployments behind auth catch this at first deserialize and fail closed, but staging/test environments mask the gap.

### Schema-registry compatibility & auto-register

- **`SR_AUTO_REGISTER_SCHEMAS_TRUE`** — `auto.register.schemas=true` in production lets any client register a schema, defeating the compatibility-control layer. Producers register schemas at deploy time via CI; runtime registration is a privilege escalation.
- **`SR_USE_LATEST_VERSION_MISSING`**, **`SR_USE_LATEST_VERSION_TRUE`**, **`SR_LATEST_COMPATIBILITY_STRICT_FALSE`** — the compatibility-floor toggles that decide whether the client respects the subject's registered evolution policy or silently writes against an arbitrary schema.

## How to find a rule's prose

```bash
grep -A12 'RuleId.<SECURITY_RULE_NAME>' src/main/java/io/conductor/kafkalinter/RuleId.java
```

The `.message(...)` block carries the full didactic body. The `.tagline(...)`, `.mechanism(...)`, `.impact(...)`, and `.whyMatters(...)` blocks decompose that prose into the four canonical didactic blocks.
