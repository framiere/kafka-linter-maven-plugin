# DESER_SR_NO_AUTH_CREDENTIALS

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: Schema Registry without `basic.auth.credentials.source` is an HTTPS URL that says "trust me, no creds needed."

## TL;DR

The linter flags configs that set `schema.registry.url` to an HTTPS URL (or to a Confluent Cloud-style hostname matching `*.confluent.cloud`) but do not set `basic.auth.credentials.source` (or `bearer.auth.credentials.source` for SaaS deployments). Managed schema registries require API key auth; without these credentials, every schema fetch fails with 401. The first poison pill / first new schema → consumer dies with `RestClientException`.

## The setup

Team uses Confluent Cloud's Schema Registry. They configure `schema.registry.url=https://psrc-xxxxx.region.aws.confluent.cloud`. They wire `KafkaAvroDeserializer`. They forget the auth. Local test (Testcontainers) works because there's no auth. Staging against Confluent Cloud: 401 errors flooding the logs, every record fails to deserialize.

## What's actually happening

`io.confluent.kafka.serializers.KafkaAvroDeserializer` (and the Protobuf/JSON Schema variants) calls `SchemaRegistryClient` to fetch the schema by ID. The HTTP request requires authentication on any non-localhost deployment:

- **Confluent Cloud** uses HTTP Basic auth with API key/secret. Config:
  ```
  basic.auth.credentials.source=USER_INFO
  basic.auth.user.info=<key>:<secret>
  ```
- **On-prem Confluent Platform** with security can use Basic, OAuth, mTLS, or SASL.
- **Apicurio Registry** uses a different property scheme (`apicurio.auth.*`).

Without credentials, the registry returns 401, the deserializer throws, every record fails — without retry, the consumer becomes a 100%-error stream.

There's a second flavor — if you store credentials in a `.properties` file but use `basic.auth.credentials.source=URL`, the credentials come from the URL itself (`https://key:secret@registry`). Hardcoding secrets in URLs is its own anti-pattern (covered by `HEADERS_SENSITIVE_KEYS`-adjacent rule).

## Why this is subtle

- The error doesn't say "missing auth" loudly — it's a `RestClientException: 401 Unauthorized` inside a stack trace four levels deep.
- Local Testcontainers / `mock://` schema registry don't require auth, so dev/test passes.
- Confluent Cloud's onboarding docs are clear, but the snippet often gets copied without the auth lines.
- Spring Boot / Quarkus property namespacing for these keys is tricky: `spring.kafka.properties.basic.auth.credentials.source=USER_INFO` is correct; `spring.kafka.consumer.basic.auth.credentials.source=USER_INFO` is *not* — these are passed verbatim, not under the consumer prefix.

## Operational impact

- 100% deserialization failure rate.
- Logs flood with stack traces.
- Consumer either retries forever (if `LogAndContinueExceptionHandler` configured) or dies on first record (default `LogAndFailExceptionHandler` for Streams; same effect for Spring's `DefaultErrorHandler` after 10 retries).
- Schema migration is blocked — first new schema ID fails to fetch.

## Failure scenarios (walkthrough)

1. **The staging surprise.** Local dev with Docker Compose + Confluent local schema registry: no auth. Staging against Confluent Cloud: 401s. First the team thinks the schema isn't published. Then they think the URL is wrong. After two hours: missing auth config.

2. **The credential rotation.** Credentials are correctly set as env vars referenced in `application.yaml`. CI rotates them. Quarkus reads them at startup. K8s pod has stale credentials in its secret. Deploy → 401 → consumer dies → on-call paged.

## How to fix

```properties
# Confluent Cloud Schema Registry
spring.kafka.properties.schema.registry.url=https://psrc-xxxxx.region.aws.confluent.cloud
spring.kafka.properties.basic.auth.credentials.source=USER_INFO
spring.kafka.properties.basic.auth.user.info=${SR_KEY}:${SR_SECRET}
```

```yaml
# Quarkus
quarkus:
  kafka:
    schema:
      registry:
        url: https://psrc-xxxxx.region.aws.confluent.cloud
    properties:
      basic.auth.credentials.source: USER_INFO
      basic.auth.user.info: ${SR_KEY}:${SR_SECRET}
```

```java
// Programmatic
props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl);
props.put(KafkaAvroDeserializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
props.put(KafkaAvroDeserializerConfig.USER_INFO_CONFIG, key + ":" + secret);
```

## When this might be a false positive

- On-prem Confluent Platform with mTLS configured at the JVM level (different auth path).
- Self-hosted Apicurio Registry using its own auth config namespace.
- Localhost / private network registry with no auth — explicit and acceptable.

## Detection strategy

- config-file: parse for `schema.registry.url` value.
- If the URL starts with `https://` and is not `localhost`/`127.0.0.1`, check for `basic.auth.credentials.source` OR `bearer.auth.credentials.source` OR mTLS hints (`schema.registry.ssl.keystore.location`).
- If URL host matches `*.confluent.cloud` and `basic.auth.user.info` is absent, flag.
- Confidence MEDIUM — alternate auth paths and on-prem variants make this a heuristic.

## References

- Confluent — Schema Registry security overview: https://docs.confluent.io/platform/current/schema-registry/security/index.html
- Confluent Cloud — Configure Schema Registry client auth: https://docs.confluent.io/cloud/current/get-started/schema-registry.html
- Confluent Kafka Avro Deserializer config: https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-avro.html
