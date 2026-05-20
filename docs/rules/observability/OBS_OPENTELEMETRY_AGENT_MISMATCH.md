# OBS_OPENTELEMETRY_AGENT_MISMATCH

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: pom-dependency + config-file
**Tagline**: Declaring `opentelemetry-api` without the agent *or* the Kafka instrumentation library is owning the boxing gloves without ever stepping in the ring.

## TL;DR

The linter flags projects whose `pom.xml` includes `io.opentelemetry:opentelemetry-api` (or `io.opentelemetry.api.OpenTelemetry` is referenced in code) but ships neither (a) the OpenTelemetry Java agent declared in Surefire/Failsafe `<argLine>`, nor (b) the `opentelemetry-kafka-clients-*` library dependency with explicit interceptor wiring. The result: code compiles and references OTel APIs, but Kafka producer/consumer hops produce no spans.

## The setup

Team adopts OpenTelemetry. They bring in `opentelemetry-api`, write `Tracer tracer = openTelemetry.getTracer("my-service");`, instrument the HTTP layer, and see HTTP spans. They assume Kafka is automatically instrumented because they're "using OTel." It isn't — Kafka instrumentation requires either the Java agent (which patches `KafkaProducer.send` reflectively) or the library-mode artifact `opentelemetry-kafka-clients-2.6` plus an explicit interceptor or `KafkaTelemetry.wrap(producer)` call.

## What's actually happening

OpenTelemetry Java has two distinct distribution models:

1. **Agent mode (`opentelemetry-javaagent.jar`).** Auto-instruments most popular libraries including Kafka clients. Requires `-javaagent:opentelemetry-javaagent.jar` on the JVM command line. The agent ships its own copies of instrumentation modules, so you don't need to add anything to `pom.xml`.

2. **Library mode (`opentelemetry-kafka-clients-2.6`).** You explicitly wire the interceptor via `interceptor.classes` or wrap the producer/consumer via `KafkaTelemetry.wrap()`. Requires the library artifact in `pom.xml`.

Pulling in only `opentelemetry-api` gives you the API surface (`Tracer`, `Span`, `Context`) but no actual instrumentation. Code compiles, runtime works, no Kafka spans emitted.

## Why this is subtle

- `opentelemetry-api` does not warn at startup that no SDK or agent is configured. By default it returns a no-op `Tracer` — every span is silently dropped.
- The agent installation is in deployment YAML / Dockerfile / `argLine`, not in `pom.xml`. A static scan of `pom.xml` can't see it. We can only see the *absence* of the library-mode artifact and *flag* if the agent isn't visible in `<argLine>`.
- Teams often mix: `opentelemetry-api` + `opentelemetry-sdk` + `opentelemetry-exporter-otlp` — looks complete — but the Kafka-specific module is missing.

## Operational impact

- HTTP spans show in the OTel collector. Kafka send/receive: silently absent.
- Records sent without `traceparent` header — downstream consumers start a new trace.
- `Tracer.spanBuilder("kafka-send").startSpan()` written in user code does emit a span, but it's not parented to any real Kafka operation and won't reflect actual broker latency.

## Failure scenarios (walkthrough)

1. **The "we have OTel" assumption.** Slack message: "We have OpenTelemetry, why do Kafka traces stop?" Investigation reveals `opentelemetry-api` in pom but no agent JAR baked into the image and no `opentelemetry-kafka-clients-2.6` dependency. Team has been operating on the assumption that adding `opentelemetry-api` instruments everything.

2. **The migration gap.** Team migrates from Brave to OTel. They remove `brave-kafka-interceptor` from `interceptor.classes` and add `opentelemetry-exporter-otlp`. They forget to add the Kafka library artifact or update `interceptor.classes` to the OTel equivalent. Trace coverage drops to zero on the Kafka hop.

## How to fix

Pick one of the two models — don't mix.

```xml
<!-- OPTION A — Agent mode -->
<!-- pom.xml: only API/SDK if you also do programmatic API calls -->
<dependency>
  <groupId>io.opentelemetry</groupId>
  <artifactId>opentelemetry-api</artifactId>
</dependency>
<!-- Deployment must include: -javaagent:/path/to/opentelemetry-javaagent.jar -->

<!-- OPTION B — Library mode -->
<dependency>
  <groupId>io.opentelemetry.instrumentation</groupId>
  <artifactId>opentelemetry-kafka-clients-2.6</artifactId>
  <version>2.25.0-alpha</version>
</dependency>
```

```properties
# OPTION B (cont.) — wire the interceptor
spring.kafka.producer.properties.interceptor.classes=\
  io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingProducerInterceptor
spring.kafka.consumer.properties.interceptor.classes=\
  io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingConsumerInterceptor
```

## When this might be a false positive

- Agent declared in deployment manifests we can't see (Kubernetes Dockerfile `ENTRYPOINT` with `-javaagent`).
- Project uses `quarkus-opentelemetry` — Quarkus wires Kafka instrumentation via its own auto-config.
- Spring Boot 3.x with `micrometer-tracing-bridge-otel` and Spring Kafka 3.0+ `observation-enabled=true` — the Micrometer-OTel bridge picks up spans from the listener container.

## Detection strategy

- pom-dependency: presence of `io.opentelemetry:opentelemetry-api` (transitive or direct).
- pom-dependency absence: `io.opentelemetry.instrumentation:opentelemetry-kafka-clients-*` AND `io.opentelemetry.javaagent:opentelemetry-javaagent` AND `io.opentelemetry.instrumentation:opentelemetry-spring-boot-starter`.
- pom analysis: scan `<argLine>` in Surefire/Failsafe `<configuration>` for `-javaagent:.*opentelemetry-javaagent`.
- bytecode: `OpenTelemetry.getTracer(` call sites without any other Kafka instrumentation.
- Confidence MEDIUM — agent declaration outside pom is invisible.

## References

- OpenTelemetry Java agent: https://opentelemetry.io/docs/zero-code/java/agent/
- `opentelemetry-kafka-clients-2.6`: https://central.sonatype.com/artifact/io.opentelemetry.instrumentation/opentelemetry-kafka-clients-2.6
- OpenTelemetry — Instrumenting Apache Kafka clients (blog): https://opentelemetry.io/blog/2022/instrument-kafka-clients/
- OpenTelemetry supported libraries: https://opentelemetry.io/docs/zero-code/java/agent/supported-libraries/
