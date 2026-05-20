# STREAMS_NO_HEALTH_ENDPOINT
**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: bytecode + pom-dependency
**Tagline**: Without a `/health/ready` probe, Kubernetes restarts your Streams app during normal rebalances.
**Source**: Confluent agent-skills — kafka-streams-programming/references/production-hardening.md § Health Checks.

## TL;DR

The linter flags Kafka Streams applications targeting Kubernetes / Docker / orchestrated environments that do not expose a `/health/live` and `/health/ready` HTTP endpoint backed by `KafkaStreams.state()`. The distinction matters: during a rebalance, the app is **alive** (don't kill it) but not **ready** (don't send traffic to interactive-query endpoints). Without separate probes, Kubernetes restarts pods during normal rebalances — causing cascading restarts and false outages.

## What's happening (the mechanism)

`KafkaStreams.State` has seven values:
`CREATED → REBALANCING → RUNNING → PENDING_SHUTDOWN → NOT_RUNNING` (normal path)
`RUNNING → PENDING_ERROR → ERROR` (failure path)

Confluent's hardening guide prescribes:
- **Liveness** (`/health/live`): `state != ERROR && state != NOT_RUNNING` → 200, else 503. Kubernetes restarts the pod when this fails.
- **Readiness** (`/health/ready`): `state == RUNNING` → 200, else 503. Kubernetes stops sending traffic when this fails.

If only one probe is exposed — or worse, the same probe is wired to both — the orchestrator can't distinguish a transient `REBALANCING` from a terminal `ERROR`. Common failure modes:

1. The dev wires `/health` returning 200 always (because "the JVM is up"). K8s never restarts crashed Streams instances → silent dead pods.
2. The dev wires both probes to `state == RUNNING`. K8s restarts the pod during every rebalance → cascading restart loop where the new pod also rebalances on join → never reaches RUNNING.

The skill ships a copy-paste `HealthCheckServer` using JDK's `com.sun.net.httpserver.HttpServer` — no Spring, no Quarkus, no extra dependency.

## Operational impact

- Cascading restart during rolling deploys: each rolling-restart triggers a rebalance → readiness probe fails → K8s thinks pod is broken → restarts it → another rebalance.
- Crashed pods that don't restart: liveness probe returns 200 forever even after `ERROR` state.
- Traffic routed to instances mid-rebalance: interactive query endpoints return `InvalidStateStoreException` to the caller (see `STREAMS_INTERACTIVE_QUERY_BEFORE_RUNNING`).

## How to fix (bad → good code)

```java
// BAD — no health endpoint at all
KafkaStreams streams = new KafkaStreams(topology, props);
streams.start();

// BAD — single endpoint used for both probes
server.createContext("/health", exchange -> {
    boolean ok = streams.state() == KafkaStreams.State.RUNNING;
    exchange.sendResponseHeaders(ok ? 200 : 503, 0);
    ...
});

// GOOD — two endpoints, two semantics (lifted from Confluent's production-hardening.md)
HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
server.createContext("/health/live", exchange -> {
    KafkaStreams.State s = streams.state();
    boolean alive = s != KafkaStreams.State.ERROR && s != KafkaStreams.State.NOT_RUNNING;
    int status = alive ? 200 : 503;
    sendJson(exchange, status, "{\"status\":\"" + (alive ? "UP" : "DOWN") + "\"}");
});
server.createContext("/health/ready", exchange -> {
    boolean ready = streams.state() == KafkaStreams.State.RUNNING;
    int status = ready ? 200 : 503;
    sendJson(exchange, status, "{\"status\":\"" + (ready ? "UP" : "DOWN") + "\"}");
});
server.setExecutor(null);
server.start();
streams.start();
```

K8s deployment snippet (from Confluent):

```yaml
livenessProbe:
  httpGet: { path: /health/live, port: 8080 }
  initialDelaySeconds: 30
  periodSeconds: 10
readinessProbe:
  httpGet: { path: /health/ready, port: 8080 }
  initialDelaySeconds: 30
  periodSeconds: 10
```

## When this might be a false positive

- A non-orchestrated deployment (single VM, systemd) where probes don't matter.
- The app uses Spring Boot Actuator or Quarkus health, with a `HealthIndicator` / `HealthCheck` wired to `KafkaStreams.state()`. Detect via the dependency on `org.springframework.boot:spring-boot-starter-actuator` + a class extending `AbstractHealthIndicator` referencing `KafkaStreams`. Skip the lint in that case.
- The app uses a different convention (`/actuator/health/liveness`, `/q/health/ready`). Detect via the actuator dependency and Spring/Quarkus health-check classes.

## Detection strategy

- Bytecode: `KafkaStreams` instance is started, but no `HttpServer.create` call references `KafkaStreams.state()`; AND no `HealthIndicator` / `HealthCheck` class references it.
- pom: no `spring-boot-starter-actuator`, no `quarkus-smallrye-health`, no `micrometer-registry-prometheus`-wired health endpoint.
- Confidence: MEDIUM. False-positive surface is high because many teams use framework health subsystems; calibrate by inspecting actuator/health dependencies first.

## References

- Confluent agent-skills — kafka-streams-programming/references/production-hardening.md § Health Checks
- Apache Kafka docs — `KafkaStreams.State`: https://kafka.apache.org/40/javadoc/org/apache/kafka/streams/KafkaStreams.State.html
- Related rules: [STREAMS_INTERACTIVE_QUERY_BEFORE_RUNNING](../kafka-streams/STREAMS_INTERACTIVE_QUERY_BEFORE_RUNNING.md), [QK_HEALTH_DISABLED](../quarkus-kafka/QK_HEALTH_DISABLED.md)
