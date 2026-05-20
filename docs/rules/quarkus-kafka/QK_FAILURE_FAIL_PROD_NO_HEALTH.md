# QK_FAILURE_FAIL_PROD_NO_HEALTH

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: `failure-strategy=fail` in production with no health-driven restart and no DLQ on the side is "one bad record = service offline until I notice."

## TL;DR

The linter flags Quarkus / SmallRye channels with `failure-strategy=fail` (the default) **on production profiles** where (a) `quarkus.smallrye-health` is disabled, or (b) the channel is not paired with a delayed-retry-topic / sibling DLQ channel. When the channel fails, it stays stopped. Without health-check integration, the orchestrator (K8s, OpenShift) won't restart the pod. The service appears up, but the channel is dead.

## The setup

Engineer keeps the default `failure-strategy=fail` because "I want to know when something breaks." First poison pill arrives; the channel goes into FAILED state. The HTTP endpoints still work. The Kubernetes liveness probe — which the team forgot to wire to `q/health/live` — sees the pod responding to /q/health and decides everything is fine. The channel sits there dead for hours.

## What's actually happening

`failure-strategy=fail` causes the SmallRye Reactive Messaging Kafka connector to:
1. Log the failure.
2. NOT commit the offset of the failing record.
3. Set the channel to a FAILED state.
4. Stop polling.

Channel state is exposed via the SmallRye Health check (`io.smallrye.reactive.messaging.health.HealthReport`) which Quarkus' MicroProfile Health integration uses. If `quarkus.smallrye-health` is enabled, the `live` endpoint will reflect "channel X is in FAILED state" and return 503. The K8s liveness probe — if pointed at `/q/health/live` — will restart the pod, which restarts the channel from the last committed offset (the poison record again). The new pod fails the same way. After enough restarts, K8s gives up; alerting fires; on-call investigates.

This is the *intended* `fail` workflow: orchestrator-driven restart + alerting on restart-loop. It requires:

- `quarkus-smallrye-health` extension on the classpath.
- K8s `livenessProbe` pointed at `/q/health/live`.
- An alert on pod restart rate.

Missing any of these and `failure-strategy=fail` becomes "silent service degradation."

## Why this is subtle

- `fail` is the default — engineers don't even know they "chose" it.
- The HTTP layer keeps working — `/api/orders` returns 200. The only sign of trouble is in the Kafka consumer group lag.
- `quarkus-smallrye-health` is an opt-in extension. Quarkus dev mode shows it, but production builds without it omit the integration.
- The liveness probe must point at `/q/health/live` specifically (not `/q/health` which is `/health/`-aggregate).
- Reading-only services with no producer side rarely think about consumer-side health. The mental model is "if the HTTP works, we're fine."

## Operational impact

- Consumer lag grows indefinitely.
- Service appears healthy via every dashboard except lag.
- "Restart pods" works briefly until the poison record is hit again.
- Customer impact: no new data processed.

## Failure scenarios (walkthrough)

1. **The missing extension.** Service is built with `quarkus-smallrye-reactive-messaging-kafka` but not `quarkus-smallrye-health`. `failure-strategy=fail` (default). Poison pill arrives. Channel stops. K8s liveness probe at `/q/health/live` returns 404. K8s treats 404 as unknown → no restart. Lag grows. Customer notices.

2. **The wrong probe target.** Health extension present; liveness probe points at `/api/healthz` (a custom controller). Channel fails. `/api/healthz` still returns 200 because the controller doesn't know about messaging health. No restart.

## How to fix

Pick one of three legitimate strategies:

### A. Keep `fail`, but wire health properly

```xml
<dependency>
    <groupId>io.quarkus</groupId>
    <artifactId>quarkus-smallrye-health</artifactId>
</dependency>
```

```yaml
# K8s
livenessProbe:
  httpGet:
    path: /q/health/live
    port: 8080
  periodSeconds: 10
  failureThreshold: 3
readinessProbe:
  httpGet:
    path: /q/health/ready
    port: 8080
```

Plus a Prometheus alert on `kube_pod_container_status_restarts_total` rate.

### B. Use a DLQ to keep moving while preserving failures

```properties
mp.messaging.incoming.orders.failure-strategy=dead-letter-queue
mp.messaging.incoming.orders.dead-letter-queue.topic=orders.dlt
```

### C. Use delayed-retry-topic for transient failures

```properties
mp.messaging.incoming.orders.failure-strategy=delayed-retry-topic
mp.messaging.incoming.orders.delayed-retry-topic.topics=orders.retry.5s,orders.retry.30s,orders.retry.5m
mp.messaging.incoming.orders.delayed-retry-topic.max-retries=5
```

## When this might be a false positive

- Internal batch job where "stop and alert" is the right behavior and `kube-state-metrics` alerting is already in place.
- Service where lag is irrelevant (e.g., one-shot historical replay).

## Detection strategy

- config-file: identify channels with `failure-strategy=fail` (or no explicit `failure-strategy`).
- pom-dependency: check for `io.quarkus:quarkus-smallrye-health` on the classpath.
- Flag if `failure-strategy=fail` AND `quarkus-smallrye-health` is absent.
- Also flag if `failure-strategy=fail` AND `quarkus.kubernetes.liveness-probe.http-action-path` is set to something other than `/q/health/live` (when present in `application.properties`).
- Confidence MEDIUM — K8s probe configuration is often outside Maven scope (Helm chart, raw YAML) — we can only see the pom+config.

## References

- Quarkus — Reactive Messaging Kafka failure strategies: https://quarkus.io/guides/kafka#failure-strategy
- Quarkus SmallRye Health: https://quarkus.io/guides/smallrye-health
- SmallRye Reactive Messaging — Health checks: https://smallrye.io/smallrye-reactive-messaging/latest/concepts/health-checks/
