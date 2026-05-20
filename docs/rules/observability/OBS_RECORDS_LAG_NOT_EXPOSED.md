# OBS_RECORDS_LAG_NOT_EXPOSED

**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: pom-dependency + config-file
**Tagline**: If `kafka_consumer_fetch_manager_records_lag_max` isn't in your scrape, no alert in the world will fire on lag.

## TL;DR

The linter flags consumer projects with no clear path for `records-lag-max` (the per-partition consumer lag gauge, exposed via JMX MBean `kafka.consumer:type=consumer-fetch-manager-metrics`) to reach a metrics backend. Even with Micrometer wired, the metric requires the `client.id` tag to disambiguate consumers and the JMX bridge or `KafkaClientMetrics` binding to forward it. Without these, the single most important consumer health signal is unobservable.

## The setup

A team adds a consumer. They want to know when it falls behind. They open Grafana and search for `lag` — and find nothing. They search Prometheus for `records_lag_max` — nothing. They eventually read the JMX MBean spec and discover that the metric was always there, but the bridge to Prometheus was never built.

## What's actually happening

`records-lag-max` (and per-partition `records-lag`) is reported by `Fetcher` after every poll. It's the difference between the partition's high-water-mark and the consumer's position. The metric lives at:

```
kafka.consumer:type=consumer-fetch-manager-metrics,client-id=<id>
  attribute: records-lag-max
kafka.consumer:type=consumer-fetch-manager-metrics,client-id=<id>,topic=<t>,partition=<p>
  attribute: records-lag
```

To reach Prometheus, one of these must be true:

1. JMX exporter (`jmx_prometheus_javaagent`) attached to the JVM with a pattern matching `consumer-fetch-manager-metrics`.
2. Micrometer `KafkaClientMetrics` bound to a `PrometheusMeterRegistry` (creates meters `kafka.consumer.fetch.manager.records.lag.max`).
3. OpenTelemetry library/agent with metrics enabled (exposes `kafka.consumer.records.lag.max` via OTLP).
4. Confluent/Datadog/custom `MetricsReporter`.

Two caveats:

- **Per-partition vs aggregated.** Micrometer's `KafkaClientMetrics` exports `records-lag-max` per `client-id` tag, **not per topic-partition**. If you have many partitions, the max-of-partitions view is what you get; the per-partition view requires a JMX scraper with `partition` label preservation (see jmx_exporter issue #609 / micrometer issue #878).
- **Rebalance freeze.** During a rebalance, the consumer group does not update lag values — they appear "frozen". Alert thresholds need `for: 5m` or longer to ride out rebalances.

## Why this is subtle

- `records-lag-max` is the *aggregate* (max across this consumer's assigned partitions). It looks like a single number — engineers assume it covers the topic.
- `records-lag-max` *drops* (returns 0) when no records are fetched in a poll period, which can look like "all caught up" when actually no consumption is happening at all. The right alert pairs lag with a non-zero `records-consumed-rate`.
- Spring Boot's auto-config exposes the metric for the *default* factory; custom factories lose it (see `OBS_SPRING_MICROMETER_NOT_BOUND`).
- Some teams alert only on broker-side `kafka_consumergroup_lag` (from JMX exporter on broker). That works, but lags by the metric scrape interval and doesn't reflect per-consumer-instance variance.

## Operational impact

- Consumer falls behind silently. Users see stale data. Page comes from a customer, not from monitoring.
- Rebalance loops mask themselves as "no traffic" because lag freezes.
- Capacity planning for partition counts is impossible without per-partition lag.

## Failure scenarios (walkthrough)

1. **The hung listener.** A `@KafkaListener` calls a slow downstream service that hangs at 5 minutes. `max.poll.interval.ms` is 300s, so the consumer is bounced and rejoined every 5 minutes. Lag grows monotonically. No metric → no alert → users notice after 4 hours.

2. **The slow rebalance.** Service deploys a new version. Rolling restart triggers rebalance after rebalance. During each rebalance, lag readings freeze. The metric appears to flatline at a normal value the entire deploy. Real lag at the end: 4 hours. The team learns from a customer escalation that "data feels stale."

## How to fix

Pick one of the four exposure paths:

```yaml
# Path 1 — JMX exporter as Java agent (most portable)
# JVM args:
#   -javaagent:/opt/jmx_exporter.jar=9404:/opt/jmx_config.yml
# jmx_config.yml:
rules:
  - pattern: kafka.consumer<type=(.+), client-id=(.+)><>(records-lag-max)
    name: kafka_$1_$3
    labels:
      client_id: "$2"
  - pattern: kafka.consumer<type=(.+), client-id=(.+), topic=(.+), partition=(.+)><>(records-lag)
    name: kafka_$1_$5
    labels:
      client_id: "$2"
      topic: "$3"
      partition: "$4"
```

```java
// Path 2 — Micrometer
@Bean
public ConsumerFactory<String, Order> consumerFactory(MeterRegistry registry) {
    DefaultKafkaConsumerFactory<String, Order> cf = new DefaultKafkaConsumerFactory<>(props);
    cf.addListener(new MicrometerConsumerListener<>(registry));
    return cf;
}
```

Pair the metric with a sensible alert:

```yaml
# Prometheus alert
- alert: KafkaConsumerLagHigh
  expr: kafka_consumer_records_lag_max > 1000
          and kafka_consumer_records_consumed_rate > 0
  for: 5m
  labels: { severity: warning }
```

## When this might be a false positive

- A higher-level platform (Confluent Cloud, Strimzi with cruise-control-lag-exporter, Burrow, Kowl) computes lag from the broker side independently.
- Team explicitly uses `kafka-consumer-groups.sh` polling + custom exporter.

## Detection strategy

- pom-dependency: `org.apache.kafka:kafka-clients` AND no observability dependency that would expose lag (`micrometer-core` + `MicrometerConsumerListener` reference in bytecode; OR `opentelemetry-kafka-clients-*`; OR `io.prometheus.jmx:jmx_prometheus_javaagent`; OR `io.confluent:kafka-streams-confluent-monitoring-interceptors`).
- config-file: presence of `metric.reporters` consumer-side counts as "exposed somehow".
- pom analysis: `-javaagent:.*jmx_prometheus_javaagent` in `<argLine>` counts as exposed.
- Confidence CONTEXT — many of these paths are invisible to a static scan.

## References

- Apache Kafka — consumer fetch-manager metrics: https://kafka.apache.org/documentation/#consumer_fetch_monitoring
- Confluent — Monitor consumer lag: https://docs.confluent.io/cloud/current/monitoring/monitor-lag.html
- JMX Prometheus exporter — consumer lag config: https://github.com/prometheus/jmx_exporter/issues/609
- Micrometer — per-partition lag discussion: https://github.com/micrometer-metrics/micrometer/issues/878
- Production monitoring stack guide: https://floriancourouge.com/en/blog/kafka-monitoring-production
