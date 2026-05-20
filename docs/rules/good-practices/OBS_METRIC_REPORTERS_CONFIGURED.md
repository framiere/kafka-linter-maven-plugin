# OBS_METRIC_REPORTERS_CONFIGURED

**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: combination
**Tagline**: Kafka exposes 100+ metrics. Configure one reporter, or you publish to /dev/null.

## TL;DR

The good-practice form of
[OBS_NO_METRIC_REPORTERS](../observability/OBS_NO_METRIC_REPORTERS.md).
A production application must register at least one path out of the
JVM for Kafka client metrics: a `MetricsReporter` implementation
(Confluent, OpenTelemetry), a Micrometer binding (`KafkaClientMetrics`),
or a JMX-to-Prometheus exporter agent. The anti-pattern doc has the
full mechanism.

## Cross-reference

See [OBS_NO_METRIC_REPORTERS](../observability/OBS_NO_METRIC_REPORTERS.md)
for the four paths (JMX agent, Micrometer, OTel, Confluent reporter)
and how Spring Boot's auto-configuration covers the default factories
but not custom ones.

## What the good-practice adds

The positive framing: one of these must be true for every Kafka client
in the application:

1. **Micrometer auto-config**: project depends on `micrometer-core` +
   a registry (e.g. `micrometer-registry-prometheus`) AND uses default
   Spring Boot factories (no custom `@Bean ProducerFactory`).
2. **OpenTelemetry agent**: javaagent at the JVM level (visible only in
   container manifest, not the lint).
3. **Explicit reporter**: `metric.reporters` set to an OTel or
   Confluent reporter class.
4. **JMX exporter**: javaagent at the JVM level.

The linter can prove 1 and 3 statically. 2 and 4 require runtime
inspection — when they're the chosen path, suppress the rule via
`<configuration><suppressOpsMetricsCheck>true</suppressOpsMetricsCheck></configuration>`
in the plugin config.

The good-practice doc also covers what to monitor once the metrics flow:

- Consumer: `records-lag-max`, `fetch-rate`, `commit-latency-avg`.
- Producer: `record-error-rate`, `record-send-rate`, `batch-size-avg`,
  `record-retry-rate`.
- Streams: `process-rate`, `commit-rate`, `restore-records-rate`,
  `alive-stream-threads`.

## References

See the linked anti-pattern doc, the Micrometer Kafka reference, and
the OpenTelemetry Kafka instrumentation docs.
