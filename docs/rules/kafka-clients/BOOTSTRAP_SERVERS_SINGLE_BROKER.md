# BOOTSTRAP_SERVERS_SINGLE_BROKER

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: Bootstrap with one host and you're one DNS blip from no Kafka at all.

## TL;DR

The linter flags `bootstrap.servers` set to a single broker. The whole point of the bootstrap list is to seed metadata discovery — one entry means one point of failure during client startup.

## What's happening (the mechanism)

`bootstrap.servers` is the list of host:port pairs the client uses to make its initial cluster metadata request. After the first metadata fetch the client talks to the actual partition leaders directly, so the bootstrap list is only needed at:

- Client startup.
- Every metadata refresh after `metadata.max.age.ms` (default 5 minutes).
- After a forced metadata refresh on any retriable error.

If the single bootstrap host is unreachable (rolling restart, DNS hiccup, network blip), the client cannot bootstrap and `send()` / `poll()` hang up to `max.block.ms` / `default.api.timeout.ms` before failing.

A list of two or three brokers is the standard; you do not need to list every broker.

## Operational impact

- Cold-start of the application during the wrong 30-second window = startup failure (CrashLoopBackOff in k8s, restart storm).
- Periodic metadata refreshes can fail and force the client through full reconnect.
- Increases incident blast radius — one broker maintenance window affects every freshly started producer/consumer.

## How to fix

```properties
# BAD
bootstrap.servers=kafka-0.example.com:9092

# GOOD
bootstrap.servers=kafka-0.example.com:9092,kafka-1.example.com:9092,kafka-2.example.com:9092

# GOOD — service-discovery style behind a stable DNS round-robin
bootstrap.servers=kafka-bootstrap.example.com:9092
```

A single hostname that resolves to multiple A records (round-robin DNS, k8s headless service) counts as multi-broker.

## When this might be a false positive

- Single-broker dev/test clusters.
- A single bootstrap hostname that fan-outs at the DNS layer — undetectable from the config string alone. MEDIUM in that case.

## Detection strategy

- Config: `bootstrap.servers` value contains zero commas AND ends in `:<port>`. WARNING.
- Bytecode: `Properties.put("bootstrap.servers", LDC "<single host>")`. WARNING.
- Suppress if hostname suggests a service-discovery endpoint (`*.svc.cluster.local`, `*.bootstrap.*`, `localhost`).

## References

- Apache Kafka producer configs — `bootstrap.servers`: https://kafka.apache.org/documentation/#producerconfigs_bootstrap.servers
- Apache Kafka consumer configs — `bootstrap.servers`: https://kafka.apache.org/documentation/#consumerconfigs_bootstrap.servers
- Confluent — Kafka client setup: https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html
