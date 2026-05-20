# KAFKA_CLIENTS_CVE_BUFFER_POOL

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: CVE-2026-35554 — your producer ships messages to the wrong topic and never tells you.

## TL;DR

A race condition in `KafkaProducer`'s `BufferPool` (disclosed January 2026 as **CVE-2026-35554** / CONFSA-2026-01) can cause a batch's `ByteBuffer` to be freed while still in flight. If a subsequent batch — possibly for a different topic — reuses the buffer before the original network request completes, the on-wire payload is corrupted, and records can be silently delivered to the wrong topic. No error is reported to the producer.

## The setup

A high-throughput producer with frequent `delivery.timeout.ms` expirations under load. Could be:
- a real-time pipeline against a slow broker;
- an enrichment service that produces to multiple topics from the same `KafkaProducer` instance (the common case);
- a test harness producing very short-lived bursts that race the timeout.

## What's actually happening

When a producer batch's `delivery.timeout.ms` elapses while the network request carrying it is still pending, the producer logic marks the batch as expired and returns its `ByteBuffer` to the shared pool. Meanwhile, the network response can still arrive — but the buffer it referenced has already been handed to a new batch (possibly for a different topic-partition). The TCP layer happily writes the second batch's contents into the buffer that the first request's logic now finalises and sends.

Visible symptoms:

- Records intended for topic A appearing on topic B (or vice versa) at low but non-zero rate, exactly during periods of broker slowness.
- No callback errors, no metric anomalies on the producer side beyond `record-expiration-rate` ticking up — which most teams interpret as "broker is slow" rather than "the producer is silently sending to the wrong topic".

This is a **data-integrity** CVE, not a confidentiality CVE — but the cross-topic delivery means data **does** cross security boundaries (a consumer on topic B that shouldn't see topic A's data now sees some).

## Why this is subtle

- The bug is timing-dependent; it does not reproduce on a healthy broker. Teams running soak tests against well-provisioned clusters will not see it.
- The symptom — wrong-topic records — looks like an application bug or a Connect/MirrorMaker misroute. Months can elapse before someone correlates the rate with broker latency spikes.
- Affects every `kafka-clients` version with the current buffer-pool design (back to 0.11 at least). The fix landed in 3.9.2 and 4.x patches per the Confluent advisory; older lines are not getting backports.

## Operational impact

- **Cross-topic data leakage** — a consumer holding read permission on topic B can read records that originated in topic A. In multi-tenant clusters this can be a contractual / regulatory incident.
- **Silent corruption** — no error path. The metric is "expired batches", which is treated as benign.
- **Hard to attribute**: by the time you discover wrong-topic records, the network conditions that triggered the race are long gone and there's no log evidence of the swap.

## How to fix

```xml
<!-- BAD: any client below the patch boundary -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.9.1</version>
</dependency>

<!-- GOOD: 3.9.2 / 4.0.2 / 4.1.2 / 4.2.x -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.9.2</version>
</dependency>
```

Defense in depth until you can patch:

- Increase `delivery.timeout.ms` so expirations almost never happen in healthy operation (the bug needs an expiration + an in-flight reply).
- Reduce `max.in.flight.requests.per.connection` (caps the size of the race window).
- Use one `KafkaProducer` per topic in the short term — eliminates cross-topic delivery as a symptom, doesn't fix the underlying race but bounds the blast radius.

## When this might be a false positive

- A producer with effectively zero traffic where `delivery.timeout.ms` never expires. The bug exists; the trigger doesn't.
- Pinned to a Confluent-supported line that has the patch backported (e.g. `cp-kafka-clients` 7.7.x post-Jan-2026) — verify via Confluent advisory CONFSA-2026-01.

## Detection strategy

- Resolved `kafka-clients` version `< 3.9.2`, **or** `[4.0.0, 4.0.2)`, **or** `[4.1.0, 4.1.2)`.
- For Confluent-flavored builds (`io.confluent:kafka` or shaded `cp-kafka-clients`), check against Confluent's patch table separately and report which advisory applies.
- Optional secondary signal: bytecode shows a `KafkaProducer` that produces to more than one topic in the same instance (raises the practical risk).

## References

- [Confluent advisory CONFSA-2026-01](https://support.confluent.io/hc/en-us/sections/360008413952-Security-Advisories-and-Security-Release-Notes)
- [Apache Kafka CVE list — CVE-2026-35554](https://kafka.apache.org/cve-list)
- [KAFKA buffer pool implementation discussion](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/BufferPool.java)
