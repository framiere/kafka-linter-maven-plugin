# QK_TRANSACTIONAL_ID_NOT_UNIQUE

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: A shared transactional.id is two producers fencing each other off forever.

## TL;DR

`mp.messaging.outgoing.<channel>.transactional.id` defaults to `${quarkus.application.name}-<channelName>`. If two pods share that string (same app name, replicas), they fence each other off via Kafka's transactional protocol — one wins, the other crashloops.

## What's happening (the mechanism)

Kafka transactions use a single producer per `transactional.id` at a time. When a new producer registers, Kafka bumps the producer epoch and fences the older one. With replicated pods sharing the same `transactional.id`:

- Pod A registers: epoch=1, OK.
- Pod B registers: epoch=2, Pod A's next send fails with `ProducerFencedException`. Pod A restarts.
- Pod A registers: epoch=3, Pod B fails. Pod B restarts.
- Forever.

The SmallRye/Quarkus default is *almost* unique — it uses the channel name — but `quarkus.application.name` is the same across replicas of one deployment. So `acks-app-orders-out` is identical for pod-1 and pod-2.

## Operational impact

- Both pods crashloop with `ProducerFencedException`.
- `kafka_producer_record_send_total` flatlines.
- No messages produced; consumer lag on output topics is infinite.

## How to fix

Pin the transactional id to a per-pod identity:

```properties
# GOOD — include pod hostname / replica index
mp.messaging.outgoing.orders.transactional.id=${quarkus.application.name}-orders-${HOSTNAME}
```

In Kubernetes, `HOSTNAME` includes the pod name, which is unique. For deployments without a stable hostname (e.g., Cloud Run), use `${quarkus.uuid}` (regenerated on startup — acceptable for transactional id, as long as the topic is not shared with an in-flight transaction).

```properties
# OK — transactional id changes on restart; in-flight txns from previous instance are aborted by broker timeout
mp.messaging.outgoing.orders.transactional.id=${quarkus.application.name}-orders-${quarkus.uuid}
```

For exactly-once semantics with stable id, use StatefulSets in Kubernetes (`pod-0`, `pod-1`, ...) and template the index in.

## When this might be a false positive

- Single-replica deployments.
- Apps using `KafkaTransactions<T>` injected as a pooled producer where the pool manages distinct ids.

## Detection strategy

- Config: `mp.messaging.outgoing.<channel>.transactional.id` set to a value that does NOT include `${HOSTNAME}`, `${quarkus.uuid}`, or a unique env var template.
- Also flag absence of `transactional.id` on channels with `enable.idempotence=true` + transactional code path detected in bytecode (less reliable).
- Confidence: MEDIUM — operator may have a unique strategy the linter can't infer.

## Consult a friend?

> 🤝 **Slow down.** Pinning `transactional.id` to `${HOSTNAME}` or `${quarkus.uuid}` is correct, but the two have very different recovery stories — and the wrong choice silently breaks EOS on the very first crash.
> - `${HOSTNAME}` (StatefulSet): id is stable across restarts → a new pod replacing pod-0 fences the previous one and *resumes* its in-flight transaction. This is what EOS requires.
> - `${quarkus.uuid}`: id changes on every startup → the previous instance's open transaction stays "owned" by a now-dead id until `transaction.timeout.ms` (default 10 min) elapses on the broker. During that window, downstream `read_committed` consumers stall, and any partition the old instance held is locked. Is that downtime acceptable?
> - Cloud Run / Lambda / autoscaled-from-zero environments: pods have no stable identity by design. Are you sure exactly-once is the right semantic here, or would at-least-once + downstream idempotence be a saner fit?
> - For Kubernetes Deployments (not StatefulSets), `${HOSTNAME}` includes a random suffix — same problem as UUID. Confirm the manifest is StatefulSet *before* you put `${HOSTNAME}` in the config.

## References

- https://kafka.apache.org/documentation/#transaction_config
- https://quarkus.io/guides/kafka (Kafka Transactions)
