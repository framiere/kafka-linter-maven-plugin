# CONSUMER_STATIC_MEMBERSHIP

**Severity**: WARNING
**Confidence**: CONTEXT
**Detection**: bytecode + config-file
**Tagline**: Static membership turns N rolling-restart rebalances into zero.

## TL;DR

The good-practice form of
[CONSUMER_GROUP_INSTANCE_ID_MISSING](../kafka-clients/CONSUMER_GROUP_INSTANCE_ID_MISSING.md).
Production consumers running on stable pods (StatefulSet, EC2 instance,
named VM) should set `group.instance.id` and a generous
`session.timeout.ms` (30-60s) to skip rebalances during rolling
restarts. This document is the positive framing; the anti-pattern doc
has the full mechanism.

## Cross-reference

See [CONSUMER_GROUP_INSTANCE_ID_MISSING](../kafka-clients/CONSUMER_GROUP_INSTANCE_ID_MISSING.md)
for the mechanism, JMX, and detection details. KIP-345.

## What the good-practice adds

- The anti-pattern flags absence. The good-practice doc reminds the
  reader that **the bundle is two keys, not one**:
  - `group.instance.id=${POD_NAME}` (or any stable per-instance string)
  - `session.timeout.ms=45000-60000` (so a brief restart fits inside)
- And one operational discipline: the instance id MUST be unique per
  live consumer. Re-using it across two running instances fences one
  with `FencedInstanceIdException`. Use the StatefulSet ordinal, not the
  Deployment hash.

```properties
# yes — bundled together
group.id=orders-svc
group.instance.id=${POD_NAME}
session.timeout.ms=60000
heartbeat.interval.ms=20000
```

## References

See the linked anti-pattern doc and KIP-345.
