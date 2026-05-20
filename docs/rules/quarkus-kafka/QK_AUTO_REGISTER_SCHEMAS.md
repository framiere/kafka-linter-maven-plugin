# QK_AUTO_REGISTER_SCHEMAS

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: config-file
**Tagline**: auto.register.schemas in prod = anyone can mint a new schema. Anyone.

## TL;DR

`auto.register.schemas=true` (the Confluent default) lets any producer push a new schema version into the registry just by sending a record with it. In production, this means a typo or a bad refactor permanently pollutes the schema history.

## What's happening (the mechanism)

Confluent's serializer auto-registers schemas on first send by default. This is great for dev (no manual registry interaction), terrible for prod:

- A developer ships a refactor that renames a field. The schema is auto-registered as v3. v2 consumers can no longer read v3 records.
- The schema-evolution policy (`BACKWARD`, `FORWARD`, `FULL`) is configured on the registry side, but `auto.register.schemas=true` together with the wrong compatibility level allows breaking changes.
- Schemas can't be deleted (only soft-deleted). The registry accumulates garbage forever.

Apicurio has an equivalent: `apicurio.registry.auto-register=true`.

## Operational impact

- Schema sprawl: dozens of slight variants in the registry.
- Consumer breakage from incompatible evolutions.
- No audit trail of WHO registered WHAT.

## How to fix

```properties
# Producers (prod): require explicit registration via CI/CD
mp.messaging.outgoing.orders.auto.register.schemas=false
mp.messaging.outgoing.orders.use.latest.version=true

# Apicurio equivalent
mp.messaging.outgoing.orders.apicurio.registry.auto-register=false
mp.messaging.outgoing.orders.apicurio.registry.find-latest=true
```

Manage schemas through a registry CLI / IaC tool with reviews.

## When this might be a false positive

- Dev profiles (`%dev`, `%test`) — auto-register is the right default there.
- Greenfield prototypes where schema discipline hasn't kicked in yet (flag it anyway).

## Detection strategy

- Config: `mp.messaging.outgoing.<channel>.auto.register.schemas=true` OR `mp.messaging.outgoing.<channel>.apicurio.registry.auto-register=true` in unprofiled or `%prod.` properties.
- Confidence: HIGH for `%prod.`, MEDIUM otherwise.

## References

- https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#configuration-details
- https://www.apicur.io/registry/docs/apicurio-registry/2.5.x/getting-started/assembly-using-kafka-client-serdes.html
