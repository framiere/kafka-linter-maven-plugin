# STREAMS_APP_ID_STABLE_AND_VERSIONED

**Severity**: WARNING
**Confidence**: MEDIUM
**Detection**: config-file
**Tagline**: `application.id` is your stream's identity. Treat it like a deployment artifact, not a runtime variable.

## TL;DR

The good-practice form of
[STREAMS_APP_ID_UNSTABLE](../kafka-streams/STREAMS_APP_ID_UNSTABLE.md).
A production Streams app's `application.id` must be stable across
restarts and versioned deliberately when topology changes break
compatibility.

## Cross-reference

See [STREAMS_APP_ID_UNSTABLE](../kafka-streams/STREAMS_APP_ID_UNSTABLE.md)
for the mechanism — every identity in Streams (consumer group, internal
topic prefix, state directory) derives from `application.id`.

## What the good-practice adds

Beyond "don't use UUIDs", the positive practice is:

- Treat `application.id` as a **versioned namespace**: `billing.aggregator.v1`,
  `billing.aggregator.v2` when an incompatible topology change is
  shipped. The old application.id's internal topics and state are
  orphaned; you migrate consumers explicitly with a documented procedure.
- Reserve `.v2`, `.v3` etc. for actual breaking changes (renamed state
  store, changed key serde, schema-breaking aggregator). Not for every
  release.
- Encode the application name and environment cleanly:
  `<team>.<app>.<env>.v<n>` reads in alphabetical sort.

```properties
# yes — stable, versioned
application.id=billing.aggregator.prod.v1
```

The Apache Kafka 3.7 `ensure.explicit.internal.resource.naming=true`
config is a useful belt-and-braces — combined with versioned
`application.id`, no topology change ever silently orphans state.

## References

See the linked anti-pattern doc and the Streams operational guide.
