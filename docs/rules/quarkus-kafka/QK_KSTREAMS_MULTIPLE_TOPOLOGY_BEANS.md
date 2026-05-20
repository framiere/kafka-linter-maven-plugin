# QK_KSTREAMS_MULTIPLE_TOPOLOGY_BEANS

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: bytecode + annotation
**Tagline**: One Topology to rule them all — extras get ignored.

## TL;DR

Quarkus Kafka Streams selects a single `@Produces Topology` CDI bean to drive the engine. Multiple `@Produces Topology` beans → CDI ambiguous resolution at startup, or silent selection of one and discarding of the others depending on qualifiers.

## What's happening (the mechanism)

`quarkus-kafka-streams` injects a `Topology` via standard CDI. The default qualifier matches a single producer. If you have two beans both producing a `Topology` with the default qualifier:

- Without qualifiers: CDI throws `AmbiguousResolutionException` at startup.
- With `@Default` on both: same outcome.
- With distinct qualifiers but no consumer-side qualifier on `quarkus-kafka-streams`: framework picks one (implementation-defined order) and the other Topology never runs.

The "silent" failure mode is the dangerous one — sub-topologies you thought were composed never execute.

## Operational impact

- Half your pipelines silently do nothing.
- No exception, no log line. Just absent output.
- Discovery happens when downstream consumers notice the missing output topic.

## How to fix

Compose multiple sub-topologies into one `StreamsBuilder`:

```java
// GOOD — single @Produces Topology, multiple sub-topologies inside
@ApplicationScoped
public class TopologyProducer {
    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        ordersPipeline(builder);
        inventoryPipeline(builder);
        return builder.build();
    }
}
```

If you genuinely need multiple Streams applications, deploy them as separate services with their own `application.id`.

## When this might be a false positive

- One bean is `@Alternative` or scoped under a profile (`@LookupIfProperty`, `@IfBuildProfile`) — only one is active at a time.

## Detection strategy

- Bytecode: scan for `@Produces` methods returning `Lorg/apache/kafka/streams/Topology;`. Count per module/jar.
- Annotation: track `@Default`, `@Named`, custom qualifiers, `@Alternative`, `@IfBuildProfile`.
- Confidence: HIGH if ≥2 active producers found.

## References

- https://quarkus.io/guides/kafka-streams
- https://docs.jboss.org/cdi/spec/2.0/cdi-spec.html (Producer methods)
