# KAFKA_SCALA_BUNDLE_IMPORTED

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: You wanted a Kafka client. You pulled in Scala, the broker, and a small star system.

## TL;DR

The coordinate `org.apache.kafka:kafka_2.13` (or `kafka_2.12`, `kafka_2.11`) is the **broker** artifact — the server bundle, including Scala runtime, ZooKeeper client, and dozens of MB of broker-side code. Java client applications should depend on `org.apache.kafka:kafka-clients`, which is ~5 MB of pure Java with no Scala.

## The setup

A developer searched Maven Central for "kafka" and copy-pasted the first hit they recognized. Or they followed a Scala tutorial and didn't realize the Scala suffix mattered. Or they're vaguely aware of the distinction but think "more is fine, it'll work either way". Indeed it works — and ships an extra 60 MB of Scala stdlib in your container image.

## What's actually happening

Apache Kafka publishes several artifacts under `org.apache.kafka`:

| Artifact | Purpose | Size | Includes |
|----------|---------|------|----------|
| `kafka-clients` | Java producer / consumer / admin clients | ~5 MB | Pure Java, slf4j-api, snappy-java |
| `kafka-streams` | Java Streams DSL | ~2 MB | Depends on kafka-clients |
| `kafka_2.13` | Broker, ZooKeeper, JAAS, embedded servers | ~50+ MB | Scala stdlib 2.13, ZooKeeper client, Jetty, all internal broker code |
| `kafka_2.12` | Same as above with Scala 2.12 | ~50+ MB | Older Scala stdlib |
| `connect-runtime` | Connect framework | ~10 MB | Reflection runtime, REST server |

Pulling `kafka_2.13` into a client application:

1. Adds the Scala runtime (`scala-library` ~5 MB).
2. Adds ZooKeeper client (`org.apache.zookeeper:zookeeper` ~1 MB + transitive Netty).
3. Adds Jetty server bits via the embedded admin server (~10 MB).
4. Adds the broker's internal request/response classes — which can class-clash with `kafka-clients` if both are on the path.
5. Pins Scala major to whatever you picked, blocking other Scala libraries in the same project.

## Why this is subtle

- The artifactId is **almost** the same — `kafka` vs `kafka-clients`. A tired developer or a stale doc gets it wrong.
- The build works. Producer / Consumer constructors exist in both (the broker artifact re-exports `kafka-clients`). Runtime works too. Only signs: container image size, classpath logs showing Scala/ZK classes, and the occasional `NoClassDefFoundError` when the broker artifact's old internal class clashes with a newer `kafka-clients` on the same classpath.
- IDE auto-complete on `org.apache.kafka:` may show the broker first because it has the shorter artifactId.

## Operational impact

- **Container image bloat** — 60+ MB extra in the deployable, plus all the transitive Scala dependencies.
- **Cold start regression** — JVM class loading scales with classpath size; this matters for serverless and short-lived jobs.
- **Classpath clashes** — having `kafka_2.13` pinned to one Kafka version and `kafka-clients` pinned to another (often via a different transitive path) produces hard-to-diagnose `NoSuchMethodError` / `LinkageError` at startup.
- **Security surface widening** — the broker artifact contains Jetty and ZooKeeper client. Both ship their own CVE streams that you now inherit for no reason.
- **License footprint** — the broker bundle pulls a wider set of licenses; some compliance reviews fail purely on that.

## How to fix

```xml
<!-- BAD -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka_2.13</artifactId>   <!-- this is the BROKER -->
    <version>3.9.2</version>
</dependency>

<!-- GOOD -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.9.2</version>
</dependency>
```

For Streams:

```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-streams</artifactId>
    <version>3.9.2</version>
</dependency>
```

If a test module legitimately needs the broker (embedded Kafka in integration tests), scope it correctly:

```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka_2.13</artifactId>
    <version>3.9.2</version>
    <scope>test</scope>
</dependency>
```

## When this might be a false positive

- A project that legitimately runs an embedded broker for tests — but it should be `<scope>test</scope>`. Flag the missing scope.
- A genuine Scala application using the Scala admin API (rare; the Java admin API is preferred since 2.0).

## Detection strategy

- Walk `project.getArtifacts()` for `groupId=org.apache.kafka` and an artifactId matching `kafka_2\.(11|12|13)`.
- If found in `compile` or `runtime` scope, ERROR. If `test`, INFO (still worth pointing out — the Scala API surface is deprecated).
- Co-detection: if `kafka-clients` is also resolved, suggest dropping the broker artifact entirely from the main classpath.

## References

- [Maven Central — kafka-clients vs kafka](https://search.maven.org/search?q=g:org.apache.kafka)
- [Apache Kafka project structure](https://github.com/apache/kafka/tree/trunk)
- [KIP-590 — Switch the AdminClient to the new RPC framework](https://cwiki.apache.org/confluence/display/KAFKA/KIP-590) (background on Java vs Scala APIs)
