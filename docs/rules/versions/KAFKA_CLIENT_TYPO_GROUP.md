# KAFKA_CLIENT_TYPO_GROUP

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: There is no io.confluent:kafka-clients. There never has been.

## TL;DR

The Kafka client is published only at `org.apache.kafka:kafka-clients`. Variants like `io.confluent:kafka-clients`, `org.apache:kafka-clients`, `kafka:kafka-clients`, or `org.apache.kafka:kafka-client` (singular) either don't exist on Maven Central, exist as someone else's namespace squat, or are typos that resolve to a broken or wrong artifact.

## The setup

A developer typed the coordinates from memory and got them wrong. Or they copied from a stale internal wiki. The pom looks plausible at a glance. The build either fails to download (most cases) or, worse, succeeds by pulling an unrelated artifact that happens to live at the wrong coordinate.

## What's actually happening

Common typos and what they do:

| Wrong coordinate | What happens |
|------------------|--------------|
| `io.confluent:kafka-clients` | 404 on Confluent's repo — `io.confluent` does not publish a `kafka-clients` artifact. |
| `org.apache:kafka-clients` | 404 on Central. |
| `org.apache.kafka:kafka-client` (no `s`) | 404 on Central. |
| `kafka:kafka-clients` | 404 (groupId must be `org.apache.kafka`). |
| `org.apache.kafka:kafka_2.13` | Resolves, but it's the **broker** (see `KAFKA_SCALA_BUNDLE_IMPORTED`). |
| `org.apache.kafka:kafka-streams-client` | 404 — the correct artifact is `kafka-streams`. |

In rare cases, an attacker has registered a similarly-named coordinate to pull off a dependency-confusion attack. The risk is real for organizations with private Nexus / Artifactory mirrors that have permissive priority orderings.

## Why this is subtle

- Maven's error message for a missing coordinate is "Could not resolve dependencies for project … the following artifacts could not be resolved" — not "you mistyped the coordinate".
- A typo within an enterprise that mirrors Central can succeed quietly if an internal repo *does* contain something at that coordinate.
- Wiki / documentation drift — internal docs sometimes show the wrong coordinate; new joiners copy it forward.

## Operational impact

- **Build failure** — most cases. Visible, fixable.
- **Wrong artifact pulled** — the dangerous case. A namespace squat on an internal mirror could ship attacker code. Dependency confusion has been a real attack technique since 2021.
- **Wasted upgrade effort** — someone tries to bump the wrong coordinate, doesn't find newer versions, files a misdirected ticket.

## How to fix

```xml
<!-- BAD: typos -->
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.9.2</version>
</dependency>

<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-client</artifactId>  <!-- missing 's' -->
    <version>3.9.2</version>
</dependency>

<!-- GOOD -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.9.2</version>
</dependency>
```

## When this might be a false positive

- Almost never. The coordinates listed above genuinely do not exist on Central. If your build resolves them, you have an internal mirror serving something at that coordinate — investigate.

## Detection strategy

- Walk `project.getDependencies()` and `project.getDependencyManagement().getDependencies()`.
- Apply a small allow-list of valid Kafka coordinates:
  - `org.apache.kafka:kafka-clients`
  - `org.apache.kafka:kafka-streams`
  - `org.apache.kafka:kafka-streams-scala_2.13` (and `_2.12`)
  - `org.apache.kafka:connect-api`, `connect-json`, `connect-runtime`, `connect-transforms`
  - `org.apache.kafka:kafka_2.13` (broker — separate rule)
- Plus known third-party coordinates: `io.confluent:kafka-avro-serializer`, etc. — these are valid.
- For any artifactId matching `kafka-*` with a wrong groupId or a fuzzy-matched typo to one of the valid ones, ERROR.

## References

- [Apache Kafka on Maven Central](https://central.sonatype.com/namespace/org.apache.kafka)
- [Dependency confusion attack writeup](https://medium.com/@alex.birsan/dependency-confusion-4a5d60fec610)
- [Maven coordinates documentation](https://maven.apache.org/pom.html#Maven_Coordinates)
