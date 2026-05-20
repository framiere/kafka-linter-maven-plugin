# QUARKUS_KAFKA_EXTENSION_RENAMED

**Severity**: WARNING
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: quarkus-smallrye-reactive-messaging-kafka was renamed to quarkus-messaging-kafka — use the new name.

## TL;DR

In Quarkus 3.9, the extension `io.quarkus:quarkus-smallrye-reactive-messaging-kafka` was relocated to `io.quarkus:quarkus-messaging-kafka` as part of a broader rename to "Quarkus Messaging". The old artifactId is still resolvable (via a Maven relocation), but it pulls a relocation pom that emits a warning at build time and there is no guarantee it will keep working forever.

## The setup

A team's pom was written when the extension's name still had `smallrye-reactive-messaging` in it. They upgraded Quarkus to 3.15+ and the build kept working — Maven relocation hid the rename. The dependency is now technically wrong, and they don't know.

## What's actually happening

The Quarkus team renamed several extensions in the 3.9 timeframe to drop "smallrye-reactive-messaging" from user-facing names. The implementation underneath is still SmallRye Reactive Messaging — the rename was purely UX.

Affected coordinates (non-exhaustive):

| Old artifactId | New artifactId |
|----------------|----------------|
| `quarkus-smallrye-reactive-messaging-kafka` | `quarkus-messaging-kafka` |
| `quarkus-smallrye-reactive-messaging-amqp` | `quarkus-messaging-amqp` |
| `quarkus-smallrye-reactive-messaging-rabbitmq` | `quarkus-messaging-rabbitmq` |
| `quarkus-smallrye-reactive-messaging` | `quarkus-messaging` |

The old `<artifactId>` resolves through a Maven `<relocation>` declaration in its pom. The relocation will:

1. Emit `[WARNING] The artifact io.quarkus:quarkus-smallrye-reactive-messaging-kafka:jar:X.Y.Z has been relocated to io.quarkus:quarkus-messaging-kafka:jar:X.Y.Z` at build time.
2. Pull the new artifact transparently.
3. **Not** guarantee the old coordinate stays published forever.

## Why this is subtle

- The build works. The warning is one line in a sea of Maven output most teams scroll past.
- The Quarkus migration guide is the only canonical place this is documented; the old extension page on `quarkus.io/extensions` redirects with a small banner.
- The rename also moved deployment-side artifacts (`*-deployment` artifactIds), affecting any custom extension that depended on the old artifactId at compile time.

## Operational impact

- **Build warnings** that get classified as noise.
- **Future breakage** when (not if) the old coordinate is dropped from Maven Central — Quarkus has done so before (e.g. some 2.x → 3.x extension drops).
- **Documentation drift** — new docs use the new name; copy-paste examples don't match the project's poms; PR reviewers get confused.

## How to fix

```xml
<!-- BAD -->
<dependency>
    <groupId>io.quarkus</groupId>
    <artifactId>quarkus-smallrye-reactive-messaging-kafka</artifactId>
</dependency>

<!-- GOOD -->
<dependency>
    <groupId>io.quarkus</groupId>
    <artifactId>quarkus-messaging-kafka</artifactId>
</dependency>
```

(No version — managed by the Quarkus BOM.)

## When this might be a false positive

- A project still on Quarkus 3.8 or earlier where the new artifactId does not yet exist. The linter should suppress this rule for Quarkus `< 3.9`.

## Detection strategy

- Walk `project.getDependencies()` for `io.quarkus:quarkus-smallrye-reactive-messaging-*`.
- Only flag if Quarkus BOM is `>= 3.9.0`.
- The rule fires on the declared dependency (not the resolved one — Maven's relocation hides the issue post-resolution).

## References

- [Quarkus 3.9 migration guide](https://github.com/quarkusio/quarkus/wiki/Migration-Guide-3.9)
- [quarkus-messaging-kafka extension page](https://quarkus.io/extensions/io.quarkus/quarkus-messaging-kafka/)
- [Maven relocation documentation](https://maven.apache.org/guides/mini/guide-relocation.html)
