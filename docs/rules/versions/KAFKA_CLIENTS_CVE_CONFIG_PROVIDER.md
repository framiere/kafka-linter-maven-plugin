# KAFKA_CLIENTS_CVE_CONFIG_PROVIDER

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: CVE-2024-31141 — ConfigProvider was happy to read whatever file you named.

## TL;DR

`kafka-clients` versions `2.3.0` through `3.5.2`, plus `3.6.0`–`3.6.2` and `3.7.0`, are vulnerable to **CVE-2024-31141**. The `ConfigProvider` mechanism would automatically resolve `${...}` placeholders against `FileConfigProvider` / `DirectoryConfigProvider`, allowing any client config to be a vehicle for reading arbitrary local files or environment variables. Fixed in **3.6.3** and **3.7.1**.

## The setup

`ConfigProvider` is the Connect-era hook that lets you write `${file:/etc/secrets/db:password}` in a connector config and have the client substitute the file's contents at startup. It is a great feature for secret externalization — and a terrible feature when applied to client config that originated from an untrusted source.

## What's actually happening

In the vulnerable range, the client treated any `${...}` in a config value as a directive to resolve through the registered `ConfigProvider` set. The standard library ships `FileConfigProvider` and `DirectoryConfigProvider` — both happy to read any path the JVM user can. Result:

- A connector config of `name=${file:/etc/passwd:root}` returns the root entry of `/etc/passwd` interpolated into the connector's identity.
- The same primitive works for env vars via the `env` provider in Connect runtimes.
- Privilege boundary crossed: a REST API caller authorized to create connectors gains read access to the JVM process's filesystem.

The fix removes the implicit resolution: ConfigProviders must be explicitly enabled by the producer/consumer creator, not silently applied to user-supplied values.

## Why this is subtle

- This is a Kafka **clients** CVE, not a Connect-only CVE. The vulnerability sits in the shared config substitution machinery. It also affects pure consumers and producers if they accept config from a config service or a config file owned by a less-privileged process.
- The fix lands on the *clients* artifact even though most discussion is in Connect terms. Plain `kafka-clients` consumers benefit from the upgrade too.
- 3.6.2 was the last patch before the fix; 3.6.3 was a backport-only release with no announcement equivalent to a feature release. Teams tracking minor lines may have skipped it.

## Operational impact

- **Filesystem read access** by anyone able to influence a config value. Service tokens, mounted secrets, EC2 metadata cache files, the JVM user's `~/.m2/settings.xml` containing repository credentials, the works.
- **Environment variable disclosure** through the same primitive — `${env:AWS_SECRET_ACCESS_KEY:fallback}`.
- **Privilege escalation in Connect** — REST API access (often less-guarded than file access) becomes filesystem read.

## How to fix

```xml
<!-- BAD: anywhere in the vulnerable range -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.5.1</version>
</dependency>

<!-- GOOD: at the patch boundary -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.6.3</version>
</dependency>

<!-- BETTER: latest supported line -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.9.2</version>
</dependency>
```

If you genuinely need `ConfigProvider`-based substitution (which most consumers/producers don't), opt in explicitly via the `config.providers` configuration with a narrow allow-list.

## When this might be a false positive

- Client config comes exclusively from constants in source, with no Connect, no external config service, no admin REST. Even then the upgrade is trivial.

## Detection strategy

- Resolved version in any of:
  - `[2.3.0, 3.5.3)`
  - `[3.6.0, 3.6.3)`
  - `[3.7.0, 3.7.1)`
- Skip projects on `>= 3.6.3` and `>= 3.7.1` and `>= 3.8.0`.

## References

- [Apache CVE list — CVE-2024-31141](https://kafka.apache.org/cve-list)
- [NVD entry for CVE-2024-31141](https://nvd.nist.gov/vuln/detail/CVE-2024-31141)
- [KIP-297 — ConfigProvider](https://cwiki.apache.org/confluence/display/KAFKA/KIP-297)
