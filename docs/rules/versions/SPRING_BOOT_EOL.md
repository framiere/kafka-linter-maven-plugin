# SPRING_BOOT_EOL

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: An EOL Spring Boot means an EOL kafka-clients underneath, and you don't get to pick.

## TL;DR

The linter flags `org.springframework.boot:spring-boot-*` (parent or BOM) at a release line that has reached OSS end of support. As of May 2026 the only OSS-supported Spring Boot lines are **3.5** and **4.0**. Everything below (including all of 2.x and 3.0–3.4) is OSS-EOL. For Kafka apps this matters double, because Boot's BOM transitively pins `spring-kafka` *and* `kafka-clients`.

## The setup

A team is on Spring Boot 2.7 because "it's the last 2.x" and they assume "last" means "supported". Spring's commercial extended-support window for 2.7 runs through June 2029 — but that requires a paid contract with VMware Tanzu. OSS support ended **30 June 2023**. The project is, in practice, on a frozen dependency tree, and the tree includes Kafka.

## What's actually happening

Spring Boot's release cadence is roughly six-monthly, with the **previous** minor line maintained for ~12 months after a new one ships. Real status (May 2026):

| Line | Released | OSS End | Commercial End | OSS Active? |
|------|----------|---------|----------------|-------------|
| 4.0 | Nov 2025 | Dec 2026 | Dec 2027 | Yes |
| 3.5 | May 2025 | Jun 2026 | Jun 2032 | Yes |
| 3.4 | Nov 2024 | Dec 2025 | Dec 2026 | No (recently ended) |
| 3.3 | May 2024 | Jun 2025 | Jun 2026 | No |
| 3.2 | Nov 2023 | Dec 2024 | Dec 2025 | No |
| 3.0 / 3.1 | 2022–2023 | 2023–2024 | 2024–2025 | No |
| 2.7 | May 2022 | Jun 2023 | Jun 2029 (Tanzu) | No (commercial only) |
| 2.0–2.6 | 2018–2021 | ≤ 2022 | ≤ 2024 | No |

What you inherit on an EOL Spring Boot line:

- **Pinned `spring-kafka`** — Boot 2.x pins spring-kafka 2.x, itself OSS-EOL.
- **Pinned `kafka-clients`** — Boot 2.7 pins kafka-clients 3.1.x (EOL since Sep 2022). Boot 3.2 pins 3.6.x (EOL since Feb 2024).
- **Pinned `snakeyaml`** — historically the carrier for CVE-2022-1471 and CVE-2022-25857. Boot's BOM was the fix path; without a Boot upgrade, you cannot bump snakeyaml without surgery.

## Why this is subtle

- The phrase "Spring Boot 2.7 is supported until 2029" floats around because the **commercial** window is that long. The OSS window ended in 2023. Teams cite the wrong number.
- Upgrading Spring Boot also upgrades `kafka-clients` underneath, sometimes by several minor versions in one step — which can trigger producer/consumer behavior changes (KIP-679 defaults, KIP-770 Streams cache config).
- The linter has to distinguish *commercial* and *OSS* support — they are not the same thing. The plugin should default to the OSS calendar; teams with a Tanzu contract can suppress.

## Operational impact

- **No security patches** beyond the OSS window — Boot 2.7 has had CVEs (e.g. snakeyaml, jackson) that only got into the 3.x line backports if a customer paid for them.
- **Old `kafka-clients` along for the ride** — see `KAFKA_CLIENTS_EOL`, `KAFKA_CLIENTS_CVE_*`. The whole CVE-2025-27817 / 27818 / 27819 set lands on you because the BOM doesn't budge.
- **Stuck on Jakarta EE 8 / Java EE 9 / javax** — Boot 3 was the `jakarta.*` switch. Boot 2 → 3 is a major refactor; the longer you wait, the harder.
- **Library compatibility shrinks** — newer Micrometer, Reactor, Hibernate stop supporting Boot 2.x.

## How to fix

```xml
<!-- BAD: parent pinned to EOL line -->
<parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>2.7.18</version>
</parent>

<!-- GOOD: supported OSS line -->
<parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.5.14</version>
</parent>
```

If parenting from `spring-boot-starter-parent` isn't possible (your org has its own parent), import the BOM:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-dependencies</artifactId>
            <version>3.5.14</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

## When this might be a false positive

- You have a Tanzu commercial support contract that covers your version. Suppress with a configured `commercialSupport=true` flag in the plugin.
- You're maintaining an LTS internal fork. Acknowledge and document.

## Detection strategy

- Inspect the resolved parent (`project.getModel().getParent()`) and the `org.springframework.boot:spring-boot-dependencies` BOM in `project.getDependencyManagement()`.
- Extract the version, normalize to its `MAJOR.MINOR` line.
- Compare against the table above (the linter should ship an embedded EOL table that's bumped on plugin releases — and a property to override for air-gapped users).

## References

- [Spring Boot support page](https://spring.io/projects/spring-boot#support)
- [Spring Boot EOL on endoflife.date](https://endoflife.date/spring-boot)
- [VMware Tanzu Spring runtime support policy](https://tanzu.vmware.com/support)
