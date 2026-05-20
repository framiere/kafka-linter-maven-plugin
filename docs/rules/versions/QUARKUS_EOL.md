# QUARKUS_EOL

**Severity**: ERROR
**Confidence**: HIGH
**Detection**: pom-dependency
**Tagline**: Quarkus releases monthly; if you're not on an LTS, you're EOL within four weeks.

## TL;DR

Quarkus ships a minor release roughly every month and supports each non-LTS release only until the next minor lands. LTS releases get one year of patches. As of May 2026 the supported LTS lines are **3.33 (LTS to Mar 2027)** and **3.27 (LTS to Sep 2026)**; the rolling current is **3.35**. Everything else is EOL.

## The setup

A team picked Quarkus 3.15 ("the LTS at the time") and froze. Or they picked Quarkus 2.16 as their "Quarkus 2 final" and never moved. The Quarkus BOM (`io.quarkus.platform:quarkus-bom` or the older `io.quarkus:quarkus-bom`) pins everything underneath — including the SmallRye Reactive Messaging Kafka connector and the `kafka-clients` version.

## What's actually happening

Quarkus release policy (verified on endoflife.date):

| Cadence | Window |
|---------|--------|
| Non-LTS minor | ~4–6 weeks, until next minor ships |
| LTS minor | 12 months from release |

Current state (May 2026):

| Line | Released | EOL | LTS? | Status |
|------|----------|-----|------|--------|
| 3.35 | May 2026 | Jun 2026 | No | Current rolling |
| 3.33 | Mar 2026 | Mar 2027 | **Yes** | Supported LTS |
| 3.27 | Sep 2025 | Sep 2026 | **Yes** | Supported LTS |
| 3.20 | Mar 2025 | Mar 2026 | Yes | Recently EOL |
| 3.15 | Sep 2024 | Sep 2025 | Yes | EOL |
| 3.8 | Feb 2024 | Feb 2025 | Yes | EOL |
| 3.2 | Jul 2023 | Jul 2024 | Yes | EOL |
| 3.0 – 3.1 | 2023 | 2023 | No | EOL |
| 2.x (all) | 2021–2023 | ≤ 2023 | No | EOL — major namespace change to Jakarta in 3.x |

Quarkus 2.x → 3.x specifically:
- `javax.*` → `jakarta.*` (Jakarta EE 9 baseline).
- CDI 4.0 rather than 2.0.
- MicroProfile 6.0 rather than 5.0.
- Mutiny 2.x rather than 1.x.

What stale Quarkus pulls along:

- An old `smallrye-reactive-messaging-kafka` (different connector class names between Quarkus 2.x and 3.x).
- An old `kafka-clients` (Quarkus 3.2 LTS pinned `kafka-clients` 3.4.x — well below current EOL boundary).
- Old GraalVM substitutions for native compilation.

## Why this is subtle

- The LTS schedule has changed. Earlier Quarkus LTSes (2.7, 2.13) had ad-hoc extensions; modern LTSes have a clean 12-month window. Teams citing "Quarkus LTS is 18 months" are quoting an old policy.
- The non-LTS releases look identical to LTS releases — same version scheme, same release notes format. Nothing in the pom flags whether the chosen version is LTS or not.
- Red Hat's commercial build of Quarkus (RHBQ) tracks a subset of LTSes (3.27, 3.20 historically). A team on RHBQ may have commercial support past the OSS EOL date.

## Operational impact

- **No CVE backports** in OSS for EOL Quarkus lines. The Quarkus security team only patches supported LTS lines and the current rolling minor.
- **Frozen SmallRye / Vert.x / Mutiny versions** — these have their own CVE streams.
- **Stuck on old GraalVM** — Quarkus 3.2's GraalVM is `22.3`; current is `23.x`. Native compilation incompatibilities accumulate.
- **Hard upgrade later** — once an LTS is two years stale, the migration spans multiple LTS jumps with breaking changes at each.

## How to fix

```xml
<!-- BAD: an EOL non-LTS line -->
<dependency>
    <groupId>io.quarkus.platform</groupId>
    <artifactId>quarkus-bom</artifactId>
    <version>3.10.0</version>     <!-- EOL since May 2024 -->
    <type>pom</type>
    <scope>import</scope>
</dependency>

<!-- GOOD: current LTS -->
<dependency>
    <groupId>io.quarkus.platform</groupId>
    <artifactId>quarkus-bom</artifactId>
    <version>3.33.0</version>     <!-- LTS to Mar 2027 -->
    <type>pom</type>
    <scope>import</scope>
</dependency>
```

For LTS-tracking teams, pick the **latest** LTS at upgrade time, not the next-after-current. Doing two LTS jumps in one window is easier than four.

## When this might be a false positive

- Red Hat build of Quarkus (RHBQ) on an OSS-EOL line that is still in RH's commercial support window. Suppress with a documented commercialSupport flag.
- An end-to-end pinned legacy build (rarely justifiable for an actively-deployed system).

## Detection strategy

- Inspect `project.getDependencyManagement()` for `io.quarkus.platform:quarkus-bom` (or `io.quarkus:quarkus-bom`).
- Extract the version, normalize to `MAJOR.MINOR`.
- Cross-reference with an embedded EOL/LTS table (refreshed on plugin release; user-overridable).
- Severity matrix:
  - 2.x → ERROR (Jakarta-namespace shift; deep migration required).
  - 3.0–3.7 → ERROR.
  - 3.8 – 3.20 → WARNING.
  - 3.27 / 3.33 / current rolling → OK.
- Optionally also detect `io.quarkus.platform:quarkus-camel-bom` etc, which lag the main BOM.

## References

- [Quarkus EOL on endoflife.date](https://endoflife.date/quarkus)
- [Quarkus LTS release policy](https://quarkus.io/blog/lts-releases/)
- [Red Hat build of Quarkus support policy](https://access.redhat.com/support/policy/updates/jboss_notes/)
