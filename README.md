# kafka-linter-maven-plugin

![rules](https://img.shields.io/badge/rules-746-c14a1f?style=for-the-badge&labelColor=16140f)
![categories](https://img.shields.io/badge/categories-10-1f3d36?style=for-the-badge&labelColor=16140f)
![java](https://img.shields.io/badge/java-17%2B-b58a3a?style=for-the-badge&labelColor=16140f)
![maven](https://img.shields.io/badge/maven-3.6.3%2B-b58a3a?style=for-the-badge&labelColor=16140f)

> **A Maven plugin that statically analyses your compiled bytecode and your Kafka configuration files and flags the failure modes a five-year-pager Kafka operator already knows by heart — at build time, before they fire in prod.**

---

## 1. Why this plugin exists

Kafka is unforgiving in a very specific way: the failure modes are **silent, slow, and structural**. The build is green, the tests are green, the staging cluster is green — and then, six months later, a broker rolls, a consumer group rebalances, a producer's `acks` setting interacts with `enable.idempotence`, and you lose ordering on a partition, or quietly drop a batch, or stall every consumer in the org because somebody hard-coded `localhost:9092` into a packaged JAR.

The set of mistakes is **not large** and **not novel**. The same dozen anti-patterns produce the same incidents in every org. But they survive code review because they look correct line-by-line — the bug is in what's *missing* (no `compression.type`, no `Callback`, no `ElectLeadersOptions` timeout) or in what's *implicit* (the default `request.timeout.ms` is 30 s; the default `acks` is `all` in 3.x but used to be `1`; the default `enable.idempotence` flipped to `true` in 3.0).

This plugin is **the curated body of those mistakes, encoded as bytecode and config-file rules**. The editorial promise is strict:

- **Every rule traces to a real production failure mode** — data loss, runtime defect, EOL/CVE exposure, observability gap, or the absence of a well-known production default. No stylistic nits, no "we prefer this pattern" rules.
- **Every rule doc carries a four-paragraph editorial block** (tagline → mechanism → impact → why-it-matters) so reading the catalog is also reading a curated guide to running Kafka in production.
- **Rules whose obvious fix has a non-obvious correctness cost are marked 🤝** — read those carefully before changing prod config.
- **No false positives by design**: when in doubt, the rule stays method-local and high-confidence. False negatives are quieter than false positives.

The linter has no runtime dependency on `kafka-clients`. It scans `target/classes/**/*.class` with [ASM](https://asm.ow2.io/) and `src/main/resources/**/{application,bootstrap}.{yml,yaml,properties}` with a small property-file walker. No Kafka cluster needed, no test containers, no agent. Build-time only.

---

## 2. What it catches

**746 rules across 10 categories.** Full machine-readable catalog: [`docs/rules/_CATALOG.md`](docs/rules/_CATALOG.md). The per-category docs live under [`docs/rules/`](docs/rules/).

<table>
<tr>
<td width="33%" valign="top">

### 🧱 kafka-clients
**259 rules**

Plain Apache Kafka producers and consumers — the lowest-level surface. `acks`, `enable.idempotence`, `max.in.flight`, offsets, rebalances, headers, Admin API lifecycle, AdminClient mutating ops missing per-call Options/timeouts.

</td>
<td width="33%" valign="top">

### 🌱 spring-kafka
**174 rules**

`@KafkaListener`, `KafkaTemplate`, error handlers, `ErrorHandlingDeserializer`, Spring DSL property paths, passthrough overrides, container concurrency, retry-topic semantics.

</td>
<td width="33%" valign="top">

### 🌊 kafka-streams
**146 rules**

Streams DSL, processor API, state stores, EOS v2, timestamp extractors, RocksDB tuning, repartitioning, standby replicas, suppressed windows, foreign-key joins.

</td>
</tr>
<tr>
<td width="33%" valign="top">

### 🔌 kafka-connect
**92 rules**

Source/sink connectors and Connect-worker config — Confluent S3/HDFS/GCS sinks, JDBC source polling, Debezium CDC, dead-letter-queue, converter pitfalls, config-provider hygiene.

</td>
<td width="33%" valign="top">

### 🔐 security
**23 rules**

Credentials, secrets, and authentication hygiene — plaintext credentials in configs, AWS-credential literals, weak SASL/SSL choices, ACL mutation without Options.

</td>
<td width="33%" valign="top">

### 🐰 quarkus-kafka
**23 rules**

SmallRye Reactive Messaging — `@Incoming`/`@Outgoing`, `Emitter`, channel config, per-channel passthrough, dead-letter-queue.

</td>
</tr>
<tr>
<td width="33%" valign="top">

### 📌 versions
**9 rules**

Library-version, EOL, BOM-drift, and CVE rules — `pom.xml` dependency presence/absence/version range.

</td>
<td width="33%" valign="top">

### 📊 observability
**7 rules**

Cross-cutting concerns: interceptors, metric reporters, OTel, deserialization safety (CVE-2023-34040), lambda hygiene, async/reactive bridge.

</td>
<td width="33%" valign="top">

### 📋 schema-registry
**7 rules**

Schema Registry serde configuration, auto-register, subject-naming-strategy, Avro 1.12+ logical-type Java mappings.

</td>
</tr>
<tr>
<td width="33%" valign="top">

### 🐰 quarkus
**6 rules**

Quarkus-framework concerns outside of `quarkus-kafka` — extension config, native-image incompatibilities, runtime defaults.

</td>
<td width="33%" valign="top">&nbsp;</td>
<td width="33%" valign="top">&nbsp;</td>
</tr>
</table>

A small sampler from the core `kafka-clients` category:

| Rule ID                              | Default   | What it catches                                                                                                  |
|--------------------------------------|-----------|------------------------------------------------------------------------------------------------------------------|
| `PRODUCER_IN_LOOP`                   | `ERROR`   | `new KafkaProducer(...)` inside a `for`/`while`/`do-while` loop or inside an iterating lambda (`forEach`, `Stream.map`, …). |
| `PRODUCER_NO_COMPRESSION`            | `ERROR`   | A method constructs a `KafkaProducer` but never sets `compression.type` anywhere in the same method.             |
| `PRODUCER_SEND_BLOCKING_GET`         | `ERROR`   | `producer.send(record).get()` — synchronous, kills batching.                                                     |
| `PRODUCER_SEND_NO_CALLBACK`          | `WARNING` | `producer.send(record)` called without a `Callback` **and** the returned `Future` is discarded.                  |
| `CONSUMER_AUTO_COMMIT_TRUE`          | `WARNING` | `enable.auto.commit=true` — risks message loss / double processing on rebalance.                                 |
| `CONSUMER_COMMIT_PER_RECORD`         | `ERROR`   | `consumer.commitSync()` inside the **per-record** loop. Commit per batch instead.                                |
| `CONSUMER_POLL_ZERO`                 | `ERROR`   | `consumer.poll(0L)` or `poll(Duration.ZERO)` — busy-loops the consumer thread.                                   |
| `ADMIN_ELECT_LEADERS_NO_OPTIONS`     | `ERROR`   | `Admin.electLeaders(ElectionType, Set)` without an `ElectLeadersOptions` — naïve retry on 30 s timeout doubles leader-epoch advance; UNCLEAN retry compounds data loss. |
| `KAFKA_BOOTSTRAP_SERVERS_LOCALHOST`  | `ERROR`   | `bootstrap.servers` contains `localhost` in a packaged artifact.                                                 |

---

## 3. How to use it

Add the plugin to the `<build>` of any project that uses `org.apache.kafka:kafka-clients` (or any downstream — Streams, Spring-Kafka, Quarkus, Connect):

```xml
<plugin>
    <groupId>io.conductor</groupId>
    <artifactId>kafka-linter-maven-plugin</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <executions>
        <execution>
            <id>lint</id>
            <goals><goal>check</goal></goals>
        </execution>
    </executions>
</plugin>
```

Then:

```bash
mvn verify
```

You'll see one of two things:

```
[INFO] kafka-linter: 0 violations.
```

or:

```
[INFO] kafka-linter: 12 violation(s) — 10 error, 2 warning.
[ERROR] PRODUCER_IN_LOOP sample.BadKafkaUsage#producerInForLoop:23 — new KafkaProducer inside loop.
[ERROR] PRODUCER_NO_COMPRESSION sample.BadKafkaUsage#producerWithoutCompression:52 — 'compression.type' never set in this method.
[WARNING] PRODUCER_SEND_NO_CALLBACK sample.BadKafkaUsage#fireAndForget:64 — send(record) without a Callback and Future is discarded.
…
[ERROR] kafka-linter: 10 error-severity violation(s) found.
```

Any `ERROR`-severity violation fails the build. `WARNING` is logged. `INFO` and `OFF` are silent.

You can invoke the plugin directly:

```bash
mvn kafka-linter:check
```

### Configuration

| Parameter          | Property                          | Default                              | Effect                                              |
|--------------------|-----------------------------------|--------------------------------------|-----------------------------------------------------|
| `<skip>`           | `kafka-linter.skip`               | `false`                              | Skip the check entirely.                            |
| `<classesDirectory>` | `kafka-linter.classesDirectory` | `${project.build.outputDirectory}`   | Override the directory of compiled classes to scan. |
| `<severities>`     | —                                 | (defaults from `RuleId`)             | Per-rule severity overrides (`ERROR`/`WARNING`/`INFO`/`OFF`). |

Per-rule overrides — turn a rule up, down, or off:

```xml
<configuration>
    <severities>
        <PRODUCER_SEND_NO_CALLBACK>ERROR</PRODUCER_SEND_NO_CALLBACK>
        <CONSUMER_AUTO_COMMIT_TRUE>OFF</CONSUMER_AUTO_COMMIT_TRUE>
        <ADMIN_ELECT_LEADERS_NO_OPTIONS>ERROR</ADMIN_ELECT_LEADERS_NO_OPTIONS>
    </severities>
</configuration>
```

Unknown rule IDs or severities are logged as warnings and ignored.

```bash
mvn verify -Dkafka-linter.skip=true
mvn kafka-linter:check -Dkafka-linter.classesDirectory=build/classes/java/main
```

---

## 4. How it works

```
target/classes/**/*.class                src/main/resources/**/*.{yml,yaml,properties}
        │                                              │
        ▼                                              ▼
ProjectScanner — walks the tree         PropertyFileScanner — parses YAML / .properties
        │                                              │
        ▼                                              │
ClassAnalyzer — ClassReader → ClassNode                │
        │                                              │
        ▼                                              │
RuleContext — per-class: LoopFinder +                  │
              LambdaTracker + AsmUtil                  │
        │                                              │
        ▼                                              ▼
        Rule[]   ←──────────────  KafkaLinterMojo  ──────────────→  PropertyFileRule[]
              emits Violation objects with severity + source line
                                       │
                                       ▼
                              SimpleReporter / VerboseReporter
                              fails the build on ERROR
```

Key files (under `src/main/java/io/conductor/kafkalinter/`):

- `KafkaLinterMojo.java` — the `@Mojo(name="check")` entry point. Wires every `RuleId` to its implementation.
- `RuleId.java` — value-class registry of all 746 rule IDs with default severity, confidence, category, doc path, and the four didactic blocks (`tagline`/`mechanism`/`impact`/`whyMatters`).
- `Severity.java` / `Confidence.java` / `Violation.java` — the reporting model.
- `scanner/` — `LoopFinder` (back-edge detection), `LambdaTracker` (INVOKEDYNAMIC + iterating-API detection), `RuleContext` (per-class predicate bundle), `AsmUtil` (the dual-walk helpers — see below), `PropertyFileRule` (config-file predicate builder).
- `rules/` — one Java file per bytecode rule, grouped by `clients/`, `streams/`, `spring/`, `connect/`, `admin/`, `quarkus/`, `security/`, `observability/`, `version/`, `config/`.
- `report/` — `SimpleReporter` and `VerboseReporter` (the latter prints the four-paragraph didactic block).

### Two detection contexts a rule can rely on

**1. Classical loops.** `LoopFinder` walks each method's instruction list and identifies back-edges — any `JumpInsnNode`, `TableSwitchInsnNode`, or `LookupSwitchInsnNode` whose target label appears **earlier** in the method than the jump itself. Every instruction between the target and the jump is "inside a loop". This catches `for`, `while`, `do-while`, and labelled jumps with no source-code access.

**2. Iterating lambdas.** When the compiler emits an `INVOKEDYNAMIC` referencing `LambdaMetafactory`, the body becomes a synthetic method (`lambda$foo$0`). If the very next instruction passes that lambda to one of a known set of iterating APIs — `Iterable.forEach`, `Collection.forEach`, `Map.forEach`, `Stream.{forEach,forEachOrdered,map,filter,peek,flatMap}` — then everything inside the synthetic lambda method is treated as "in a loop".

Both feed into one predicate: `RuleContext.isInLoopOrIteratingLambda(method, insn)`. `PRODUCER_IN_LOOP`, `CONSUMER_IN_LOOP`, `PRODUCER_FLUSH_IN_LOOP` all share it.

### Dual-walk pattern for method-call rules

A naïve `MethodInsnNode`-only rule misses method-reference captures. `BiFunction<X,Y,Z> elect = admin::electLeaders` compiles to an `INVOKEDYNAMIC` whose bsm-args contain a `REF_invokeInterface` Handle on the target method — the user-class bytecode contains zero direct `INVOKEINTERFACE` on the unsafe overload, only the indy site.

Every dedicated method-call rule therefore walks both:

```java
for (AbstractInsnNode insn : mn.instructions) {
    if (insn instanceof MethodInsnNode mi
            && OWNERS.contains(mi.owner)
            && METHOD_NAME.equals(mi.name)
            && mi.desc != null
            && !mi.desc.contains(OPTIONS_TYPE_TOKEN)) {
        out.add(violation(ctx, mn, insn));
        continue;
    }
    if (insn instanceof InvokeDynamicInsnNode indy) {
        Handle h = AsmUtil.indyTargetHandle(indy, OWNERS, METHOD_NAME, null);
        if (h != null && h.getDesc() != null && !h.getDesc().contains(OPTIONS_TYPE_TOKEN)) {
            out.add(violation(ctx, mn, insn));
        }
    }
}
```

`AsmUtil.indyTargetHandle(indy, owners, name, desc)` peels back the `LambdaMetafactory` bsm-args to the implementing-method `Handle` and matches owner + name + (optional) descriptor.

### Method-locality

Most heuristics are intentionally method-local: a config built by a helper method and passed in is not analysed end-to-end. This keeps the analysis simple, fast, and predictable. **False negatives are quieter than false positives**, and the editorial line in this project is that a rule that fires too rarely is fixable, a rule that cries wolf is uninstalled.

---

## 5. Adding a new rule and testing it

The workflow below is the one used for every recent rule (admin no-Options rules, deprecated-API rules, etc.). It compresses into one cycle: doc → registry → implementation → wire-in → BAD + GOOD ITs → install → targeted IT → full IT → commit.

### Step 1 — Write the doc first

`docs/rules/<category>/<RULE_ID>.md`. Treat it as the spec: tagline, mechanism, operational impact, how to fix, when it's a false positive, detection strategy, references. The four didactic blocks in the rule registry should mirror this doc.

### Step 2 — Register the `RuleId`

Add a constant in `RuleId.java`:

```java
public static final RuleId ADMIN_ELECT_LEADERS_NO_OPTIONS = register(builder("ADMIN_ELECT_LEADERS_NO_OPTIONS")
        .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-clients")
        .docPath("docs/rules/kafka-clients/ADMIN_ELECT_LEADERS_NO_OPTIONS.md")
        .tagline("Admin.electLeaders without ElectLeadersOptions inherits the 30 s default timeout.")
        .mechanism("...")
        .impact("...")
        .whyMatters("...")
        .build());
```

Cross-link related rules in the prose with `[[rule-id-in-lowercase-with-hyphens]]`.

### Step 3 — Implement the rule

**Bytecode rule** — create `rules/<category>/<RuleClass>.java` implementing `Rule`. Use the dual-walk pattern from §4 above if the rule targets a method call (catches both direct INVOKE* and `INVOKEDYNAMIC` method-reference captures). Use `RuleContext` for the in-loop / iterating-lambda predicate when relevant. Stay method-local unless cross-method is essential.

**Property-file rule** — no Java class needed. Call `PropertyFileRule.predicate(...)` or `PropertyFileRule.literal(...)` directly inside `KafkaLinterMojo.buildRules(...)`.

### Step 4 — Wire it in

```java
addIfEnabled(rules, sev, RuleId.ADMIN_ELECT_LEADERS_NO_OPTIONS, AdminElectLeadersNoOptionsRule::new);
```

Replaces any generic `MethodCallRule::new` wiring the rule used to ride on.

### Step 5 — Author the integration-test fixtures

Every new rule gets a **BAD + GOOD pair** in `src/it/`. The two halves are non-negotiable: BAD proves the rule fires on the anti-pattern across every relevant bytecode shape; GOOD proves the rule does not false-positive on the corrected form.

```
src/it/<rule-slug>-bad/
├── pom.xml                  # ERROR severity for the rule under test; OFF for every companion rule
├── invoker.properties       # invoker.buildResult = failure
└── src/main/java/sample/Bad<RuleClass>.java   # 8 methods, each a distinct fire shape

src/it/<rule-slug>-good/
├── pom.xml                  # same ERROR severity — the GOOD fixture proves zero fires
├── invoker.properties       # invoker.buildResult = success
└── src/main/java/sample/Good<RuleClass>.java  # mirror of BAD with the corrected API
```

**The canonical 8-fire BAD structure** (one per dedicated method-call rule):

1. `xA` — direct INVOKE* on the unsafe overload, vanilla.
2. `xB` — direct INVOKE* with a meaningfully different argument value (e.g. PREFERRED vs UNCLEAN).
3. `xC` — direct INVOKE* with an intermediate local-variable binding.
4. `xD` — direct INVOKE* inside a `static` helper.
5. `biFactory` (or `triFactory` etc.) — `admin::method` as a method reference bound to a `BiFunction` / `Function` whose erased descriptor matches the unsafe overload.
6. `customFactory` — same method reference bound to a custom `@FunctionalInterface` SAM with the same erased descriptor — exercises a second `INVOKEDYNAMIC` bsm-shape.
7. `useLocalFactory` — local SAM binding via bound-receiver method reference, applied inline.
8. `xAll` — `.map(a -> a.method(...))` over a stream — the synthetic `lambda$xAll$0` carries a direct INVOKE* on the unsafe overload.

Together these cover every shape ASM will see: direct INVOKEINTERFACE/INVOKEVIRTUAL/INVOKESTATIC, two distinct indy method-reference captures, an indy-via-lambda-body shape, and the static-helper case. If your rule fires for one shape but not all eight, the dual walk has a gap.

### Step 6 — Install the plugin

```bash
mvn -q clean install -DskipTests
```

### Step 7 — Run the targeted IT

```bash
mvn -q invoker:install invoker:integration-test invoker:verify \
    -Dinvoker.test='<rule-slug>-*'
```

Then verify the BAD fixture fired **exactly N times across N distinct methods** (N = 8 for the canonical structure):

```bash
grep '<RULE_ID> sample\.' target/it/<rule-slug>-bad/build.log \
    | grep -oE 'sample\.[a-zA-Z0-9$]+#[a-zA-Z0-9$]+' \
    | sort | uniq -c
```

Eight lines, one count each. If you see fewer than eight distinct methods, the dual walk is missing a shape; if you see a higher count on one method, you're double-counting.

### Step 8 — Run the full IT suite

```bash
mvn -q invoker:install invoker:integration-test invoker:verify
```

All 520+ ITs must pass.

---

## Caveats and known limitations

- **Method-local by design.** A config built by a helper method and passed in won't be analysed end-to-end. False negatives are quieter than false positives.
- **Closed iterating-API list.** A project-specific `Iterables.forEach` won't trigger the lambda-tracking rules. `LambdaTracker.ITERATING_METHODS` is closed on purpose.
- **Inline lambdas only** for the lambda tracker — storing a lambda in a local variable before passing it to `forEach` is not tracked.
- **Java 8+ bytecode only.** The plugin assumes `INVOKEDYNAMIC`-based lambdas; pre-Java-8 anonymous-inner-class emulation is not recognised.

---

## Project layout

```
.
├── pom.xml                                            # io.conductor:kafka-linter-maven-plugin
├── docs/
│   ├── JAVA_AGENT.md
│   ├── STATIC_ANALYSIS.md
│   └── rules/                                         # one .md per rule, grouped by category
│       ├── _CATALOG.md                                # machine-readable index of all 746 rules
│       ├── kafka-clients/                             # producer / consumer / admin docs
│       ├── kafka-streams/                             # DSL + state-store docs
│       ├── spring-kafka/                              # @KafkaListener / KafkaTemplate docs
│       ├── quarkus-kafka/                             # SmallRye Reactive Messaging docs
│       ├── observability/                             # interceptors, OTel, CVE docs
│       ├── schema-registry/                           # serde / subject-strategy docs
│       ├── security/                                  # credentials / ACL docs
│       ├── versions/                                  # EOL / CVE / BOM-drift docs
│       └── good-practices/                            # cross-cutting hygiene docs
└── src/
    ├── main/java/io/conductor/kafkalinter/
    │   ├── KafkaLinterMojo.java                       # @Mojo(name="check") — wires the rule list
    │   ├── RuleId.java                                # value-class registry of all rules
    │   ├── Severity.java / Confidence.java / Violation.java
    │   ├── report/                                    # SimpleReporter, VerboseReporter
    │   ├── rules/                                     # one .java per bytecode rule
    │   │   ├── admin/                                 # AdminClient bytecode rules
    │   │   ├── clients/                               # KafkaProducer/KafkaConsumer rules
    │   │   ├── config/                                # MethodCallRule + PropertyFileRule
    │   │   ├── connect/                               # Kafka Connect bytecode rules
    │   │   ├── observability/
    │   │   ├── quarkus/
    │   │   ├── security/
    │   │   ├── spring/
    │   │   ├── streams/                               # KafkaStreams bytecode rules
    │   │   └── version/
    │   └── scanner/                                   # LoopFinder, LambdaTracker, AsmUtil, RuleContext, PropertyFileRule, KafkaTypes
    └── it/                                            # 520+ BAD/GOOD invoker-plugin fixtures
        ├── kafka-clients-{good,bad}/
        ├── kafka-streams-{good,bad}/
        ├── spring-kafka-{good,bad}/
        ├── quarkus-kafka-{good,bad}/
        └── <rule-slug>-{bad,good}/                    # one pair per dedicated rule
```
