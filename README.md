# kafka-linter-maven-plugin

A Maven plugin that scans your compiled bytecode with [ASM](https://asm.ow2.io/) and your `application.properties` / `application.yml` and flags common Kafka anti-patterns at build time. No runtime overhead, no Kafka cluster needed — just static analysis of `target/classes` and config files.

![rules](https://img.shields.io/badge/rules-660-c14a1f?style=for-the-badge&labelColor=16140f)
![categories](https://img.shields.io/badge/categories-10-1f3d36?style=for-the-badge&labelColor=16140f)
![java](https://img.shields.io/badge/java-17%2B-b58a3a?style=for-the-badge&labelColor=16140f)
![maven](https://img.shields.io/badge/maven-3.6.3%2B-b58a3a?style=for-the-badge&labelColor=16140f)

- **Group / Artifact**: `io.conductor:kafka-linter-maven-plugin`
- **Goal**: `kafka-linter:check` (bound to the `verify` phase by default)
- **Detects**: **660 rules** across **10 categories** — kafka-clients, spring-kafka, kafka-streams, kafka-connect, quarkus-kafka, security, versions, observability, quarkus, schema-registry.
- **Reports**: `ERROR` (fails the build), `WARNING` (logged), `INFO` (nudge); every rule is individually tunable.

---

## Rule catalog at a glance

Every rule traces to a real production failure mode — data loss, silent footgun, EOL/CVE exposure, observability gap, or the absence of a well-known production default. The full table is in [`docs/rules/_CATALOG.md`](docs/rules/_CATALOG.md). The 10 categories live under `docs/rules/`:

<table>
<tr>
<td width="33%" valign="top">

### 🧱 kafka-clients
**211 rules**

Plain Apache Kafka producers and consumers — the lowest-level surface. `acks`, `enable.idempotence`, `max.in.flight`, offsets, rebalances, headers.

</td>
<td width="33%" valign="top">

### 🌱 spring-kafka
**174 rules**

`@KafkaListener`, `KafkaTemplate`, error handlers, `ErrorHandlingDeserializer`, Spring DSL property paths, passthrough overrides.

</td>
<td width="33%" valign="top">

### 🌊 kafka-streams
**117 rules**

Streams DSL, processor API, state stores, EOS v2, timestamp extractors, rocksdb tuning, repartitioning, standby replicas.

</td>
</tr>
<tr>
<td width="33%" valign="top">

### 🔌 kafka-connect
**85 rules**

Source/sink connectors and Connect-worker config — Confluent S3/HDFS/GCS sinks, JDBC source polling, Debezium CDC, dead-letter-queue, converter pitfalls, config-provider hygiene.

</td>
<td width="33%" valign="top">

### 🐰 quarkus-kafka
**23 rules**

SmallRye Reactive Messaging — `@Incoming`/`@Outgoing`, `Emitter`, channel config, per-channel passthrough, dead-letter-queue.

</td>
<td width="33%" valign="top">

### 🔐 security
**23 rules**

Credentials, secrets, and authentication hygiene — plaintext credentials in configs, AWS-credential literals, weak SASL/SSL choices.

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

### 🐰 quarkus
**6 rules**

Quarkus-framework concerns outside of `quarkus-kafka` — extension config, native-image incompatibilities, runtime defaults.

</td>
</tr>
<tr>
<td width="33%" valign="top">

### 📋 schema-registry
**5 rules**

Schema Registry serde configuration, auto-register, subject-naming-strategy, Avro 1.12+ logical-type Java mappings.

</td>
<td width="33%" valign="top">

&nbsp;

</td>
<td width="33%" valign="top">

&nbsp;

</td>
</tr>
</table>

Every rule doc carries a four-paragraph editorial block (tagline, mechanism, impact, why-it-matters) so reading the catalog is also reading a curated guide to production Kafka. Rules whose obvious fix has a non-obvious correctness cost are marked 🤝 — read those carefully before changing prod config.

---

## Quick start

Add the plugin to the `<build>` of any project that uses `org.apache.kafka:kafka-clients`:

```xml
<plugin>
    <groupId>io.conductor</groupId>
    <artifactId>kafka-linter-maven-plugin</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <executions>
        <execution>
            <id>lint</id>
            <goals>
                <goal>check</goal>
            </goals>
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

or, when problems are found:

```
[INFO] kafka-linter: 12 violation(s) — 10 error, 2 warning.
[ERROR] PRODUCER_IN_LOOP sample.BadKafkaUsage#producerInForLoop:23 — new KafkaProducer inside loop — instantiate once and reuse.
[ERROR] PRODUCER_NO_COMPRESSION sample.BadKafkaUsage#producerWithoutCompression:52 — KafkaProducer is built here but 'compression.type' is never set in this method.
[WARNING] PRODUCER_SEND_NO_CALLBACK sample.BadKafkaUsage#producerSendNoCallback:64 — send(record) without a Callback and the returned Future is discarded — errors will go unnoticed.
…
[ERROR] kafka-linter: 10 error-severity violation(s) found.
```

Any `ERROR`-severity violation fails the build. `WARNING` is logged but does not fail.

You can also invoke it directly:

```bash
mvn kafka-linter:check
```

---

## What it detects

Each rule has a stable ID (used in config) and a default severity. All rules act on compiled bytecode (or, for property-file rules, on the YAML/properties files under `src/main/resources/`), so they catch the problem wherever it lives — in your code, in a transitively compiled module, in a generated source, anywhere there is a `.class`.

The full machine-readable catalog (all 660 rule IDs, defaults, categories, doc paths) is in [`docs/rules/_CATALOG.md`](docs/rules/_CATALOG.md). A small sampler from the core `kafka-clients` category:

| Rule ID                        | Default   | What it catches                                                                                                  |
|--------------------------------|-----------|------------------------------------------------------------------------------------------------------------------|
| `PRODUCER_IN_LOOP`             | `ERROR`   | `new KafkaProducer(...)` inside a `for`/`while`/`do-while` loop or inside an iterating lambda (`forEach`, `Stream.map`, …). Producers must be long-lived. |
| `CONSUMER_IN_LOOP`             | `ERROR`   | Same idea for `KafkaConsumer`.                                                                                   |
| `PRODUCER_NO_COMPRESSION`      | `ERROR`   | A method constructs a `KafkaProducer` but never sets `compression.type` anywhere in the same method.             |
| `CLIENT_ID_MISSING`            | `WARNING` | A `KafkaProducer`/`KafkaConsumer`/`KafkaStreams` is constructed but `client.id` is never set in the same method — broker logs see `producer-1` instead of your service. |
| `PRODUCER_SEND_BLOCKING_GET`   | `ERROR`   | `producer.send(record).get()` — synchronous, kills batching. Use a `Callback`.                                    |
| `PRODUCER_SEND_NO_CALLBACK`    | `WARNING` | `producer.send(record)` called without a `Callback` **and** the returned `Future` is discarded — errors will go unnoticed. |
| `PRODUCER_FLUSH_IN_LOOP`       | `ERROR`   | `producer.flush()` inside a loop — defeats batching.                                                              |
| `CONSUMER_AUTO_COMMIT_TRUE`    | `WARNING` | `enable.auto.commit=true` in the consumer config — risks message loss / double processing on rebalance.           |
| `CONSUMER_COMMIT_PER_RECORD`   | `ERROR`   | `consumer.commitSync()` inside the **per-record** loop of a `poll()` cycle (commit per record). Commit per batch instead. |
| `CONSUMER_POLL_ZERO`           | `ERROR`   | `consumer.poll(0L)` or `poll(Duration.ZERO)` — busy-loops the consumer thread.                                    |
| `KAFKA_BOOTSTRAP_SERVERS_LOCALHOST` | `ERROR` | `bootstrap.servers` contains `localhost` or `127.0.0.1` in a packaged artifact — that's a build-config leak, not a default. |
| `KAFKA_BOOTSTRAP_SERVERS_SINGLE_BROKER` | `ERROR` | `bootstrap.servers` lists exactly one broker — defeats the bootstrap failover contract clients rely on. |

A small sampler from the `kafka-connect` category (Connect worker / source / sink config; `.properties` files under `src/main/resources/`):

| Rule ID                                            | Default   | What it catches                                                                                                  |
|----------------------------------------------------|-----------|------------------------------------------------------------------------------------------------------------------|
| `CONNECT_S3_SINK_FLUSH_SIZE_TOO_SMALL`             | `WARNING` | S3 sink with `flush.size` < 100 — small-object storm: thousands of tiny files crippling downstream scan jobs and S3 list APIs. |
| `CONNECT_STORAGE_SINK_PARTITION_DURATION_MS_TOO_LOW` | `WARNING` | Time-based partitioner with `partition.duration.ms` < 60000 ms — sub-minute partition windows multiply object count, the storm seen from a different axis. |
| `CONNECT_STORAGE_SINK_ROTATE_INTERVAL_MS_TOO_LOW`  | `WARNING` | `rotate.interval.ms` < 60000 ms — event-time-based file rotation under a minute; same small-object-storm risk as the other two cadence rules. |
| `CONNECT_JDBC_SOURCE_POLL_INTERVAL_MS_TOO_LOW`     | `WARNING` | Confluent JDBC source with `poll.interval.ms` < 1000 ms — sub-second DB polling saturates connection pools, pins MVCC snapshots, and amplifies WAL/redo activity. For genuine sub-second freshness use Debezium. |
| `CONNECT_S3_SINK_S3_PART_SIZE_TOO_SMALL`           | `WARNING` | `s3.part.size` below the S3 multipart-upload floor (5 MiB) — S3 rejects parts below 5 MiB, upload fails. |
| `CONNECT_JDBC_SOURCE_MODE_COLUMN_MISSING`          | `ERROR`   | `mode=incrementing` or `mode=timestamp` without the corresponding `incrementing.column.name` / `timestamp.column.name` — connector refuses to start at runtime. |

### How "in a loop" is detected

The plugin recognises two iteration contexts uniformly:

1. **Classical loops.** ASM walks each method's instruction list and identifies *back-edges*: any `JumpInsnNode`, `TableSwitchInsnNode`, or `LookupSwitchInsnNode` whose target label appears **earlier** in the method than the jump instruction itself. Every instruction between the target and the jump is "inside a loop". This catches `for`, `while`, `do-while`, and labelled jumps without needing source code.

2. **Iterating lambdas.** When the compiler emits an `INVOKEDYNAMIC` referencing `LambdaMetafactory`, the body of the lambda becomes a synthetic method (`lambda$foo$0`). If the very next instruction passes that lambda to one of a known set of iterating APIs — `Iterable.forEach`, `Collection.forEach`, `List.forEach`, `Set.forEach`, `Map.forEach`, `Stream.{forEach,forEachOrdered,map,filter,peek,flatMap}` — then everything inside the synthetic lambda method is treated as "in a loop".

Both contexts feed into one predicate: `RuleContext.isInLoopOrIteratingLambda(method, insn)`. The `PRODUCER_IN_LOOP`, `CONSUMER_IN_LOOP`, and `PRODUCER_FLUSH_IN_LOOP` rules all share it.

### How `CONSUMER_COMMIT_PER_RECORD` avoids the canonical commit-per-batch pattern

The naïve "any `commitSync()` inside any loop in a method that calls `poll()`" check would false-positive on the textbook idiom:

```java
while (running()) {
    ConsumerRecords<K,V> records = consumer.poll(Duration.ofMillis(500));
    for (ConsumerRecord<K,V> r : records) {
        handle(r);
    }
    consumer.commitSync();   // legitimate: per batch
}
```

The rule narrows this down: it only flags `commitSync()` calls whose enclosing back-edge target appears **after** the first `poll()` call in the method. That singles out the *inner* per-record loop while leaving the outer poll-while loop alone.

### How `PRODUCER_SEND_NO_CALLBACK` distinguishes fire-and-forget from legitimate `send()`

Three shapes of `send()` in bytecode:

- `producer.send(record)` followed by `POP` — the returned `Future` is dropped on the stack. **Fire-and-forget. Flagged.**
- `Future f = producer.send(record)` — no `POP`, the value is consumed. **Not flagged.**
- `producer.send(record).get()` — followed by an `INVOKE` of `Future.get`. **Handled by `PRODUCER_SEND_BLOCKING_GET` instead.**

### How `PRODUCER_NO_COMPRESSION` works

For each method that contains `new KafkaProducer(...)`, the rule scans the instruction list for any `LDC` of the literal string `"compression.type"`, or for any `INVOKE` of `ProducerConfig.COMPRESSION_TYPE_CONFIG`. If none is found in the same method, the construction site is flagged. (Cross-method config builders go undetected — by design, to keep the analysis local and predictable.)

### How `CONSUMER_AUTO_COMMIT_TRUE` works

The rule looks for `Properties.put` / `Properties.setProperty` calls whose first argument is the literal `"enable.auto.commit"` (or `ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG`) and whose second argument is the literal string `"true"` or boolean `true`.

---

## Configuration

### Per-rule severity overrides

You can change any rule's severity to `ERROR`, `WARNING`, or `OFF`:

```xml
<plugin>
    <groupId>io.conductor</groupId>
    <artifactId>kafka-linter-maven-plugin</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <configuration>
        <severities>
            <PRODUCER_SEND_NO_CALLBACK>ERROR</PRODUCER_SEND_NO_CALLBACK>
            <CONSUMER_AUTO_COMMIT_TRUE>OFF</CONSUMER_AUTO_COMMIT_TRUE>
        </severities>
    </configuration>
    <executions>
        <execution>
            <id>lint</id>
            <goals><goal>check</goal></goals>
        </execution>
    </executions>
</plugin>
```

Unknown rule IDs or severities are logged as warnings and ignored.

### Other parameters

| Parameter                          | Property                            | Default                              | Effect                                            |
|------------------------------------|-------------------------------------|--------------------------------------|---------------------------------------------------|
| `<skip>`                           | `kafka-linter.skip`                 | `false`                              | Skip the check entirely.                          |
| `<classesDirectory>`               | `kafka-linter.classesDirectory`     | `${project.build.outputDirectory}`   | Override the directory of compiled classes to scan. |
| `<severities>` (Map<String,String>)| —                                   | (defaults from `RuleId`)             | Per-rule severity overrides.                      |

Examples:

```bash
mvn verify -Dkafka-linter.skip=true
mvn kafka-linter:check -Dkafka-linter.classesDirectory=build/classes/java/main
```

---

## How it works under the hood

```
target/classes/**/*.class
        │
        ▼
ProjectScanner       — walks the directory tree
        │
        ▼
ClassAnalyzer        — for each .class: ClassReader → ClassNode
        │
        ▼
RuleContext          — per-class: LoopFinder + LambdaTracker
        │  (one shared "is this instruction in a loop?" predicate)
        ▼
Rule[]               — each rule receives (ClassNode, MethodNode, RuleContext)
        │  emits Violation objects with severity + source line
        ▼
KafkaLinterMojo      — aggregates, logs, fails the build on ERROR
```

Key files (under `src/main/java/io/conductor/kafkalinter/`):

- `KafkaLinterMojo.java` — the `@Mojo(name="check")` entry point.
- `scanner/LoopFinder.java` — back-edge detection over an `InsnList`.
- `scanner/LambdaTracker.java` — `INVOKEDYNAMIC` + iterating-API detection.
- `scanner/RuleContext.java` — combines the two predicates into one.
- `rules/*.java` — one file per rule.
- `RuleId.java` / `Severity.java` / `Violation.java` — the reporting model.

The plugin has no runtime dependency on `kafka-clients` — it identifies Kafka types by their internal name (`org/apache/kafka/clients/producer/KafkaProducer`, etc.), so the linter itself stays a small JAR (~ASM + Maven plugin API).

---

## Testing

Two layers of tests live in this repo.

### Integration tests (`src/it/`)

Two sample Maven projects exercise the plugin end-to-end through [`maven-invoker-plugin`](https://maven.apache.org/plugins/maven-invoker-plugin/):

IT projects are named `<framework>-good` (clean usage, build should succeed) and `<framework>-bad` (one method per anti-pattern, build should fail). Four framework pairs exist:

- `kafka-clients-{good,bad}` — plain Apache Kafka producer/consumer surface.
- `kafka-streams-{good,bad}` — DSL + processor API, state stores, EOS.
- `spring-kafka-{good,bad}` — `@KafkaListener`, `KafkaTemplate`, error handlers, Spring DSL property paths.
- `quarkus-kafka-{good,bad}` — SmallRye Reactive Messaging channels, `@Incoming`/`@Outgoing`, `Emitter`.

`-good` projects expect `kafka-linter: 0 violations.` and `BUILD SUCCESS`. `-bad` projects expect `BUILD FAILURE` with at least one violation per anti-pattern method.

Each IT has an `invoker.properties` declaring the expected build result:

```properties
# kafka-clients-good/invoker.properties
invoker.goals = verify
invoker.buildResult = success

# kafka-clients-bad/invoker.properties
invoker.goals = verify
invoker.buildResult = failure
```

The build passes iff the actual result matches the declared one — the plugin's own pass/fail behavior **is** the assertion. No post-build BeanShell/Groovy scripts needed.

### Running the full build

```bash
mvn clean verify
```

This:

1. Compiles the plugin.
2. Runs the (currently empty placeholder) JUnit suite via Surefire.
3. Packages the plugin JAR.
4. Installs it into the local repository (`maven-invoker-plugin install` goal).
5. Builds each IT in `src/it/` in a clone under `target/it/`, invoking the plugin on its `target/classes`.
6. Compares the actual build result against `invoker.buildResult`.

With `<streamLogs>true</streamLogs>`, you'll see every IT's log inline — useful for eyeballing which rule IDs fired in the bad project.

### Adding a new rule

1. Write the doc first: `docs/rules/<category>/<RULE_ID>.md`. Treat it as the spec — tagline, what's happening (mechanism), operational impact, how to fix, when it's a false positive, detection strategy, references.
2. Register the `RuleId` in `RuleId.java` with default severity, confidence, category, doc path, message, and the four didactic blocks (`tagline`/`mechanism`/`impact`/`whyMatters`). Cross-link related rules with `[[rule-id-in-lowercase-with-hyphens]]`.
3. Implement the rule:
   - **Bytecode rule**: create `rules/<category>/MyNewRule.java` implementing `Rule`. Use `RuleContext` for loop/lambda info. Same-method scope unless cross-method is essential — keeps the heuristic predictable.
   - **Property-file rule**: no Java class needed — call `PropertyFileRule.predicate(...)` or `PropertyFileRule.literal(...)` directly in `KafkaLinterMojo`.
4. Wire it into `KafkaLinterMojo.buildRules(...)`.
5. Add an anti-pattern method (or property) to the matching `src/it/<framework>-bad/` source tree.
6. Add the counter-example to `<framework>-good/` to prove the rule doesn't false-positive — the `-good` IT must keep `0 violations`.
7. `mvn install -DskipTests=false` — all 8 ITs must pass.

---

## Caveats and known limitations

- The "no compression" and "auto.commit=true" rules are **method-local**: a config built by a helper method and passed in won't be analysed end-to-end. This is intentional — keeps the analysis simple, fast, and predictable. False negatives are quieter than false positives.
- Custom iteration wrappers (a project-specific `Iterables.forEach`) won't trigger the lambda-tracking rules. The iterating-API list in `LambdaTracker.ITERATING_METHODS` is closed by design.
- Storing a lambda in a local variable before passing it to `forEach` is not tracked. Inline lambdas only.
- The plugin assumes Java 8+ bytecode (anything that uses `InvokeDynamic` for lambdas). Pre-Java-8 anonymous-inner-class lambda emulation isn't recognised.

---

## Project layout

```
.
├── pom.xml                                            # io.conductor:kafka-linter-maven-plugin
├── docs/
│   └── rules/                                         # one .md per rule, grouped by category
│       ├── _CATALOG.md                                # generated machine-readable index of all 232 rules
│       ├── kafka-clients/                             # 40 rule docs + _INDEX.md
│       ├── kafka-streams/                             # 47 rule docs + _INDEX.md
│       ├── spring-kafka/                              # 32 rule docs + _INDEX.md
│       ├── quarkus-kafka/                             # 36 rule docs + _INDEX.md
│       ├── observability/                             # 14 rule docs + _INDEX.md
│       ├── schema-registry/                           # 7 rule docs + _INDEX.md
│       ├── warpstream/                                # 4 rule docs + _INDEX.md
│       ├── versions/                                  # 28 rule docs + _INDEX.md
│       └── good-practices/                            # 24 rule docs + _INDEX.md
├── src/
│   ├── main/java/io/conductor/kafkalinter/
│   │   ├── KafkaLinterMojo.java                       # @Mojo(name="check") — wires the rule list
│   │   ├── RuleId.java                                # rule registry: id, severity, confidence, doc path, tagline/mechanism/impact/whyMatters
│   │   ├── Severity.java                              # ERROR | WARNING | INFO | OFF
│   │   ├── Confidence.java                            # HIGH | MEDIUM | CONTEXT
│   │   ├── Violation.java                             # reporting record
│   │   ├── report/                                    # SimpleReporter, VerboseReporter
│   │   ├── rules/                                     # one .java per bytecode rule, grouped by category
│   │   │   ├── clients/                               # KafkaProducer/KafkaConsumer bytecode rules
│   │   │   ├── streams/                               # KafkaStreams bytecode rules
│   │   │   ├── spring/                                # Spring-Kafka bytecode rules
│   │   │   └── ...
│   │   └── scanner/                                   # LoopFinder, LambdaTracker, PropertyFileRule, RuleContext
│   └── it/
│       ├── kafka-clients-{good,bad}/                  # plain producer/consumer ITs
│       ├── kafka-streams-{good,bad}/                  # streams DSL ITs
│       ├── spring-kafka-{good,bad}/                   # @KafkaListener / KafkaTemplate ITs
│       └── quarkus-kafka-{good,bad}/                  # SmallRye Reactive Messaging ITs
└── README.md
```
