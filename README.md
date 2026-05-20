# kafka-linter-maven-plugin
Minimal Kafka linter for the jvm ecosystem. Catch obvious issues before their bite you.

A Maven plugin that scans your compiled bytecode with [ASM](https://asm.ow2.io/) and flags common Kafka producer / consumer anti-patterns at build time. No runtime overhead, no Kafka cluster needed — just static analysis of `target/classes`.

- **Group / Artifact**: `io.conductor:kafka-linter-maven-plugin`
- **Goal**: `kafka-linter:check` (bound to the `verify` phase by default)
- **Requires**: Java 17+, Maven 3.6.3+
- **Detects**: 9 rules across producer & consumer code paths
- **Reports**: `ERROR` (fails the build) or `WARNING` (logged only); every rule is individually tunable

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

Each rule has a stable ID (used in config) and a default severity. All rules act on compiled bytecode, so they catch the problem wherever it lives — in your code, in a transitively compiled module, in a generated source, anywhere there is a `.class`.

| Rule ID                        | Default   | What it catches                                                                                                  |
|--------------------------------|-----------|------------------------------------------------------------------------------------------------------------------|
| `PRODUCER_IN_LOOP`             | `ERROR`   | `new KafkaProducer(...)` inside a `for`/`while`/`do-while` loop or inside an iterating lambda (`forEach`, `Stream.map`, …). Producers must be long-lived. |
| `CONSUMER_IN_LOOP`             | `ERROR`   | Same idea for `KafkaConsumer`.                                                                                   |
| `PRODUCER_NO_COMPRESSION`      | `ERROR`   | A method constructs a `KafkaProducer` but never sets `compression.type` anywhere in the same method.             |
| `PRODUCER_SEND_BLOCKING_GET`   | `ERROR`   | `producer.send(record).get()` — synchronous, kills batching. Use a `Callback`.                                    |
| `PRODUCER_SEND_NO_CALLBACK`    | `WARNING` | `producer.send(record)` called without a `Callback` **and** the returned `Future` is discarded — errors will go unnoticed. |
| `PRODUCER_FLUSH_IN_LOOP`       | `ERROR`   | `producer.flush()` inside a loop — defeats batching.                                                              |
| `CONSUMER_AUTO_COMMIT_TRUE`    | `WARNING` | `enable.auto.commit=true` in the consumer config — risks message loss / double processing on rebalance.           |
| `CONSUMER_COMMIT_PER_RECORD`   | `ERROR`   | `consumer.commitSync()` inside the **per-record** loop of a `poll()` cycle (commit per record). Commit per batch instead. |
| `CONSUMER_POLL_ZERO`           | `ERROR`   | `consumer.poll(0L)` or `poll(Duration.ZERO)` — busy-loops the consumer thread.                                    |

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

IT projects are named `<framework>-good` (clean usage, build should succeed) and `<framework>-bad` (one method per anti-pattern, build should fail). Today only `kafka-clients-good` and `kafka-clients-bad` exist; framework-specific IT pairs (`kafka-streams-*`, `spring-kafka-*`, `quarkus-kafka-*`) are added alongside the rules that exercise them.

- `src/it/kafka-clients-good/` — producer/consumer as singletons, `compression.type=snappy`, `enable.auto.commit=false`, `commitSync()` per batch, `poll(Duration.ofMillis(500))`, `send(...)` with a `Callback`. Expected outcome: `kafka-linter: 0 violations.` and `BUILD SUCCESS`.
- `src/it/kafka-clients-bad/` — one method per anti-pattern; every implemented rule fires at least once. Expected outcome: `kafka-linter: N violation(s) — …` and `BUILD FAILURE`.

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

1. Add a `RuleId` constant in `RuleId.java` with its default severity, confidence, category, doc path, message, and tagline.
2. Create `rules/MyNewRule.java` implementing `Rule`. Use `RuleContext` if you need loop/lambda info.
3. Wire it into `KafkaLinterMojo.buildRules(...)`.
4. Add an anti-pattern method to the matching `src/it/<framework>-bad/` source tree (create the IT pair if it doesn't exist yet).
5. Optionally add the counter-example to `<framework>-good/` to prove the rule doesn't false-positive.
6. `mvn verify` — the bad IT should report one more violation; the IT will pass because the declared failure result still matches.

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
├── src/
│   ├── main/java/io/conductor/kafkalinter/
│   │   ├── KafkaLinterMojo.java                       # @Mojo(name="check")
│   │   ├── RuleId.java                                # implemented rules + metadata registry
│   │   ├── Severity.java                              # ERROR | WARNING | INFO | OFF
│   │   ├── Confidence.java                            # HIGH | MEDIUM | CONTEXT
│   │   ├── Violation.java                             # reporting record
│   │   ├── report/                                    # SimpleReporter, VerboseReporter
│   │   ├── rules/                                     # one .java per rule
│   │   └── scanner/                                   # LoopFinder, LambdaTracker, etc.
│   └── it/
│       ├── kafka-clients-good/                        # 0 expected violations
│       └── kafka-clients-bad/                         # N expected violations
└── README.md
```
