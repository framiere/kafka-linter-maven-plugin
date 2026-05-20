# Static analysis — what this plugin sees, and what it doesn't

This document is a long-form companion to `README.md`. The README tells you **what** the plugin does and **how to use it**. This document tells you **why the analysis is shaped the way it is**, so that when a rule fails to fire on code that looks broken — or fires on code that looks fine — you can reason about it instead of guessing.

The audience is a Kafka practitioner who is comfortable with Java but has never written a static analyser. No prior ASM knowledge is assumed.

---

## 1. The two-layer model

The plugin runs in two distinct analysis layers, and every rule in the catalog lives in exactly one of them.

### 1.1 Bytecode-scoped rules (the `Rule` interface)

These rules walk **one class at a time**. They are given:

- the parsed `ClassNode` (ASM Tree API),
- each `MethodNode` inside it,
- a `RuleContext` carrying per-class precomputed predicates (`isInLoopOrIteratingLambda`).

They produce `Violation` objects whose `className` is the fully-qualified internal name (`org/example/MyService`) and whose `line` is the source line emitted by `javac` into the `LineNumberTable` attribute (when the project was compiled with `-g`, which is the default).

The vast majority of behavioural rules live here: anything that looks at *what the code does* — calls, instantiations, control flow, configuration literals — is bytecode-scoped.

### 1.2 Project-scoped rules (the `ProjectScopedRule` interface)

These rules see the entire project as a single read-only snapshot (`ProjectContext`):

- the resolved Maven artifact graph (`groupId:artifactId → Artifact`, including the resolved version),
- the `maven.compiler.source` / `target` / `release` property values,
- every `application.properties` and `application.yml` under `src/main/resources`,
- a `relativize(Path)` helper so emitted violations carry project-relative paths instead of a developer's home directory.

They produce violations whose `className` is a short, symbolic locator — `pom.xml`, `src/main/resources/application.properties`, `kafka-clients:3.4.0` — paired with a `methodName` that names the location *within* that artefact (e.g. `dependency:io.quarkus:quarkus-smallrye-reactive-messaging-kafka` or `configuration`).

Why a separate interface? Because the questions these rules answer aren't about *code*. They're about the **shape of the build**: which artefact is on the classpath, in which version, with which CVE history; which Java level the project compiles to; whether a framework-level guard property is set. Forcing those questions through ASM would be ridiculous — they'd have to be inferred from class file names and MANIFEST entries, which is much weaker information than `MavenProject.getArtifacts()` already gives us for free.

The split also means project-scoped rules **don't need classes at all**. A project that doesn't compile, or whose `target/classes` is empty, still gets a complete CVE / EOL / version audit on the next `mvn verify`.

---

## 2. Why ASM, and what ASM lets us do

We use [ASM](https://asm.ow2.io/) (specifically the Tree API: `ClassNode`, `MethodNode`, `InsnList`, `AbstractInsnNode`) rather than any of the alternatives:

| Alternative | Why we didn't pick it |
|---|---|
| **Reflection** | Requires loading the classes into a JVM that has the project's runtime dependencies on the classpath. Defeats the "no Kafka cluster needed" goal. |
| **JavaParser / Spoon (source-level)** | Forces a re-parse of the project's source tree, must understand the build's source roots and generated-sources directories, and misses anything that came from a generated `.class` (Avro stubs, Lombok output, Quarkus build-step classes). |
| **Error Prone / Checkstyle** | Same source-level constraint; also tightly coupled to a specific compiler. |
| **JavaPoet / `javap`-style text** | Can't be queried programmatically without re-parsing strings. |

ASM Tree gives us:

1. **Class structure**: name, superclass, interfaces, visible/invisible annotations on the class, fields, methods.
2. **Method structure**: name, descriptor (signature in JVM-internal form), visible/invisible annotations on the method *and* on its parameters.
3. **Method body as a doubly-linked list of `AbstractInsnNode`**: one node per JVM instruction (`ILOAD`, `INVOKEVIRTUAL`, `INVOKEDYNAMIC`, `LDC`, `IFEQ`, `GOTO`, `TABLESWITCH`, …). Each call site we care about (`producer.send(...)`, `new KafkaProducer(...)`, `properties.put("key", "true")`) is a `MethodInsnNode` or a sequence anchored by one.
4. **Constant pool access**: `LdcInsnNode` carries the literal strings, ints, doubles, types, and method handles that `javac` interned. This is how the linter recognises configuration keys (`"compression.type"`, `"enable.auto.commit"`) without ever loading the class.
5. **Line numbers**: `LineNumberNode` instructions are interleaved with executable instructions. Walking backwards from a flagged instruction gives the source line `javac` recorded — exactly what an IDE jumps to.

The Tree API (vs. the Visitor API) is the right choice when you need to walk back and forth in the instruction list (to find the literal that preceded a `put` call, or the back-edge target of a jump). The visitor API would force a re-scan per question.

---

## 3. What static analysis can — and cannot — see

This is the section that should make every false negative make sense.

### 3.1 What we see well

**a) Configuration keys written as literals.** `props.put("enable.auto.commit", "true")` is two `LDC` instructions feeding `Properties.put`. The linter sees both literals and the call. This is how `CONSUMER_AUTO_COMMIT_TRUE`, `PRODUCER_ACKS_ZERO`, `CONSUMER_ALLOW_AUTO_CREATE_TOPICS_TRUE`, and most of the kafka-clients config rules work. Confidence: HIGH.

**b) Configuration keys written via constants.** `props.put(ProducerConfig.ACKS_CONFIG, "0")` resolves the constant at compile time — `javac` inlines the literal `"acks"` into the caller's constant pool. The linter sees the literal, not the field reference. Same confidence as (a).

**c) Method calls to known owner/method pairs.** `Thread.sleep(...)`, `Future.get()`, `KafkaStreams.cleanUp()`, `KStream.through(...)`, `consumer.commitSync()` — all are `MethodInsnNode(owner, name, descriptor)` and trivial to match.

**d) Annotation presence.** `@KafkaListener`, `@Incoming`, `@Blocking`, `@Async`, `@AvroGenerated` are recorded by `javac` in the class file's `RuntimeVisibleAnnotations` or `RuntimeInvisibleAnnotations` attribute. We check both — many annotations are `SOURCE`/`CLASS` retention and only appear in the invisible list. Confidence: HIGH.

**e) Type instantiation.** `new KafkaProducer<>(props)` compiles to `NEW org/apache/kafka/clients/producer/KafkaProducer` followed by `DUP` and an `<init>` call. Catching the `NEW` opcode of a known FQCN is exact.

**f) Loops at the bytecode level.** A `for`, `while`, or `do-while` always compiles to a *back-edge*: a `GOTO`, `IF*`, `TABLESWITCH`, or `LOOKUPSWITCH` whose target label appears earlier in the method than the instruction itself. We compute the back-edges once per method and reuse the predicate. This handles labelled jumps, irregular loops, and compiler-emitted control flow without needing source.

**g) Iterating lambdas.** When `javac` sees `stream.forEach(x -> doIt(x))`, it emits an `INVOKEDYNAMIC` referencing `LambdaMetafactory.metafactory` whose target is a synthetic method (`lambda$foo$0`). The very next instruction is an `INVOKEINTERFACE` of `Stream.forEach`. We recognise the pair and treat the synthetic method as "inside a loop" — that's how `PRODUCER_IN_LOOP` catches `streams.forEach(rec -> new KafkaProducer(...))`.

**h) Cross-key constraints within one class.** When two configuration keys interact (`transactional.id` set + `enable.idempotence=false`), we collect both literals during the same class walk and emit a violation only when both are present. This is how `PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE` and `SPRING_ERROR_HANDLING_DESERIALIZER_NO_DELEGATES` work.

**i) Maven dependency facts.** From `ProjectContext` we know every resolved transitive artefact, its version, and its scope. CVE-by-version-range rules, EOL rules, and "old artefact name was renamed" rules live here.

### 3.2 What we cannot see — and the rules we deliberately don't write

**a) Values that originate at runtime.** `props.put("enable.auto.commit", flag ? "true" : "false")` puts a `ICONST` and a branch into the bytecode where the literal would otherwise sit. The linter doesn't reason about the branch, so it can't say whether the key is `"true"` at runtime. **Heuristic**: when the value isn't a literal, we don't fire. False negative, by design.

**b) Configuration assembled in a helper.** If a method calls `Configs.producerDefaults()` which returns a populated `Properties`, the literals are in the helper, not in the caller. The linter is *method-local* for performance and predictability — chasing assignments across methods would require alias analysis and would still miss anything routed through `Map`-returning factories. **Heuristic**: rules that look for "key X must be set when class Y is constructed" only check the same method. False negatives are preferred to false positives.

**c) Reflection.** `Class.forName("org.apache.kafka.clients.producer.KafkaProducer").getConstructor(...).newInstance(props)` doesn't emit a `NEW` opcode for `KafkaProducer`. We don't model reflection. If you instantiate Kafka clients reflectively, the linter sees nothing.

**d) Externalised configuration.** A YAML / properties file is parsed by Spring or Quarkus at runtime; the linter cannot, in general, statically connect `spring.kafka.consumer.properties.enable.auto.commit=true` in `application.yml` to the actual `KafkaConsumer` instance. **Partial workaround**: project-scoped rules read those files directly and check well-known framework keys (`mp.messaging.incoming.X.connector`, `spring.kafka.consumer.*`, `%prod.quarkus.kafka.devservices.enabled`). But the *binding* between a config file and a particular code path is framework-specific and we don't synthesise it.

**e) Thread-of-control / concurrency invariants.** "This consumer is shared across two threads" or "this producer is closed before this `send` returns" needs flow-sensitive analysis with happens-before reasoning. Out of scope for a Maven plugin.

**f) Dynamic dispatch.** When an interface method is called (`Serializer.serialize`), we know the static type but not the concrete one. Rules that say "you must use serialiser X" can only check the static reference. If serdes are injected via DI, we can't see the binding.

**g) Generated code we never read.** Avro `SpecificRecord` stubs, Lombok output, Mapstruct mappers — these *are* `.class` files and we *do* analyse them. But framework-generated classes that live in JARs we don't recurse into (Quarkus build-step output baked into `quarkus-run.jar`, Spring auto-configuration classes) are outside our scope. We analyse what the user's `target/classes` actually contains.

### 3.3 Confidence levels are how we communicate these limits

Every rule in `_CATALOG.md` carries a confidence level:

- **`HIGH`** — descriptor-level match. The detection is exact within the class. False positives are rare and almost always indicate a deliberate workaround.
- **`MEDIUM`** — heuristic detection: cross-method walk, structural pattern, "the key is in this method and the constructor is in this method, both at once". False positives possible; the doc explains when.
- **`CONTEXT`** — depends on signals the linter can't see (broker version, scraper configuration, deployment shape, profile activation, runtime DI binding). The rule fires; whether it's a real defect requires looking at the surrounding infrastructure.
- **`CONSULT`** — the fix has a non-obvious correctness cost (almost always EOS / transactions / isolation). The rule fires *and* the doc points the reader to the `## Consult a friend?` block.

When you triage a violation, the confidence tells you how much human judgement to spend on it. `HIGH` is usually "fix it." `CONTEXT` and `CONSULT` are usually "open the doc, read the Consult block, decide."

---

## 4. The shape of a rule

Concretely, a bytecode rule looks like this (paraphrased from `MethodCallRule`):

```java
public final class MethodCallRule implements Rule {
    private final RuleId ruleId;
    private final Severity severity;
    private final Set<String> owners;         // ASM internal names, e.g. "org/apache/kafka/streams/KafkaStreams"
    private final Set<String> methodNames;    // e.g. "cleanUp"
    private final String detail;

    @Override
    public List<Violation> check(ClassNode cn, MethodNode mn, RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (AbstractInsnNode insn : mn.instructions) {
            if (insn instanceof MethodInsnNode m
                    && owners.contains(m.owner)
                    && methodNames.contains(m.name)) {
                out.add(new Violation(ruleId, severity, cn.name, mn.name,
                        ctx.lineOf(insn), detail));
            }
        }
        return out;
    }
}
```

That's it. No type inference, no flow graph, no symbol table. The whole plugin is a few hundred such loops, sharing helpers (`isInLoopOrIteratingLambda`, `lineOf`, `findLiteralBefore`) so the rule files stay short and reviewable.

A project-scoped rule is even simpler:

```java
public final class QkDevservicesInProdRule implements ProjectScopedRule {
    @Override
    public List<Violation> check(ProjectContext ctx) {
        if (!isQuarkusKafkaProject(ctx)) return List.of();
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            String guard = e.getValue().getProperty("%prod.quarkus.kafka.devservices.enabled");
            if (!"false".equalsIgnoreCase(String.valueOf(guard).trim())) {
                out.add(new Violation(/* … */));
            }
        }
        return out;
    }
}
```

The simplicity of these signatures is deliberate. Every contributor — including someone who has never written ASM — can read a rule file end-to-end in under a minute. That's the only way a 200-rule catalog stays maintainable.

---

## 5. Tradeoffs we accepted

Every static analyser is a stack of compromises. Here are ours.

**Method-local over global.** We never trace a value across method boundaries. The cost is false negatives on "builder helper" patterns. The benefit is that the analysis is `O(instructions)` per class, deterministic, and easy to debug. Global analysis on real-world projects routinely takes minutes; ours runs in under a second for a typical Spring Boot service.

**Heuristic over sound.** We will miss things (false negatives), and we accept that. We will *not* knowingly fire on safe code (false positives). The reasoning: a linter that cries wolf is muted; a linter with known gaps is supplemented (with code review, runtime observability, or a Java agent — see `JAVA_AGENT.md`).

**Literal-only matching.** Configuration keys must appear as literals (directly or via `javac`-inlined constants) for us to recognise them. Computed keys (`String key = "enable." + name;`) are invisible. This excludes a handful of legitimate dynamic-config patterns; in exchange we never false-positive on identifier-shaped strings that *look* like Kafka keys but aren't.

**No transitive class scan by default.** We walk `target/classes` of the project under analysis. We don't pull in JARs the project depends on. This means a Kafka anti-pattern *inside* a vendored library JAR won't be flagged — only the user's own code is in scope. Reasoning: the user can fix their own code; a vendored library is the vendor's problem, and the noise would be enormous.

**One class, no cross-class reasoning.** Rules see one `ClassNode` at a time. Cross-class invariants ("Producer constructed in Service.java is closed in Cleanup.java") would need a whole-program build, which contradicts every other goal here. We split such checks into the closest local approximations — see how `PRODUCER_NOT_CLOSED` is doc-only because the only reliable form of it is runtime detection.

**Severity is what fails the build; confidence is what your eyes need.** We never let a `MEDIUM`/`CONTEXT` rule default to `ERROR`. The user can promote one if they're willing to own the noise.

---

## 6. When static analysis is the wrong tool

If you read sections 3.2 and 5 and think "the things you can't see are exactly the things I care about" — you're right, and you want a runtime detector. The companion document `JAVA_AGENT.md` lays out the design space for a Java agent that catches the runtime-only defects: thread-of-control violations, externalised config that diverges from what the code assumes, EOS commit sequences that look correct statically but interleave incorrectly at runtime, deserialiser injection points, and so on. The two tools are complementary, not competing.

The Maven plugin will tell you, before you ship, that you wrote `props.put("enable.idempotence", "false")` next to a `transactional.id`. The runtime agent will tell you, in staging, that two threads share the same `KafkaProducer` instance and one of them is closing it from inside a shutdown hook while the other is mid-`send`. You want both.
