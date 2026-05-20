# Java agent — the runtime complement to the Maven plugin

This document is a design note, not an implemented feature. It describes the class of Kafka defects that **only a JVM-attached agent** can catch, sketches how such an agent would be structured, and lays out the boundaries between static (build-time) and dynamic (runtime) analysis so they don't overlap or compete.

If you are reading this because the static linter missed something obvious to a human reading the code at runtime, the right reaction is usually: this is an agent-shaped defect, not a linter-shaped one.

---

## 1. Why a runtime detector is necessary at all

`STATIC_ANALYSIS.md` enumerates the classes of defects the Maven plugin cannot see. The short list:

1. **Externalised configuration → code binding.** Spring Boot, Quarkus, and Micronaut all assemble Kafka clients from a stack of YAML/properties files, environment variables, profile-conditional beans, and runtime DI. The linter sees the *static* assembly (literal keys in source) and the *file* (project-scoped rule), but the join — *which client instance ends up with which property* — is computed at startup. By the time the JVM has a configured `KafkaProducer`, the binding exists, in memory, and the agent can inspect it.

2. **Thread-of-control invariants.** `KafkaConsumer` is documented as not thread-safe. `KafkaProducer` is thread-safe but has subtle exceptions (transactional state machine, `close()` mid-send). Static analysis sees the call site; it does not know which thread made the call. The runtime agent does — `Thread.currentThread()` is one instrumentation hook away.

3. **EOS / transactional sequence violations.** A transaction is `beginTransaction → send×N → sendOffsetsToTransaction → commitTransaction`. Static analysis can see that all four method names appear in the same class. It cannot see whether they execute in that order on the same producer instance and on the same thread, or whether a `RuntimeException` between `sendOffsetsToTransaction` and `commitTransaction` is followed by `abortTransaction` rather than `close()`. The runtime agent can wrap the producer and observe the state machine directly.

4. **Resource lifecycle.** "Was this producer closed?" is provable at runtime by hooking `close()` and the producer's finaliser/reference queue. It is not provable statically across non-trivial codebases — the linter has a doc-only `PRODUCER_NOT_CLOSED` for exactly this reason.

5. **Deserialiser blast radius.** Jackson default typing, polymorphic Avro, JNDI lookups in deserialisers — these surface when an attacker-controlled payload hits a deserialise call. A runtime agent can intercept `Deserializer.deserialize(...)` and check the resolved configuration, the actual class graph being instantiated, and the originating bytes' provenance. The linter only checks the *configuration shape*; the agent checks the *actual behaviour*.

6. **Cluster-side behaviour.** The linter doesn't talk to a broker. A runtime agent can observe `Metadata` responses, partition counts, broker versions, ISR transitions, and `RecordTooLargeException` rates — and surface them as the *consequences* of static decisions the linter flagged (or missed).

7. **Library version actually used at runtime.** With shading, fat-JAR-of-fat-JARs, OSGi, JPMS, and Java agents-attached-by-other-agents, the version of `kafka-clients` that *resolved* in Maven is sometimes not the version that *loads* in production. A runtime agent reads the actual loaded class's package version (`Package.getImplementationVersion()`) and reports drift.

---

## 2. The shape of an agent the linter would pair with

The agent and the Maven plugin should share three things:

1. **The `RuleId` enum.** A finding emitted at runtime should carry the same identifier the linter would emit at build time. If a runtime detector finds a `CONSUMER_NOT_THREAD_SAFE` violation, the operator should be able to look up `docs/rules/kafka-clients/CONSUMER_NOT_THREAD_SAFE.md` without translation.

2. **The `Severity` / `Confidence` model.** Runtime detectors generally run at higher confidence (we observed the violation actually happen) but the severity envelope is the same.

3. **The reporter format.** A single triage tool should read both linter output (in `target/kafka-linter.json` or similar) and agent output (a JSON line per finding written to stdout / a file / a Kafka topic) without caring which side produced it.

What the agent **does not** share with the plugin:

- The detection layer. The plugin is ASM; the agent is `java.lang.instrument.Instrumentation` + ByteBuddy (or ASM directly).
- The runtime model. The plugin has zero state; the agent has per-`KafkaConsumer` / per-`KafkaProducer` ledger objects.
- The deployment surface. The plugin is `mvn verify`; the agent is `-javaagent:kafka-runtime-detector.jar` on the JVM under test.

### 2.1 Attachment

```sh
java -javaagent:/opt/kafka-runtime-detector.jar=findings=/var/log/kafka-findings.json,severity=warn \
     -jar app.jar
```

The agent's `premain` (and `agentmain`, for dynamic attachment to a running JVM) installs a `ClassFileTransformer` that:

1. **Identifies target classes by name.** Without loading them. ByteBuddy's `AgentBuilder.type(ElementMatchers.named(...))` is the idiomatic API. We target the canonical Kafka client classes (`org.apache.kafka.clients.producer.KafkaProducer`, `…consumer.KafkaConsumer`, `…streams.KafkaStreams`, the SmallRye and Spring listener container internals).

2. **Weaves advice around specific methods.** Advice is a small Java class whose static methods are inlined into the target. For `KafkaProducer.send(ProducerRecord, Callback)` an advice would:
   - capture `this`, `currentThread()`, `record`, `callback`, the producer's transactional state (`txnManager` field, accessed via a reflective accessor cached at agent init);
   - record the call into a per-producer ring buffer;
   - install a callback wrapper that observes `RecordMetadata` / `Exception` on completion;
   - update the producer's "open" state on `close()`.

3. **Emits findings.** A finding is a `RuleId` plus a small context object plus a timestamp plus the originating thread / class / method. Findings go to a configured sink — file, syslog, OpenTelemetry, or back into a control Kafka topic.

### 2.2 The ledger

Each instrumented client gets a ledger object stored in an `IdentityHashMap<KafkaProducer, ProducerLedger>` (with weak keys to avoid leaking memory after `close()`). The ledger tracks:

- creation thread,
- accumulated set of threads that have called instance methods (to detect `CONSUMER_NOT_THREAD_SAFE` and the producer's "consumer-method-from-non-creator-thread" cases),
- transactional state machine progress (`initTransactions` seen? `beginTransaction` seen? mid-`send`-after-commit?),
- count of open transactions never followed by `commit`/`abort`,
- the resolved configuration (captured at construction time from the `ProducerConfig` argument).

The ledger is what lets the agent answer "is this defect happening?" with the same rule IDs the linter uses to answer "could this defect happen?".

### 2.3 Rules an agent uniquely catches

A non-exhaustive list — these are the runtime-only signatures the agent should target first:

- **`CONSUMER_NOT_THREAD_SAFE` (runtime form).** Observed when ledger sees consumer method calls from more than one thread between two `poll()`s. The linter has a static form (`KafkaConsumer` field on a class that also has fields hinting at thread sharing) but it's `CONTEXT`-confidence. The agent's form is `HIGH`.
- **`PRODUCER_NOT_CLOSED` (runtime form).** Producer finalised without `close()` being called. Linter is doc-only.
- **`PRODUCER_USED_AFTER_CLOSE` (runtime form).** Any method call on a `KafkaProducer` after `close()` returned. Linter form is shallow.
- **`CONSUMER_NO_WAKEUP_SHUTDOWN`.** Detected when the JVM enters shutdown and a consumer is mid-`poll()` without a preceding `wakeup()`. Linter sees the missing call site; the agent confirms the hang.
- **`PRODUCER_TXN_STATE_VIOLATION`.** Any deviation from `beginTransaction → send → sendOffsetsToTransaction → commitTransaction|abortTransaction`. Static analysis can flag missing calls; only the agent can flag misordered ones.
- **`PRODUCER_RECORD_TOO_LARGE_DROPPED`.** `RecordTooLargeException` was thrown by the producer; the user's callback swallowed it. The agent observes both.
- **`HEADERS_SENSITIVE_KEYS_RUNTIME`.** Headers actually written at runtime, inspected as bytes, against the deny-list. The linter only sees literal header keys; the agent sees the runtime ones.
- **`CONFIG_DRIFT_VS_DECLARED`.** Resolved configuration on the live `KafkaProducer` doesn't match what the YAML / properties file declared (typically: env-var override forgot to land). Cross-checks the *file* the linter read with the *map* the producer was actually given.
- **`AGENT_CLASSPATH_VERSION_DRIFT`.** Runtime-loaded `kafka-clients` version differs from the version Maven resolved. Surfaces shading / fat-jar collisions invisible to `mvn verify`.

### 2.4 Rules that move from `CONTEXT`/`CONSULT` in the linter to `HIGH` in the agent

A handful of catalog rules carry a `CONTEXT` or `CONSULT` confidence specifically because the linter cannot see runtime state. The agent should re-emit those rules at `HIGH` when it directly observes the violation:

- `PRODUCER_IDEMPOTENCE_DISABLED` (linter sees literal; agent sees resolved `ProducerConfig`).
- `PRODUCER_TXN_ID_WITHOUT_IDEMPOTENCE` (same).
- `STREAMS_EOS_V1_DEPRECATED` (runtime `processing.guarantee` value).
- `CONSUMER_AUTO_COMMIT_TRUE` (resolved consumer config).
- `CONSUMER_ALLOW_AUTO_CREATE_TOPICS_TRUE` (same).
- `QK_DEVSERVICES_IN_PROD` (agent observes a Testcontainers-spawned broker in a `prod` profile — definitive).
- `SCHEMA_REGISTRY_URL_MISSING` (resolved serialiser config).

The agent doesn't replace the linter for these; it confirms or refutes the static finding with runtime evidence.

---

## 3. Boundaries: who is responsible for what

A clear split keeps both tools focused. The Maven plugin is responsible for:

- **Pre-deployment gates.** Anything that should fail CI before a binary is built.
- **Catalog-level documentation.** Each `RuleId` has a `.md` doc explaining the mechanism, impact, and fix — irrespective of detection layer.
- **Build-time signals**: artefact versions, CVE ranges, EOL, configuration *files*, annotation presence, code structure.
- **Cheap, deterministic, offline analysis.** Re-runnable in seconds on any commit.

The Java agent is responsible for:

- **In-process runtime invariants.** Threading, lifecycle, state-machine ordering, actual resolved configuration, observed cluster behaviour.
- **Confirming the linter's `CONTEXT` / `CONSULT` calls.** Promoting suspicions to facts.
- **Catching defects in code the linter can't see**: vendored JARs, dynamically loaded plugins, generated lambda call sites that escape the static analysis.
- **Production observability.** Continuous, low-overhead, with a sink (file, OTel, Kafka topic) that operators already monitor.

The split is enforced by a single rule: **a finding emitted by either tool uses the same `RuleId`.** If you can't pick a `RuleId` that both tools could legitimately emit, the rule isn't well-scoped and the doc needs to be rewritten — usually because the description conflates a *cause* (static configuration) with an *effect* (runtime behaviour) and they should be separate rules.

---

## 4. What we are not building (yet)

The agent does not exist in this repository. This document is the design we'd implement if the user asks for it. The reasons it isn't already here:

1. **The linter is the bigger lever.** Catching a defect in `mvn verify` is strictly cheaper than catching it in staging. The Maven plugin should be exhaustive *first*.
2. **The runtime detector has operational costs.** It adds JVM startup time, has to ship as a separate artefact, has to negotiate with whatever observability stack the user already runs. Justifying that requires a real adopter.
3. **The static / dynamic split is decided up front, but the agent design hardens with the linter.** Each time we add a rule to the linter that ends up `CONTEXT` or `CONSULT`, we're identifying a runtime-detector candidate. The agent should be built once the linter's catalog has settled, not while it's still moving.

When the agent does land, this document is the spec it must satisfy — same `RuleId` taxonomy, same severity/confidence model, same reporter contract. Anything else introduces drift between two tools that are meant to look like one product to the operator.

---

## 5. Alternatives we considered

**a) Bytecode rewriting at build time (a la AspectJ compile-time weaving).** Rejected because it requires every user to either weave aspects into their own JAR (intrusive) or apply a load-time weaver agent (which is what we'd do anyway). Same operational cost, fewer benefits.

**b) Sidecar / proxy approach.** A Kafka proxy that observes traffic. Catches some classes of defects (oversized records, unauthenticated producers) but cannot see the client's threading or lifecycle. Useful as a third tool, not a substitute.

**c) OpenTelemetry-only.** OTel can carry the *findings* but not produce them — it's a transport. We'd still need the agent to detect the violations and emit OTel events.

**d) APM vendor integration.** Datadog / New Relic / Dynatrace ship JVM agents that already instrument Kafka clients for metrics. They don't detect *defects*, only collect *signals*. Different product. The agent we describe here would coexist with APM agents (instrumentation namespaces are independent).

---

## 6. Reading order for someone picking this up

1. `README.md` — what the plugin does today.
2. `docs/STATIC_ANALYSIS.md` — what the plugin can and cannot see, and why.
3. This document — what an agent would catch that the plugin can't.
4. `docs/rules/_CATALOG.md` — the full rule catalog, with per-rule docs.
5. The source: `src/main/java/io/conductor/kafkalinter/rules/` — one file per rule.

A would-be agent author starts at step 3, picks five rules from step 4 whose docs say "the linter cannot see X at runtime", and prototypes the instrumentation for those five before broadening.
