# Contributing — adding a new lint rule

This document is the **operating playbook for one full rule cycle**. It captures the conventions distilled over many rule additions so a new contributor can land a green, didactic, indy-safe rule end-to-end without re-deriving the pattern.

Read [`README.md`](README.md) first for the high-level *why* and *how it works*. This file is the *what to type, in what order*.

> **Editorial discipline (non-negotiable).** Every rule traces to a real production failure mode — data loss, runtime defect, EOL/CVE exposure, observability gap, or the absence of a well-known production default. No stylistic nits. No "we prefer". If you can't write four didactic paragraphs (tagline / mechanism / impact / why-it-matters) that an on-call SRE would nod at, the rule isn't ready.

---

## The cycle, in one paragraph

Pick a generically-wired rule (or a registered-but-not-implemented `RuleId`). Replace its generic `MethodCallRule` wiring with a dedicated `XRule` class that does a dual walk — `MethodInsnNode` for direct INVOKE\*, `InvokeDynamicInsnNode` for method-reference captures. Add the corresponding `RuleId` constant with all four didactic blocks filled. Author a BAD + GOOD IT fixture pair under `src/it/` — the BAD has **exactly 8 fire shapes across 8 distinct methods**, the GOOD mirrors them with the safe API and proves zero fires. Install the plugin locally, run the targeted IT, verify the per-method fire count with a one-liner, run the full IT suite, commit Conventional Commits + Co-Authored-By trailer, push. One cycle, one commit, one rule.

---

## Step 1 — Pick the rule

Two valid starting points:

1. **A registered-but-generic rule.** Search `KafkaLinterMojo.java` for `new MethodCallRule(...)`. Each remaining one is a generic wiring that hasn't been promoted to a dedicated class yet. Indy method-reference captures bypass it — that's the gap you're closing.
2. **A registered-but-unimplemented `RuleId`.** Some `RuleId` constants are listed but not wired in. Wire one in.

Don't introduce a new rule that doesn't already have a registered `RuleId` — register it first (step 3) before implementing.

---

## Step 2 — Write the rule doc

`docs/rules/<category>/<RULE_ID>.md`. Treat the doc as the spec for the four didactic blocks in `RuleId.java`. The contents should answer, in order:

- **Tagline.** One sentence that names the API surface and the operational failure mode in plain prose.
- **Mechanism.** What's actually happening at the protocol / runtime / bytecode level. Include defaults (e.g. "the no-Options overload inherits the ~30 s `request.timeout.ms`").
- **Operational impact.** A concrete failure scenario — Cruise Control's preferred-leader-election job times out mid-batch; a Strimzi reconciliation marks `KafkaTopic` NotReady; an UNCLEAN election retry compounds data loss. Real cluster shapes, real client libraries, real symptoms.
- **How to fix.** The migration in one or two lines of code. Cite the specific Options class and a sensible default value.
- **When it's a false positive.** Be honest. If the rule fires on method-local detection only, say so.
- **Detection strategy.** Which scanner predicates / walks it uses.
- **References.** Linked KIPs, Apache Kafka source, sibling rules.

Cross-link related rules in prose with `[[rule-id-in-lowercase-with-hyphens]]`. Mark rules whose obvious fix has a non-obvious correctness cost with 🤝.

---

## Step 3 — Register the `RuleId`

In `RuleId.java`, alphabetically within the category section:

```java
public static final RuleId ADMIN_ELECT_LEADERS_NO_OPTIONS = register(builder("ADMIN_ELECT_LEADERS_NO_OPTIONS")
        .defaultSeverity(Severity.ERROR).confidence(Confidence.HIGH).category("kafka-clients")
        .docPath("kafka-clients/ADMIN_ELECT_LEADERS_NO_OPTIONS.md")
        .message("One-line on what fires and the migration. Carries verbatim into the violation message when the rule's class doesn't override it.")
        .tagline("...")
        .mechanism("...")
        .impact("...")
        .whyMatters("...")
        .build());
```

Confidence guide:

- `HIGH` — descriptor-level match (the call signature alone is the proof).
- `MEDIUM` — heuristic or cross-method walk; some false positives possible.
- `CONTEXT` — depends on environment / runtime signals the linter cannot see.

Severity guide:

- `ERROR` — production data loss, runtime defect that will fire, or unmistakable broken invariant.
- `WARNING` — silent footgun, observability gap, correctness risk under load.
- `INFO` — best-practice nudge.

---

## Step 4 — Implement the rule class

For a method-call rule (the most common shape), use this template. Substitute the bracketed names. The dual walk is the load-bearing pattern — **do not omit the `InvokeDynamicInsnNode` branch**, otherwise method-reference captures slip through.

```java
package io.conductor.kafkalinter.rules.admin;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.Rule;
import io.conductor.kafkalinter.scanner.AsmUtil;
import io.conductor.kafkalinter.scanner.KafkaTypes;
import io.conductor.kafkalinter.scanner.RuleContext;
import org.objectweb.asm.Handle;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class AdminElectLeadersNoOptionsRule implements Rule {

    private static final Set<String> OWNERS = KafkaTypes.ADMIN_OWNERS;
    private static final String METHOD_NAME = "electLeaders";
    private static final String OPTIONS_TYPE_TOKEN =
            "Lorg/apache/kafka/clients/admin/ElectLeadersOptions;";

    private final Severity severity;

    public AdminElectLeadersNoOptionsRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.ADMIN_ELECT_LEADERS_NO_OPTIONS;
    }

    @Override
    public List<Violation> check(RuleContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (MethodNode mn : ctx.classNode().methods) {
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
                    if (h != null && h.getDesc() != null
                            && !h.getDesc().contains(OPTIONS_TYPE_TOKEN)) {
                        out.add(violation(ctx, mn, insn));
                    }
                }
            }
        }
        return out;
    }

    private Violation violation(RuleContext ctx, MethodNode mn, AbstractInsnNode insn) {
        return new Violation(
                RuleId.ADMIN_ELECT_LEADERS_NO_OPTIONS, severity,
                ctx.classNode().name, mn.name, AsmUtil.lineOf(insn),
                "Carry the rule's didactic prose VERBATIM into the message — taglines, mechanism, impact, why-it-matters condensed into a long single string. The on-call engineer who hits this in a Maven log should not need to open the doc to understand why it fired.");
    }
}
```

### What the helpers do

- `KafkaTypes.ADMIN_OWNERS` — the `Set<String>` of internal names that count as `Admin` (covers both `org/apache/kafka/clients/admin/Admin` and `org/apache/kafka/clients/admin/AdminClient`). Reuse the existing per-surface sets from `KafkaTypes.java` rather than defining new ones inline.
- `AsmUtil.indyTargetHandle(indy, owners, name, desc)` — peels back the `LambdaMetafactory` bsm-args to the implementing-method `Handle` and matches on owner + name. Pass `null` for `desc` when the rule's predicate compares against the descriptor itself (as above); pass a concrete descriptor when matching a single overload by full signature.
- `AsmUtil.lineOf(insn)` — walks back from the instruction to the nearest `LineNumberNode` and returns the source line, or `0` if unknown.
- `RuleContext.isInLoopOrIteratingLambda(method, insn)` — the unified back-edge + iterating-lambda predicate; use it when the rule cares about being "inside a loop" (e.g. `PRODUCER_IN_LOOP`).

### Class-name conventions

- `rules/admin/AdminXRule.java` for `Admin` / `AdminClient` rules.
- `rules/clients/ProducerXRule.java` and `rules/clients/ConsumerXRule.java`.
- `rules/streams/StreamsXRule.java`.
- `rules/spring/SpringXRule.java`.
- `rules/connect/ConnectXRule.java`.
- One rule per file. The file name matches the class name. The class name matches the `RuleId` in CamelCase plus `Rule`.

### Property-file rules

No Java class needed. Call `PropertyFileRule.predicate(...)` or `PropertyFileRule.literal(...)` directly inside `KafkaLinterMojo.buildRules(...)` — see existing wirings for `CONSUMER_AUTO_COMMIT_TRUE` and `KAFKA_BOOTSTRAP_SERVERS_LOCALHOST`.

---

## Step 5 — Wire it into the Mojo

In `KafkaLinterMojo.java`, find the previous `addIfEnabled(... s -> new MethodCallRule(...))` wiring and replace with:

```java
addIfEnabled(rules, sev, RuleId.ADMIN_ELECT_LEADERS_NO_OPTIONS, AdminElectLeadersNoOptionsRule::new);
```

Add the import at the top of the file. Keep the alphabetical-within-category section ordering.

---

## Step 6 — Author the BAD IT fixture

```
src/it/<rule-slug>-bad/
├── pom.xml
├── invoker.properties
└── src/main/java/sample/Bad<RuleClass>.java
```

### `pom.xml`

The rule under test is `ERROR`. **Every companion rule that would otherwise fire on the same fixture must be `OFF`**, otherwise the fire-count one-liner is contaminated.

```xml
<configuration>
    <severities>
        <ADMIN_ELECT_LEADERS_NO_OPTIONS>ERROR</ADMIN_ELECT_LEADERS_NO_OPTIONS>
        <ADMIN_CLOSE_NO_TIMEOUT>OFF</ADMIN_CLOSE_NO_TIMEOUT>
        <KAFKA_CLIENTS_CVE_CONFIG_PROVIDER>OFF</KAFKA_CLIENTS_CVE_CONFIG_PROVIDER>
        <KAFKA_CLIENTS_CVE_BUFFER_POOL>OFF</KAFKA_CLIENTS_CVE_BUFFER_POOL>
        <KAFKA_CLIENTS_CVE_SCRAM_REPLAY>OFF</KAFKA_CLIENTS_CVE_SCRAM_REPLAY>
    </severities>
</configuration>
```

### `invoker.properties`

```properties
invoker.goals = verify
invoker.buildResult = failure
```

### `Bad<RuleClass>.java` — the canonical 8-fire structure

One method per fire shape. **The doc-comment above the class must say "RULE: <RULE_ID> — must fire EXACTLY 8 times across this class (one per method below)."** That sentence is the load-bearing assertion the verification one-liner enforces.

| # | Method        | Shape                                                                                      |
|---|---------------|--------------------------------------------------------------------------------------------|
| 1 | `xA`          | Direct INVOKE\* on the unsafe overload, vanilla.                                           |
| 2 | `xB`          | Direct INVOKE\* with a meaningfully different argument value (e.g. PREFERRED vs UNCLEAN).  |
| 3 | `xC`          | Direct INVOKE\* with an intermediate local-variable binding.                               |
| 4 | `xD`          | Direct INVOKE\* inside a `static` helper.                                                  |
| 5 | `biFactory`   | `admin::method` bound to a `Function` / `BiFunction` whose erased descriptor matches.      |
| 6 | `customFactory` | Same method reference bound to a custom `@FunctionalInterface` SAM with the same erased descriptor. Exercises a second `INVOKEDYNAMIC` bsm-shape. |
| 7 | `useLocalFactory` | Local SAM binding via bound-receiver method reference, applied inline.                 |
| 8 | `xAll`        | `.map(a -> a.method(...))` over a stream — the synthetic `lambda$xAll$0` carries a direct INVOKE\*. |

If your target method takes more than 2 args, shapes 5 and 6 use a custom 3-arg (or N-arg) SAM, since Java has no built-in `TriFunction`. Define one inline:

```java
@FunctionalInterface
interface ElectLeadersFactory {
    ElectLeadersResult apply(ElectionType type, Set<TopicPartition> partitions);
}
```

Worked example: [`src/it/admin-elect-leaders-no-options-bad/src/main/java/sample/BadAdminElectLeadersNoOptions.java`](src/it/admin-elect-leaders-no-options-bad/src/main/java/sample/BadAdminElectLeadersNoOptions.java).

---

## Step 7 — Author the GOOD IT fixture

Mirror the BAD layout, with `invoker.buildResult = success` and code paths exercising the **safe** API (the Options-bearing overload).

```
src/it/<rule-slug>-good/
├── pom.xml                  # same ERROR severity — the GOOD fixture proves zero fires
├── invoker.properties       # invoker.buildResult = success
└── src/main/java/sample/Good<RuleClass>.java
```

The GOOD fixture mirrors all 8 BAD methods one-for-one with the Options-bearing overload:

```java
private static ElectLeadersOptions opts() {
    return new ElectLeadersOptions().timeoutMs(120_000);
}

public ElectLeadersResult electA(Admin admin) {
    return admin.electLeaders(ElectionType.PREFERRED, partitions(), opts());
}
```

If the safe overload has a different arity than the unsafe one, the SAM definitions in shapes 5 and 6 expand accordingly (e.g. a 3-arg `OptionsElectLeadersFactory`).

Worked example: [`src/it/admin-elect-leaders-no-options-good/src/main/java/sample/GoodAdminElectLeadersNoOptions.java`](src/it/admin-elect-leaders-no-options-good/src/main/java/sample/GoodAdminElectLeadersNoOptions.java).

---

## Step 8 — Install and run targeted IT

```bash
mvn -q clean install -DskipTests
mvn -q invoker:install invoker:integration-test invoker:verify \
    -Dinvoker.test='<rule-slug>-*'
```

Both ITs must pass — BAD fails its build as declared, GOOD passes its build as declared, both satisfy invoker's expected-result check.

Then verify the BAD fired **exactly 8 times across 8 distinct methods**:

```bash
grep '<RULE_ID> sample\.' target/it/<rule-slug>-bad/build.log \
    | grep -oE 'sample\.[a-zA-Z0-9$]+#[a-zA-Z0-9$]+' \
    | sort | uniq -c
```

Expected output — eight lines, count `1` each:

```
      1 sample.BadAdminElectLeadersNoOptions#biFactory
      1 sample.BadAdminElectLeadersNoOptions#customFactory
      1 sample.BadAdminElectLeadersNoOptions#electA
      1 sample.BadAdminElectLeadersNoOptions#electB
      1 sample.BadAdminElectLeadersNoOptions#electC
      1 sample.BadAdminElectLeadersNoOptions#electD
      1 sample.BadAdminElectLeadersNoOptions#lambda$electAll$0
      1 sample.BadAdminElectLeadersNoOptions#useLocalFactory
```

Common failure modes and what they mean:

- **Fewer than 8 distinct methods.** The dual walk has a gap — usually the `InvokeDynamicInsnNode` branch isn't matching the bsm-shape used by `customFactory` or `useLocalFactory`. Inspect the bytecode with `javap -p -c target/classes/.../BadXXX.class` and check what method handle the indy bsm-args contain.
- **A method fires more than once.** You're matching both the indy site AND a synthetic INVOKE\* the compiler emitted inside the same method. The fixture method needs splitting, or the rule's branches need a `continue` between them.
- **The `lambda$xAll$0` line is missing.** The synthetic lambda is in a separate method; check that the rule iterates `ctx.classNode().methods` (plural), not just the user-visible methods.

---

## Step 9 — Run the full IT suite

```bash
mvn -q invoker:install invoker:integration-test invoker:verify
```

Expect **all** ITs to pass (the count grows by 2 per cycle — one BAD, one GOOD).

Inspect the report directory if any IT fails:

```bash
grep -L 'result="success"' target/invoker-reports/BUILD-*.xml
```

---

## Step 10 — Commit

Conventional Commits, with the Co-Authored-By trailer. One rule = one commit.

```
feat(<category>): dedicated <RULE_ID> rule with indy capture

Replace generic MethodCallRule wiring with <RuleClass> catching
the <signature> overload via dual MethodInsnNode +
InvokeDynamicInsnNode walk. Carries verbatim, didactic
failure-mode prose into the violation message
(<failure-mode-1>, <failure-mode-2>, …, indy bypass).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
```

`<category>` is one of: `admin`, `clients`, `streams`, `spring`, `connect`, `quarkus`, `security`, `observability`, `version`. For a doc-only PR use `docs(scope)`, for a test-only PR use `test(scope)`.

Then push and you're done.

---

## Anti-patterns to avoid

- **Don't omit the `InvokeDynamicInsnNode` branch.** A method-reference capture like `admin::electLeaders` will silently bypass the rule. Every dedicated method-call rule **must** walk both `MethodInsnNode` and `InvokeDynamicInsnNode`.
- **Don't make the BAD fixture share a method with another rule's BAD.** Per-rule IT isolation means per-rule fixture files. Cross-rule contamination breaks the fire-count assertion.
- **Don't turn other rules on in the IT pom.** The fire-count grep matches by rule ID, but mixed severities in the IT pom inflate the build log and slow review. The companion-OFF stanza is non-negotiable.
- **Don't write violation messages that point readers to "see the doc". Inline the prose.** The Maven log is the only thing some operators will read. The four-paragraph didactic block belongs in the message — verbatim, no abbreviation.
- **Don't add cross-method analysis without a strong reason.** Method-local heuristics are the project's editorial line. The fewer non-local walks, the fewer false positives.
- **Don't introduce new rules without a registered `RuleId`.** Register first, implement second. The catalog is the truth-pointer.
