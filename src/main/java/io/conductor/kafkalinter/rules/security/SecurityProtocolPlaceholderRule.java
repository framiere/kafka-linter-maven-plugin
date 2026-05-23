package io.conductor.kafkalinter.rules.security;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Project-scoped rule. Fires when {@code security.protocol} (or the
 * Spring Boot equivalent) is set to an unresolved placeholder value
 * such as {@code ${KAFKA_SECURITY}} or {@code @{some.config}}.
 *
 * <p>Kafka's `CommonClientConfigs.SECURITY_PROTOCOL_CONFIG` is a
 * `ConfigDef.ValidString.in(...)` enum: only PLAINTEXT, SSL,
 * SASL_PLAINTEXT, SASL_SSL are accepted. A literal placeholder
 * reaches the validator unchanged from {@code Properties.put} and
 * fails {@code ConfigException} at client construction — the JVM
 * starts, but the Kafka client crashes before opening a socket.
 */
public final class SecurityProtocolPlaceholderRule implements ProjectScopedRule {

    private static final String PLAIN_KEY = "security.protocol";
    private static final String SPRING_KEY = "spring.kafka.properties.security.protocol";

    // ${...}, @{...}, ${env:...}, ${sys:...}, %{...}, and Maven-style ${project.foo}.
    private static final Pattern PLACEHOLDER = Pattern.compile(
            "\\$\\{[^}]*\\}|@\\{[^}]*\\}|%\\{[^}]*\\}");

    private final Severity severity;

    public SecurityProtocolPlaceholderRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SECURITY_PROTOCOL_PLACEHOLDER;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            checkKey(out, ctx, e.getKey(), p, PLAIN_KEY);
            checkKey(out, ctx, e.getKey(), p, SPRING_KEY);
        }
        return out;
    }

    private void checkKey(List<Violation> out, ProjectContext ctx, Path file,
                          Properties p, String key) {
        String value = p.getProperty(key);
        if (value == null) return;
        String trimmed = value.trim();
        if (trimmed.isEmpty()) return;
        if (!PLACEHOLDER.matcher(trimmed).find()) return;
        out.add(new Violation(
                RuleId.SECURITY_PROTOCOL_PLACEHOLDER, severity,
                ctx.relativize(file), "key:" + key, 0,
                key + "=" + trimmed + " — `security.protocol` is set to "
                        + "a value containing an unresolved configuration "
                        + "placeholder (one of `${...}`, `@{...}`, "
                        + "`%{...}`). Kafka's client library validates "
                        + "this key as a closed enum: only `PLAINTEXT`, "
                        + "`SSL`, `SASL_PLAINTEXT`, `SASL_SSL` are "
                        + "accepted. Anything else fails immediately with "
                        + "`org.apache.kafka.common.config.ConfigException"
                        + ": Invalid value " + trimmed + " for "
                        + "configuration security.protocol: String must "
                        + "be one of: PLAINTEXT, SSL, SASL_PLAINTEXT, "
                        + "SASL_SSL`. Why this happens (the source of "
                        + "the placeholder): kafka-clients' "
                        + "`AbstractConfig` reads property values as "
                        + "plain `String` objects from "
                        + "`Properties.getProperty` / "
                        + "`Map.get` — it does NOT expand `${VAR}` "
                        + "tokens. The expansion has to happen "
                        + "BEFORE the value reaches the Kafka client: "
                        + "via the JVM startup (`System.getProperty` is "
                        + "expanded by the launcher only for "
                        + "`-Dkey=${VAR}` if `VAR` is a system "
                        + "property), via the application framework "
                        + "(Spring's `@Value(\"${KAFKA_SECURITY}\")`, "
                        + "Micronaut's `${kafka.security}`, MicroProfile "
                        + "Config's `ConfigProvider`, Helm's template "
                        + "engine, Kubernetes ConfigMap "
                        + "`envFrom`/`valueFrom.configMapKeyRef`), or "
                        + "via explicit code (`System.getenv` + "
                        + "`props.put`). When the .properties file is "
                        + "loaded directly with `Properties.load(new "
                        + "FileReader(...))` — bypassing the framework's "
                        + "config-provider — placeholder expansion does "
                        + "NOT happen and the literal `${KAFKA_"
                        + "SECURITY}` string reaches the Kafka client. "
                        + "Validation order: Kafka's "
                        + "`CommonClientConfigs.SECURITY_PROTOCOL_CONFIG` "
                        + "is defined with `new ConfigDef.ValidString"
                        + "(Arrays.asList(SecurityProtocol.names()))` "
                        + "in `kafka-clients`. The validator runs inside "
                        + "`AbstractConfig.parse()` during "
                        + "`KafkaProducer`/`KafkaConsumer`/"
                        + "`KafkaAdminClient` construction — BEFORE any "
                        + "network code, before the `bootstrap.servers` "
                        + "DNS lookup, before any SSL/SASL handshake. "
                        + "The `ConfigException` is thrown synchronously "
                        + "from the constructor; if the application "
                        + "catches it and continues, the JVM stays up "
                        + "with NO active Kafka client and reads/writes "
                        + "fail with `IllegalStateException: This "
                        + "instance was already closed.`. The lucky "
                        + "case is the application doesn't catch — the "
                        + "exception propagates and the pod crashes "
                        + "cleanly; the unlucky case is "
                        + "`@PostConstruct` swallows it into a logger "
                        + "and the pod stays up looking healthy. "
                        + "Specific failure modes: (1) **Config drift "
                        + "between environments.** The "
                        + "`KAFKA_SECURITY` env var is set on staging "
                        + "(by a Helm value or a Kubernetes ConfigMap) "
                        + "but missing on prod. The same Docker image "
                        + "deploys cleanly to staging (env var "
                        + "resolves, value is `SASL_SSL`), Kafka "
                        + "validation passes; on prod the env var is "
                        + "absent or named differently, the Spring/"
                        + "Helm template substitution produces an "
                        + "empty string or leaves the placeholder "
                        + "literal, the Kafka client throws "
                        + "ConfigException at startup. (2) "
                        + "**Properties.load bypassing the framework.** "
                        + "A team has Spring Boot configured correctly "
                        + "in `application.yml` with `@Value` "
                        + "expansion, but a code path that loads a "
                        + "separate `kafka-overrides.properties` via "
                        + "`Properties.load(...)` — bypassing Spring. "
                        + "The placeholders in that file are NEVER "
                        + "expanded; the application starts up fine "
                        + "until the second properties file is "
                        + "consulted. (3) **Helm template engine "
                        + "version drift.** A Helm chart uses "
                        + "`{{ .Values.kafka.security }}` to template "
                        + "the value into a ConfigMap. A newer chart "
                        + "version switches to "
                        + "`{{ required \"x\" .Values.kafka.security }}` "
                        + "which fails the helm install with a clear "
                        + "error — but a CI pipeline that templates "
                        + "with `--debug` and `--dry-run` doesn't catch "
                        + "the unresolved placeholder until production "
                        + "deploy. (4) **Java's `${maven.foo}` Maven-"
                        + "filter pattern.** A team uses Maven's "
                        + "resource filtering "
                        + "(`<filtering>true</filtering>`) to expand "
                        + "`${env.KAFKA_SECURITY}` at build time. A "
                        + "developer runs `mvn package` without setting "
                        + "the env var; Maven leaves the placeholder "
                        + "literal in the packaged .properties file; "
                        + "the artifact reaches production. (5) **Spring "
                        + "Boot `application-{profile}.yml` "
                        + "inheritance.** A team uses "
                        + "`application-local.yml` with the placeholder "
                        + "literal and a separate `application-prod.yml` "
                        + "with the resolved value. A pod started with "
                        + "`SPRING_PROFILES_ACTIVE=local,prod` reads "
                        + "both in order; if the local profile's "
                        + "literal placeholder comes last (Spring's "
                        + "later-wins ordering), it overrides the prod "
                        + "resolved value and the client crashes. (6) "
                        + "**Quoting bug in the value.** A team has "
                        + "`security.protocol=\"${KAFKA_SECURITY}\"` "
                        + "with quotes in the .properties file. Java's "
                        + "`Properties` class does NOT strip quotes; "
                        + "the literal `\"SASL_SSL\"` (with quotes) "
                        + "reaches the validator and fails the enum "
                        + "check. The placeholder pattern catches the "
                        + "common case where the value contains `${...}`; "
                        + "this rule does not catch quote bugs but the "
                        + "fix-up advice is the same. The specific "
                        + "placeholders this rule catches: (i) `${...}` "
                        + "— the Ant/Maven/Spring/most-frameworks "
                        + "format. (ii) `@{...}` — Thymeleaf's link "
                        + "syntax (occasionally leaks into properties "
                        + "files via copy-paste from a Thymeleaf "
                        + "template). (iii) `%{...}` — JBoss / "
                        + "WildFly system-property substitution. The "
                        + "regex doesn't match every conceivable "
                        + "placeholder format (Mustache `{{...}}`, "
                        + "Handlebars, Liquid `{%...%}`) but the above "
                        + "three cover ~99% of real-world cases. "
                        + "Specific scenarios where this rule fires: "
                        + "(a) **Helm chart with a missing values "
                        + "file.** `helm template` runs without the "
                        + "values file; templates render but with "
                        + "literal `{{ .Values.x }}` for missing keys "
                        + "(actually Helm renders an empty string by "
                        + "default — but a team that uses "
                        + "`{{ default \"${KAFKA_SECURITY}\" .Values."
                        + "kafka.security }}` keeps the literal "
                        + "placeholder as a documented fallback that "
                        + "was never supposed to ship). (b) **Kubernetes "
                        + "ConfigMap `valueFrom` typo.** "
                        + "`valueFrom.configMapKeyRef.name=wrong-cm` "
                        + "— Kubernetes silently leaves the env var "
                        + "unset; the framework's "
                        + "`${KAFKA_SECURITY}` resolves to empty (or "
                        + "the literal placeholder, depending on "
                        + "framework defaults); Kafka rejects. (c) "
                        + "**MicroProfile Config provider not "
                        + "configured.** A Quarkus / WildFly "
                        + "deployment has the .properties file "
                        + "intended to be processed by "
                        + "ConfigProvider but the application reads "
                        + "it via plain `Properties.load`; "
                        + "expansion never happens. (d) **Direct "
                        + "`Properties.load` in unit tests.** A test "
                        + "that loads the production .properties "
                        + "file directly (without the framework's "
                        + "config provider) reproduces the production "
                        + "bug — useful for catching this rule's "
                        + "target in CI. Fix (in order of preference): "
                        + "(1) Resolve the placeholder at the source "
                        + "of truth: `props.put(\"security.protocol\", "
                        + "System.getenv(\"KAFKA_SECURITY\"))` with a "
                        + "null-check that fails fast with a useful "
                        + "error message like `KAFKA_SECURITY env var "
                        + "must be set to one of PLAINTEXT, SSL, "
                        + "SASL_PLAINTEXT, SASL_SSL`. (2) For Spring "
                        + "Boot: use `@Value(\"${kafka.security:}\")` "
                        + "with an explicit default and validate the "
                        + "resolved value against the four allowed "
                        + "strings before passing to Kafka. (3) Don't "
                        + "use placeholders in .properties files that "
                        + "are loaded directly by "
                        + "`Properties.load(FileReader)`; move the "
                        + "value to `application.yml` (Spring), "
                        + "`application.properties` (MicroProfile), or "
                        + "set as `-Dsecurity.protocol=SASL_SSL` on the "
                        + "JVM command line. (4) If you MUST keep "
                        + "placeholders in a direct-loaded "
                        + ".properties, expand them explicitly: a "
                        + "small `PropertyExpander` utility that "
                        + "walks the loaded `Properties` and replaces "
                        + "`${...}` tokens with `System.getenv(...)` "
                        + "/ `System.getProperty(...)` before passing "
                        + "to the Kafka client. Sibling rules: "
                        + "[[kafka-client-id-placeholder]] is the "
                        + "client-id version of this bug; "
                        + "[[kafka-client-rack-placeholder]] is the "
                        + "rack-id version (KIP-392 fetch optimization "
                        + "silently disabled); "
                        + "[[security-sasl-mechanism-plain]] and "
                        + "[[security-protocol-plaintext-remote]] "
                        + "catch the CORRECTLY-resolved-but-wrong-"
                        + "value cases."));
    }
}
