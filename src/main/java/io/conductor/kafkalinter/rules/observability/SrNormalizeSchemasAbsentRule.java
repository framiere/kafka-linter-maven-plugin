package io.conductor.kafkalinter.rules.observability;

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

/**
 * Project-scoped rule. Fires when a properties file talks to Schema Registry
 * (via {@code schema.registry.url} or its Spring Boot prefixed equivalent
 * {@code spring.kafka.properties.schema.registry.url}) but does NOT set
 * {@code normalize.schemas} (plain or Spring-prefixed).
 *
 * <p>{@code normalize.schemas} (KIP-855, Confluent SR client 6.0, October 2020)
 * is the canonicalization gate: when {@code true}, the client canonicalizes
 * schema TEXT (alphabetize fields, normalize unions, strip comments, resolve
 * namespace aliases) before hashing for registry-identity check. When unset or
 * {@code false} (the framework default), every formatter-driven reorder,
 * codegen-toolchain difference, and emit-order quirk creates a near-duplicate
 * schema version in SR.
 *
 * <p>Connect converter properties are out of scope (covered by the Connect
 * converter rules); this rule applies only to producer/consumer/Streams shapes
 * (no {@code connector.class}).
 *
 * <p>Emits one INFO violation per offending file.
 */
public final class SrNormalizeSchemasAbsentRule implements ProjectScopedRule {

    private static final String SR_URL_PLAIN = "schema.registry.url";
    private static final String SR_URL_SPRING = "spring.kafka.properties.schema.registry.url";
    private static final String NORMALIZE_PLAIN = "normalize.schemas";
    private static final String NORMALIZE_SPRING = "spring.kafka.properties.normalize.schemas";
    private static final String CONNECTOR_CLASS = "connector.class";

    private final Severity severity;

    public SrNormalizeSchemasAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SR_NORMALIZE_SCHEMAS_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Path file = e.getKey();
            Properties p = e.getValue();

            // Skip Connect worker/connector configs — converter properties are out of scope.
            if (trimOrNull(p.getProperty(CONNECTOR_CLASS)) != null) continue;

            String plainUrl = trimOrNull(p.getProperty(SR_URL_PLAIN));
            String springUrl = trimOrNull(p.getProperty(SR_URL_SPRING));
            if (plainUrl == null && springUrl == null) continue;

            String normalizePlain = trimOrNull(p.getProperty(NORMALIZE_PLAIN));
            String normalizeSpring = trimOrNull(p.getProperty(NORMALIZE_SPRING));
            if (normalizePlain != null || normalizeSpring != null) continue;

            String urlKey = plainUrl != null ? SR_URL_PLAIN : SR_URL_SPRING;

            out.add(new Violation(
                    RuleId.SR_NORMALIZE_SCHEMAS_ABSENT, severity,
                    ctx.relativize(file), "key:" + urlKey, 0,
                    "Properties file talks to Schema Registry (`" + urlKey + "` set) but "
                            + "`normalize.schemas` is NOT set (neither the plain key nor the "
                            + "Spring Boot prefixed `spring.kafka.properties.normalize.schemas`). "
                            + "Per KIP-855 (Confluent SR client 6.0, October 2020), "
                            + "`normalize.schemas` is the CANONICALIZATION GATE for schema TEXT "
                            + "before the client hashes it to compute the registry-identity "
                            + "lookup. When `false` (the framework default for backward "
                            + "compatibility with pre-6.0 registries that contain non-normalized "
                            + "historical schemas), the client uses the schema TEXT EXACTLY as "
                            + "emitted by codegen / hand-written .avsc / .proto files: field "
                            + "ORDER matters, whitespace matters, comments matter, union member "
                            + "ORDER matters, and unresolved namespace aliases matter. When "
                            + "`true`, the client first normalizes the schema text (alphabetizes "
                            + "fields, normalizes unions, strips comments, resolves aliases) "
                            + "before hashing — semantically-equivalent schemas hash to the "
                            + "SAME registry ID. With the default `false` for this app, common "
                            + "developer-flow operations create near-duplicate schema versions: "
                            + "(1) IDE code-formatter triggered subject-version explosion — a "
                            + "developer runs `mvn spotless:apply` or IntelliJ Reformat on the "
                            + "Avro codegen output (or a hand-written .avsc), the formatter "
                            + "reorders fields alphabetically, the rebuilt producer JAR emits "
                            + "the SAME schema with DIFFERENT byte-layout, the producer's first "
                            + "send registers a NEW subject-version with the same fields in "
                            + "different order; multiplied across producers and re-formats, "
                            + "subjects accumulate dozens of byte-different but semantically-"
                            + "identical versions; (2) multi-toolchain Avro codegen — Maven's "
                            + "avro-maven-plugin emits fields in declaration order, Gradle's "
                            + "avro-gradle-plugin sorts them, sbt-avro normalizes namespaces "
                            + "differently; an org with mixed Maven+Gradle producers pointing at "
                            + "the same SR cluster gets every subject duplicated under each "
                            + "toolchain's emit-order; (3) cross-team registry pollution — Team "
                            + "A registers `com.acme.Order` from a hand-written .avsc, Team B "
                            + "regenerates the same logical schema from a OpenAPI-to-Avro "
                            + "converter that emits in a different order — SR stores both as "
                            + "distinct subject-versions, both consumers cache different schema-"
                            + "IDs, the registry-CPU spikes during compatibility-checks because "
                            + "every check compares byte-by-byte against every prior version; "
                            + "(4) compatibility-check false-positives — `latestCompatibility="
                            + "BACKWARD` rejects a schema that ADDS A FIELD because the byte-"
                            + "level diff also touches existing-field-order; the developer "
                            + "responds by setting compatibility to NONE (the worst-of-all "
                            + "fixes), losing real compatibility protection; (5) consumer schema-"
                            + "cache thrashing — the default `KafkaAvroDeserializer` schema "
                            + "cache size is 1000 (CACHED_SCHEMA_MAP_SIZE_CONFIG); under "
                            + "normalize.schemas=false, every byte-different copy occupies a "
                            + "slot, the cache hits its cap, oldest entries evict and re-fetch "
                            + "from SR on the next record carrying the evicted ID, network-"
                            + "round-trip per consumer-thread per evicted ID = consumer-latency "
                            + "spikes; (6) Spring Boot autoconfig default-leak — Spring "
                            + "`KafkaAutoConfiguration` does NOT set `normalize.schemas` in its "
                            + "default template; every Spring Boot Kafka app inherits the "
                            + "framework's `false` default and silently accumulates SR pollution "
                            + "even when the operator never explicitly chose `false`. INFO is "
                            + "the right level (not WARNING) because flipping to `true` is a "
                            + "one-way migration for legacy registries containing pre-6.0 non-"
                            + "normalized schemas: re-registering existing schemas under the "
                            + "normalized form creates NEW schema IDs (the historical IDs "
                            + "remain, but every producer/consumer cache-miss on an old ID re-"
                            + "fetches and may re-cache under the normalized form). For green-"
                            + "field deployments (every new Kafka cluster after October 2020 — "
                            + "THE COMMON CASE), `normalize.schemas=true` is the correct choice "
                            + "and should have been the default; for migrated orgs the "
                            + "deliberate choice is `=false` with documentation explaining why. "
                            + "Fix: add `" + NORMALIZE_PLAIN + "=true` (or `" + NORMALIZE_SPRING
                            + "=true` for Spring Boot apps) to this properties file. If you "
                            + "are operating against a legacy registry that requires `=false`, "
                            + "set the key explicitly to `false` with a comment documenting the "
                            + "legacy-registry constraint — that disables this rule's trigger "
                            + "(absence) and records the deliberate choice. Sibling rules: "
                            + "SR_AUTO_REGISTER_SCHEMAS_TRUE (producer-side governance gate — "
                            + "orthogonal but related), SR_USE_LATEST_VERSION_MISSING (the "
                            + "schema-pinning sibling — also a half-migrated state), "
                            + "SR_LATEST_COMPATIBILITY_STRICT_FALSE (the compatibility-check "
                            + "engine side of the same canonicalization concern), CONNECT_AVRO_"
                            + "AUTO_REGISTER_SCHEMAS_TRUE (the Connect-converter equivalent of "
                            + "the auto-register gate)."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
