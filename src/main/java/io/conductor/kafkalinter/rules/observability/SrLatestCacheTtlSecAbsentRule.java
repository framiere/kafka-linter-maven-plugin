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
 * Project-scoped rule. Fires when a properties file is in the
 * fully-hardened Category-A producer combination
 * ({@code auto.register.schemas=false} + {@code use.latest.version=true})
 * but does NOT set {@code latest.cache.ttl.sec} (or sets it to {@code -1}).
 *
 * <p>Per KIP-906 (Confluent SR client 7.0, July 2022),
 * {@code latest.cache.ttl.sec} defaults to {@code -1} (no expiry). With the
 * default, the producer's latest-version cache freezes at first send and never
 * refreshes — new schema versions registered post-startup are NEVER picked up.
 *
 * <p>The rule does NOT fire when {@code use.latest.version} is unset/false
 * (covered by {@link SrUseLatestVersionMissingRule}) or when
 * {@code auto.register.schemas} is true/unset (covered by the auto-register
 * rule). Connect converters are out of scope (different cache architecture).
 *
 * <p>Emits one INFO violation per offending file.
 */
public final class SrLatestCacheTtlSecAbsentRule implements ProjectScopedRule {

    private static final String AUTO_REGISTER_SCHEMAS = "auto.register.schemas";
    private static final String USE_LATEST_VERSION = "use.latest.version";
    private static final String LATEST_CACHE_TTL_PLAIN = "latest.cache.ttl.sec";
    private static final String LATEST_CACHE_TTL_SPRING = "spring.kafka.properties.latest.cache.ttl.sec";

    private final Severity severity;

    public SrLatestCacheTtlSecAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SR_LATEST_CACHE_TTL_SEC_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Path file = e.getKey();
            Properties p = e.getValue();

            String autoRegister = trimOrNull(p.getProperty(AUTO_REGISTER_SCHEMAS));
            if (autoRegister == null || !"false".equalsIgnoreCase(autoRegister)) continue;

            String useLatest = trimOrNull(p.getProperty(USE_LATEST_VERSION));
            if (useLatest == null || !"true".equalsIgnoreCase(useLatest)) continue;

            String ttlPlain = trimOrNull(p.getProperty(LATEST_CACHE_TTL_PLAIN));
            String ttlSpring = trimOrNull(p.getProperty(LATEST_CACHE_TTL_SPRING));

            // Fire if both unset, OR if plain is explicitly -1 (the no-expiry default).
            boolean bothUnset = ttlPlain == null && ttlSpring == null;
            boolean plainIsMinusOne = ttlPlain != null && "-1".equals(ttlPlain);
            if (!bothUnset && !plainIsMinusOne) continue;
            // If Spring is set to a positive value, don't fire (the operator made a choice).
            if (ttlSpring != null && !"-1".equals(ttlSpring)) continue;

            String currentState = bothUnset
                    ? "latest.cache.ttl.sec is UNSET (defaults to -1, meaning no expiry)"
                    : "latest.cache.ttl.sec=-1 (no expiry)";

            out.add(new Violation(
                    RuleId.SR_LATEST_CACHE_TTL_SEC_ABSENT, severity,
                    ctx.relativize(file), "key:" + USE_LATEST_VERSION, 0,
                    "Producer config has the fully-hardened Category-A combination set "
                            + "(`" + AUTO_REGISTER_SCHEMAS + "=false` + `" + USE_LATEST_VERSION
                            + "=true`) BUT " + currentState + ". With this knob unset or at -1, "
                            + "Confluent's `AbstractKafkaAvroSerDeConfig` keeps the latest-"
                            + "version cache PERMANENT for the JVM lifetime: the producer "
                            + "fetches the latest registered schema version ONCE at first send, "
                            + "caches the schema-ID, and writes every subsequent record with "
                            + "that initially-cached ID — even if new compatible schema versions "
                            + "are registered post-startup via Terraform / CI/CD / a peer "
                            + "producer. Per KIP-906 (Confluent SR client 7.0, July 2022), the "
                            + "knob defaults to -1 for backward compatibility with pre-7.0 "
                            + "clients that had no cache-TTL support, but for the Category-A "
                            + "producer combination this default renders `use.latest.version="
                            + "true` effectively inert past the first send. The bug shape is "
                            + "delayed and disaster-triggered: producers run fine, records "
                            + "flow, consumers decode successfully — but new fields registered "
                            + "in SR are silently absent from records produced by long-running "
                            + "JVMs. Common shapes: (1) Terraform-managed schema registration "
                            + "— Terraform registers version N+1 on Monday; producers using "
                            + "this config keep emitting records with version N's schema-ID "
                            + "for weeks until someone redeploys them for an unrelated reason; "
                            + "(2) Helm-chart Category-A defaults — internal chart templates "
                            + "the Category-A combination but omits latest.cache.ttl.sec; "
                            + "every Spring Boot producer inherits the permanent cache; "
                            + "(3) long-lived JVMs — Streams apps or ingestion daemons running "
                            + "30+ days between deploys silently ignore every schema-"
                            + "registration in that window; (4) multi-team schema evolution — "
                            + "Team A registers Monday; Team B's producers (Category-A but no "
                            + "TTL) keep emitting Monday's last-week-schema-ID for weeks; "
                            + "Team C's freshly-deployed producer immediately picks up the new "
                            + "schema; consumers see a mix; (5) Confluent docs ambiguity — the "
                            + "security-hardening guide enables `use.latest.version=true` but "
                            + "mentions `latest.cache.ttl.sec` only in the Advanced "
                            + "Configuration section that developers skip; (6) Spring Boot "
                            + "autoconfig — `KafkaAutoConfiguration` does NOT set this knob; "
                            + "every Spring Boot Kafka producer using Confluent SR inherits "
                            + "-1 by default. INFO is the right level (not WARNING) because "
                            + "some narrow legitimate cases want pinned-version behavior (a "
                            + "producer that must follow strict change-management discipline "
                            + "and only pick up new versions on deploy). The rule prompts the "
                            + "operator to make the choice explicit — either set a positive "
                            + "TTL (refresh-on-cadence) or set -1 with documentation "
                            + "(deliberate pinning). Fix: add `" + LATEST_CACHE_TTL_PLAIN
                            + "=300` (5 minutes — Confluent's documented recommendation) or a "
                            + "value matching your org's schema-evolution cadence (3600 = 1 "
                            + "hour for weekly evolution; 60 = 1 minute for high-velocity "
                            + "schema-changes). For Spring Boot: `"
                            + LATEST_CACHE_TTL_SPRING + "=300`. For deliberate pinning: set "
                            + "the key explicitly to -1 with a comment documenting the "
                            + "change-management rationale. Sibling rules: SR_USE_LATEST_"
                            + "VERSION_MISSING (the second-knob-missing half-migration, "
                            + "orthogonal to this rule's third-knob gap), SR_AUTO_REGISTER_"
                            + "SCHEMAS_TRUE (the Category-C-not-yet-migrated state), SR_"
                            + "LATEST_COMPATIBILITY_STRICT_FALSE (interacts with this knob — "
                            + "stale cached-latest means stale compatibility checks), SR_"
                            + "NORMALIZE_SCHEMAS_ABSENT (orthogonal canonicalization concern, "
                            + "but both surface as 'default-is-wrong-for-modern-"
                            + "deployments')."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
