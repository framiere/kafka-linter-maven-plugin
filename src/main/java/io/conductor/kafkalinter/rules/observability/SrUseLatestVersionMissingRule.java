package io.conductor.kafkalinter.rules.observability;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * Project-scoped rule. Fires when a properties file sets
 * {@code auto.register.schemas=false} (the producer-hardening choice) but does
 * NOT set {@code use.latest.version=true} in the same file.
 *
 * <p>This is the half-migrated Category-C → Category-A producer state: the
 * governance gate is in place (the producer can no longer push new schemas to
 * SR) but the version-pinning gate is open (the producer caches the FIRST
 * schema-ID it sees and never refreshes), silently dropping new compatible
 * fields registered out-of-band.
 *
 * <p>Detection uses the EXACT unprefixed keys {@code auto.register.schemas}
 * and {@code use.latest.version}; the Kafka Connect converter-prefixed forms
 * ({@code key.converter.auto.register.schemas},
 * {@code value.converter.auto.register.schemas}) are out of scope and are
 * covered by {@code CONNECT_AVRO_AUTO_REGISTER_SCHEMAS_TRUE} instead.
 *
 * <p>Emits one violation per offending file.
 */
public final class SrUseLatestVersionMissingRule implements ProjectScopedRule {

    private static final String AUTO_REGISTER_SCHEMAS = "auto.register.schemas";
    private static final String USE_LATEST_VERSION = "use.latest.version";

    private final Severity severity;

    public SrUseLatestVersionMissingRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.SR_USE_LATEST_VERSION_MISSING;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String autoRegister = trimOrNull(p.getProperty(AUTO_REGISTER_SCHEMAS));
            if (autoRegister == null) continue;
            if (!"false".equalsIgnoreCase(autoRegister)) continue;

            String useLatest = trimOrNull(p.getProperty(USE_LATEST_VERSION));
            if (useLatest != null && "true".equalsIgnoreCase(useLatest)) continue;

            String useLatestDesc = useLatest == null
                    ? "use.latest.version is UNSET (defaults to false)"
                    : "use.latest.version=" + useLatest;

            out.add(new Violation(
                    RuleId.SR_USE_LATEST_VERSION_MISSING, severity,
                    ctx.relativize(e.getKey()), "key:" + AUTO_REGISTER_SCHEMAS, 0,
                    "Producer config sets `" + AUTO_REGISTER_SCHEMAS + "=false` (the correct "
                            + "hardening choice — producers should not push new schemas to "
                            + "Schema Registry on the fly) BUT " + useLatestDesc + ". This is "
                            + "the half-migrated Category-C → Category-A producer state from "
                            + "Confluent's compliance taxonomy: with auto-register OFF the "
                            + "producer can no longer POST new schemas to SR (the governance "
                            + "gate is in place), but with use.latest.version=false the "
                            + "producer also does NOT look up the LATEST registered schema — "
                            + "instead, on first send it asks SR for the ID matching the "
                            + "schema embedded in its compiled POJO/Avro/Proto class, caches "
                            + "that ID, and writes every subsequent record with it. The "
                            + "producer is now PINNED to that schema ID for its entire JVM "
                            + "lifetime: as new compatible schema versions are registered in "
                            + "SR (via Terraform, CI/CD, or a peer producer), the half-"
                            + "migrated producer keeps writing records with the OLD cached "
                            + "ID. Downstream consumers using the latest schema project the "
                            + "missing-newer-fields to defaults — silent semantic data loss. "
                            + "Worse, if anyone hard-deletes the cached schema version from "
                            + "SR (cleanup task, retention policy), the producer's next "
                            + "`send()` fails with `Schema not found for ID <N>` and the "
                            + "outage is typically traced back to this misconfiguration only "
                            + "after extensive debugging. Fix: add `use.latest.version=true` "
                            + "alongside the existing `auto.register.schemas=false` to "
                            + "complete the Category-A migration (the producer will then "
                            + "fetch the latest registered version from SR per `cache."
                            + "capacity.config` intervals, keeping records in lockstep with "
                            + "current SR state). Pair with `latest.compatibility.strict="
                            + "true` (the default — see related rule "
                            + "SR_LATEST_COMPATIBILITY_STRICT_FALSE) to ensure the runtime "
                            + "POJO can actually be projected into the latest schema. "
                            + "Sibling rules: SR_AUTO_REGISTER_SCHEMAS_TRUE (the not-yet-"
                            + "migrated Category-C state), SR_USE_LATEST_VERSION_TRUE (over-"
                            + "hardened without strict-mode), SR_LATEST_COMPATIBILITY_"
                            + "STRICT_FALSE (disabled compatibility gate)."));
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
