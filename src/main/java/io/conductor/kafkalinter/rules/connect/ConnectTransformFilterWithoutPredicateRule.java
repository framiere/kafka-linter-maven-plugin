package io.conductor.kafkalinter.rules.connect;

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
import java.util.Set;

/**
 * Project-scoped rule. Fires when a Kafka Connect connector .properties file
 * declares an SMT of type {@code org.apache.kafka.connect.transforms.Filter}
 * (or its {@code $Key} / {@code $Value} variants) WITHOUT a corresponding
 * {@code transforms.<name>.predicate} reference.
 *
 * <p>The Filter SMT returns {@code null} from {@code apply()}, which the
 * Connect framework interprets as "drop this record." Without a predicate,
 * the Filter unconditionally drops EVERY record passing through the
 * connector — silently, with no log, metric, or DLQ trail.
 *
 * <p>Emits one violation per offending SMT name per file.
 */
public final class ConnectTransformFilterWithoutPredicateRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String TRANSFORMS = "transforms";
    private static final Set<String> FILTER_SMT_CLASSES = Set.of(
            "org.apache.kafka.connect.transforms.Filter",
            "org.apache.kafka.connect.transforms.Filter$Key",
            "org.apache.kafka.connect.transforms.Filter$Value");

    private final Severity severity;

    public ConnectTransformFilterWithoutPredicateRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_TRANSFORM_FILTER_WITHOUT_PREDICATE;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (trimOrNull(p.getProperty(CONNECTOR_CLASS)) == null) continue;

            String transformsRaw = trimOrNull(p.getProperty(TRANSFORMS));
            if (transformsRaw == null) continue;

            for (String token : transformsRaw.split(",")) {
                String name = token.trim();
                if (name.isEmpty()) continue;

                String type = trimOrNull(p.getProperty("transforms." + name + ".type"));
                if (type == null) continue;
                if (!FILTER_SMT_CLASSES.contains(type)) continue;

                String predicate = trimOrNull(p.getProperty("transforms." + name + ".predicate"));
                if (predicate != null) continue;

                out.add(new Violation(
                        RuleId.CONNECT_TRANSFORM_FILTER_WITHOUT_PREDICATE, severity,
                        ctx.relativize(e.getKey()), "key:transforms." + name + ".type", 0,
                        "Kafka Connect SMT `transforms." + name + ".type=" + type
                                + "` is the Filter SMT but no predicate is attached "
                                + "(`transforms." + name + ".predicate` is absent or empty). "
                                + "The Filter SMT returns null from apply() — without a "
                                + "predicate, the Connect framework invokes apply() on EVERY "
                                + "record, dropping every record from the pipeline. The "
                                + "connector reports RUNNING and worker metrics look normal, "
                                + "but the downstream (Kafka topic for a source connector, "
                                + "external sink for a sink connector) receives ZERO records. "
                                + "The drop is silent — no log, no metric, no DLQ. Fix: "
                                + "attach a predicate via `transforms." + name + ".predicate="
                                + "<predicate-name>` (and declare the predicate in the "
                                + "top-level `predicates=` config) so Filter drops only the "
                                + "intended subset of records; or remove the Filter SMT "
                                + "entirely if the operator's intent was a different "
                                + "transform (ExtractField for sub-field extraction, "
                                + "ReplaceField for field-level projection, etc.). Sibling "
                                + "rules: CONNECT_ERRORS_TOLERANCE_ALL_NO_DLQ (silent-data-"
                                + "loss at the converter layer), CONNECT_DEBEZIUM_EVENT_"
                                + "PROCESSING_FAILURE_HANDLING_MODE_SKIP_OR_WARN (Debezium "
                                + "event-processing silent-drop), CONNECT_DEBEZIUM_SKIPPED_"
                                + "OPERATIONS_DROPS_DATA (Debezium operation-filter silent-"
                                + "drop)."));
            }
        }
        return out;
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        String t = v.trim();
        return t.isEmpty() ? null : t;
    }
}
