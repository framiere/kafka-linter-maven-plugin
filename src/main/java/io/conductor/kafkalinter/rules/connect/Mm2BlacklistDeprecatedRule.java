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
 * Project-scoped rule. Fires when a MirrorMaker 2 connector config
 * ({@code connector.class} is one of {@code MirrorSourceConnector},
 * {@code MirrorCheckpointConnector}, or {@code MirrorHeartbeatConnector})
 * carries the legacy {@code topics.blacklist} or {@code groups.blacklist}
 * keys. These were renamed to {@code topics.exclude}/{@code groups.exclude}
 * by KIP-629 (Kafka 2.8) and are scheduled for removal in Kafka 4.0.
 *
 * <p>Emits one violation per legacy key found per file.
 */
public final class Mm2BlacklistDeprecatedRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";

    private static final Set<String> MM2_CLASSES = Set.of(
            "org.apache.kafka.connect.mirror.MirrorSourceConnector",
            "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
            "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector"
    );

    private static final String TOPICS_BLACKLIST = "topics.blacklist";
    private static final String GROUPS_BLACKLIST = "groups.blacklist";

    private final Severity severity;

    public Mm2BlacklistDeprecatedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.MM2_BLACKLIST_DEPRECATED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String connectorClass = trimOrNull(p.getProperty(CONNECTOR_CLASS));
            if (connectorClass == null) continue;
            if (!MM2_CLASSES.contains(connectorClass)) continue;

            String topicsBlacklist = trimOrNull(p.getProperty(TOPICS_BLACKLIST));
            if (topicsBlacklist != null) {
                out.add(new Violation(
                        RuleId.MM2_BLACKLIST_DEPRECATED, severity,
                        ctx.relativize(e.getKey()), "key:" + TOPICS_BLACKLIST, 0,
                        "MirrorMaker 2 connector (" + connectorClass + ") sets the "
                                + "legacy key topics.blacklist=" + topicsBlacklist
                                + " — this key was RENAMED to topics.exclude by "
                                + "KIP-629 (Kafka 2.8, 2020) as part of the codebase-wide "
                                + "rename of `blacklist`/`whitelist` configuration keys "
                                + "to `exclude`/`include`. The legacy key is accepted as "
                                + "a DEPRECATED ALIAS in MM2 2.8-3.x (the connector "
                                + "starts, reads the value, and logs a WARN: "
                                + "\"The configuration 'topics.blacklist' was supplied "
                                + "but isn't a known config. (Use 'topics.exclude' "
                                + "instead.)\") and is SCHEDULED for REMOVAL in Kafka 4.0 "
                                + "— at which point the connector will fail at startup "
                                + "with ConfigException: Unknown configuration "
                                + "'topics.blacklist'. Fix: replace topics.blacklist="
                                + topicsBlacklist + " with topics.exclude="
                                + topicsBlacklist + " (semantics IDENTICAL — same "
                                + "comma-separated regex/glob list of topics to skip "
                                + "during replication; no value change required). If "
                                + "both keys are currently set (a safe migration "
                                + "intermediate), remove the legacy key — the new key "
                                + "wins at config-resolution time and the legacy key is "
                                + "cruft. Common origins: (a) MM2 config from a "
                                + "2019-2020-era tutorial or Confluent blog post; "
                                + "(b) Helm-chart template from the 2.x era with the "
                                + "legacy key hard-coded; (c) migration from Confluent "
                                + "Replicator that translated `topic.blacklist` "
                                + "(singular) to `topics.blacklist` (plural) without "
                                + "updating to the modern `topics.exclude`."));
            }

            String groupsBlacklist = trimOrNull(p.getProperty(GROUPS_BLACKLIST));
            if (groupsBlacklist != null) {
                out.add(new Violation(
                        RuleId.MM2_BLACKLIST_DEPRECATED, severity,
                        ctx.relativize(e.getKey()), "key:" + GROUPS_BLACKLIST, 0,
                        "MirrorMaker 2 connector (" + connectorClass + ") sets the "
                                + "legacy key groups.blacklist=" + groupsBlacklist
                                + " — this key was RENAMED to groups.exclude by "
                                + "KIP-629 (Kafka 2.8, 2020) as part of the codebase-wide "
                                + "rename of `blacklist`/`whitelist` configuration keys "
                                + "to `exclude`/`include`. The legacy key is accepted as "
                                + "a DEPRECATED ALIAS in MM2 2.8-3.x (the connector "
                                + "starts, reads the value, and logs a WARN: "
                                + "\"The configuration 'groups.blacklist' was supplied "
                                + "but isn't a known config. (Use 'groups.exclude' "
                                + "instead.)\") and is SCHEDULED for REMOVAL in Kafka 4.0 "
                                + "— at which point the connector will fail at startup "
                                + "with ConfigException: Unknown configuration "
                                + "'groups.blacklist'. Fix: replace groups.blacklist="
                                + groupsBlacklist + " with groups.exclude="
                                + groupsBlacklist + " (semantics IDENTICAL — same "
                                + "comma-separated regex/glob list of consumer-groups to "
                                + "skip during checkpoint-offset replication; no value "
                                + "change required). If both keys are currently set "
                                + "(a safe migration intermediate), remove the legacy "
                                + "key — the new key wins at config-resolution time "
                                + "and the legacy key is cruft. Common origins: "
                                + "(a) MM2 config from a 2019-2020-era tutorial; "
                                + "(b) Helm-chart template hard-coded with the legacy "
                                + "key; (c) bulk-rename across configs that missed the "
                                + "`groups.*` pair while updating only the `topics.*` "
                                + "pair."));
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
