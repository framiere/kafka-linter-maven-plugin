package io.conductor.kafkalinter.rules.connect;

import io.conductor.kafkalinter.RuleId;
import io.conductor.kafkalinter.Severity;
import io.conductor.kafkalinter.Violation;
import io.conductor.kafkalinter.rules.ProjectScopedRule;
import io.conductor.kafkalinter.scanner.ProjectContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * Project-scoped rule. Fires when a Kafka Connect connector .properties file
 * declares {@code topic.creation.<group>.<key>=<value>} entries for a group
 * that is NEITHER listed in {@code topic.creation.groups=<csv>} NOR equal to
 * the reserved name {@code default}. KIP-558 (Kafka 2.6+) topic-auto-creation
 * silently ignores orphan {@code topic.creation.<X>.<Y>} entries — no error,
 * no warning — so the per-group topic-config overrides are never applied.
 *
 * <p>Sibling of {@link ConnectTransformDefinedButNotListedRule} and
 * {@link ConnectPredicateDefinedButNotListedRule} — same chain-list-orphan
 * pattern, but for the topic-creation-groups chain.
 *
 * <p>One violation per orphan group per file (subkeys are aggregated).
 */
public final class ConnectTopicCreationGroupDefinedButNotListedRule implements ProjectScopedRule {

    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String GROUPS_LIST = "topic.creation.groups";
    private static final String TOPIC_CREATION_PREFIX = "topic.creation.";
    private static final String RESERVED_DEFAULT_GROUP = "default";
    // Non-per-group keys under the topic.creation. prefix to skip.
    private static final Set<String> NON_GROUP_KEYS = Set.of("groups", "enable");

    private final Severity severity;

    public ConnectTopicCreationGroupDefinedButNotListedRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONNECT_TOPIC_CREATION_GROUP_DEFINED_BUT_NOT_LISTED;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!notBlank(p.getProperty(CONNECTOR_CLASS))) continue;

            Set<String> declaredGroups = parseChain(p.getProperty(GROUPS_LIST));

            LinkedHashMap<String, Set<String>> orphanSubkeysByGroup = new LinkedHashMap<>();
            for (String key : p.stringPropertyNames()) {
                if (!key.startsWith(TOPIC_CREATION_PREFIX)) continue;
                String tail = key.substring(TOPIC_CREATION_PREFIX.length());
                // Skip non-per-group keys: topic.creation.groups, topic.creation.enable
                if (NON_GROUP_KEYS.contains(tail)) continue;
                int dotIdx = tail.indexOf('.');
                if (dotIdx <= 0) continue;
                String group = tail.substring(0, dotIdx);
                String subkey = tail.substring(dotIdx + 1);
                if (subkey.isEmpty()) continue;
                if (RESERVED_DEFAULT_GROUP.equals(group)) continue;
                if (declaredGroups.contains(group)) continue;

                orphanSubkeysByGroup.computeIfAbsent(group, g -> new TreeSet<>()).add(subkey);
            }

            for (Map.Entry<String, Set<String>> orphan : orphanSubkeysByGroup.entrySet()) {
                String group = orphan.getKey();
                String subkeys = String.join(", ", orphan.getValue());
                String chainDisplay = p.getProperty(GROUPS_LIST) == null
                        ? "<unset>" : p.getProperty(GROUPS_LIST).trim();
                out.add(new Violation(
                        RuleId.CONNECT_TOPIC_CREATION_GROUP_DEFINED_BUT_NOT_LISTED, severity,
                        ctx.relativize(e.getKey()),
                        "key:topic.creation." + group + ".include", 0,
                        "Connect connector " + p.getProperty(CONNECTOR_CLASS)
                                + " defines topic.creation." + group + ".* entries (subkeys: "
                                + subkeys + ") but '" + group + "' is NOT listed in "
                                + "topic.creation.groups=" + chainDisplay + " (and is not the "
                                + "reserved 'default' group). KIP-558's topic-auto-creation "
                                + "framework iterates ONLY the groups in topic.creation.groups "
                                + "— every topic.creation." + group + ".<key> entry is SILENTLY "
                                + "IGNORED at runtime (no error log, no warning). New source "
                                + "topics that the operator intended to match the '" + group
                                + "' group fall back to topic.creation.default.* settings "
                                + "instead, so the per-group overrides (cleanup.policy, "
                                + "partitions, replication.factor, retention.ms, etc.) are "
                                + "never applied. Either add '" + group + "' to "
                                + "topic.creation.groups=<csv> (so the group registers and its "
                                + "overrides apply), OR remove the orphan topic.creation."
                                + group + ".* entries (so the config reflects actual runtime "
                                + "behavior — no per-group override for that group)."));
            }
        }
        return out;
    }

    private static Set<String> parseChain(String raw) {
        if (!notBlank(raw)) return Set.of();
        Set<String> out = new HashSet<>();
        for (String token : raw.split(",")) {
            String t = token.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }

    private static boolean notBlank(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
