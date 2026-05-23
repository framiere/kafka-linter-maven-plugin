package io.conductor.kafkalinter.rules.streams;

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
 * Project-scoped rule. Fires on a Kafka Streams .properties file
 * (identified by top-level {@code application.id}, excluding Connect
 * configs via {@code connector.class}) that does NOT set
 * {@code rack.aware.assignment.tags}.
 *
 * <p>Streams declares {@code rack.aware.assignment.tags} with
 * default {@code Collections.emptyList()} in
 * {@code StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG}. KIP-708
 * (Apache Kafka 3.2, May 2022) introduced this knob as the rack-
 * aware standby placement mechanism for the
 * HighAvailabilityTaskAssignor (default since 2.6).
 *
 * <p>Without the configuration, the assignor places standbys
 * randomly across the cluster — meaning multi-AZ Kubernetes
 * deployments may end up with all 3 copies of a partition's state
 * in the SAME AZ, and an AZ outage stalls those partitions until
 * recovery.
 *
 * <p>INFO severity: the default is correct for single-AZ
 * deployments and stateless topologies, but is wrong for multi-AZ
 * stateful Streams deployments (the most common production shape
 * on managed Kubernetes).
 */
public final class StreamsPropertiesRackAwareAssignmentTagsAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String RACK_AWARE_ASSIGNMENT_TAGS = "rack.aware.assignment.tags";

    private final Severity severity;

    public StreamsPropertiesRackAwareAssignmentTagsAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_RACK_AWARE_ASSIGNMENT_TAGS_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(RACK_AWARE_ASSIGNMENT_TAGS))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_RACK_AWARE_ASSIGNMENT_TAGS_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + RACK_AWARE_ASSIGNMENT_TAGS, 0,
                    "Kafka Streams .properties file (detected via top-"
                            + "level `" + APPLICATION_ID + "`) does NOT set "
                            + "`" + RACK_AWARE_ASSIGNMENT_TAGS + "`. Streams "
                            + "defaults `" + RACK_AWARE_ASSIGNMENT_TAGS + "` "
                            + "to `Collections.emptyList()` (declared in "
                            + "`StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_"
                            + "CONFIG` with `ConfigDef.Type.LIST`). KIP-708 "
                            + "(Apache Kafka 3.2, May 2022) introduced this "
                            + "knob as the RACK-AWARE STANDBY PLACEMENT "
                            + "mechanism for the HighAvailabilityTaskAssignor "
                            + "(default since 2.6). The mechanism requires "
                            + "TWO things: (a) every instance declares its "
                            + "location via `client.tag.<key>=<value>` (e.g., "
                            + "`client.tag.zone=us-east-1a`, `client.tag."
                            + "cluster=primary`); (b) the operator lists the "
                            + "tag keys in `" + RACK_AWARE_ASSIGNMENT_TAGS
                            + "=zone,cluster`. The assignor's standby-"
                            + "placement algorithm uses the listed tags as "
                            + "FAULT DOMAINS, ranking candidate standby "
                            + "placements by HOW MANY of the listed tags "
                            + "DIFFER from the active's tags — preferring "
                            + "'all-tags-differ' over 'some-tags-differ' "
                            + "over 'no-tags-differ'. With the default "
                            + "empty list, the assignor skips rack-aware "
                            + "scoring entirely: standbys are placed by "
                            + "load-balancing only, with no regard for "
                            + "fault-domain diversity. For multi-AZ "
                            + "Kubernetes deployments (the most common "
                            + "production shape on managed K8s — EKS/GKE/AKS "
                            + "all default to 3-AZ node distribution), the "
                            + "default is wrong: random placement means "
                            + "roughly 1/N^(num.standby.replicas) of "
                            + "partitions have ALL copies in the same AZ, "
                            + "and an AZ outage stalls those partitions "
                            + "until recovery. Specific bug shapes: (a) "
                            + "**Multi-AZ Kubernetes — AZ failure stalls "
                            + "topology partitions** — 6 pods across 3 AZs, "
                            + "`num.standby.replicas=2`; without rack-"
                            + "awareness, ~30% of partitions have all 3 "
                            + "copies in the same AZ; AWS AZ outages happen "
                            + "1-2x/year per region; setting `"
                            + RACK_AWARE_ASSIGNMENT_TAGS + "=zone` ensures "
                            + "every partition has 1 copy in each AZ. (b) "
                            + "**Multi-cluster Streams (primary+DR)** — a "
                            + "single bootstrap.servers list spanning two "
                            + "K8s clusters; standbys may all land in "
                            + "primary; setting `" + RACK_AWARE_ASSIGNMENT_TAGS
                            + "=cluster` + per-pod `client.tag.cluster` "
                            + "spreads standbys across clusters. (c) **3-"
                            + "rack on-premise Kafka** — single-rack power "
                            + "outage (UPS failure) kills active+standbys "
                            + "co-located on that rack. (d) **Single-AZ "
                            + "deployment — default is fine** — no rack-"
                            + "awareness benefit; document the deliberate "
                            + "empty-list choice. (e) **Stateless topology** "
                            + "— `num.standby.replicas=0`; no standbys to "
                            + "place; default empty list is correct. (f) "
                            + "**Spring Boot Streams autoconfig — org-wide "
                            + "default leaks** — Spring's `KafkaStreams"
                            + "Configuration` bean omits this key; every "
                            + "Spring Boot Streams app inherits the empty-"
                            + "list framework default. Fix: ONE LINE in "
                            + ".properties plus PER-INSTANCE TAG INJECTION. "
                            + "For multi-AZ Kubernetes deployments: `"
                            + RACK_AWARE_ASSIGNMENT_TAGS + "=zone` in "
                            + ".properties AND inject `client.tag.zone="
                            + "<az-name>` per pod via Downward API "
                            + "(`spec.template.spec.containers[].env."
                            + "valueFrom.fieldRef.fieldPath: \"metadata."
                            + "labels['topology.kubernetes.io/zone']\"`), "
                            + "Helm template, or Spring Boot @Value. Multi-"
                            + "tag deployments: `" + RACK_AWARE_ASSIGNMENT_TAGS
                            + "=zone,cluster` declares MULTIPLE fault "
                            + "domains. Sibling rules [[streams-properties-"
                            + "num-standby-replicas-absent]] (rack-"
                            + "awareness only matters if you have standbys) "
                            + "and [[streams-properties-acceptable-recovery-"
                            + "lag-absent]] (the lag-threshold companion)."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
