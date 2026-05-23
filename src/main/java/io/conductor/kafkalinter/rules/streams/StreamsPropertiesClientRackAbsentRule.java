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
 * (identified by top-level {@code application.id}) that does NOT set
 * {@code client.rack}.
 *
 * <p>KIP-392 (Apache Kafka 2.4, December 2019) introduced consumer
 * rack-aware fetching. Streams inherits this directly: every
 * StreamThread runs an embedded {@code KafkaConsumer} for the source
 * topics (main consumer + restore consumer + global consumer), and
 * top-level consumer configs in the Streams .properties — such as
 * {@code client.rack} — are forwarded verbatim to all internal
 * consumers. Without {@code client.rack}, every StreamThread's fetch
 * traverses the inter-AZ network in multi-AZ deployments.
 *
 * <p>INFO severity because (a) the optimization requires broker-side
 * coordination the linter cannot verify, (b) single-AZ deployments do
 * not benefit, (c) the rule is a documentation nudge.
 *
 * <p>A file is "streams-shaped" when it sets {@code application.id} as
 * a TOP-LEVEL key. Excludes Connect ({@code connector.class}).
 */
public final class StreamsPropertiesClientRackAbsentRule implements ProjectScopedRule {

    private static final String APPLICATION_ID = "application.id";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String CLIENT_RACK = "client.rack";

    private final Severity severity;

    public StreamsPropertiesClientRackAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.STREAMS_PROPERTIES_CLIENT_RACK_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            if (!isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(CLIENT_RACK))) continue;
            out.add(new Violation(
                    RuleId.STREAMS_PROPERTIES_CLIENT_RACK_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + CLIENT_RACK, 0,
                    "Kafka Streams .properties file (detected via top-"
                            + "level `" + APPLICATION_ID + "`) does NOT "
                            + "set `" + CLIENT_RACK + "`. KIP-392 (Apache "
                            + "Kafka 2.4, December 2019) introduced "
                            + "consumer rack-aware fetching, and Kafka "
                            + "Streams inherits it directly because every "
                            + "StreamThread runs an embedded "
                            + "`KafkaConsumer`: top-level consumer configs "
                            + "in the Streams .properties (such as `"
                            + CLIENT_RACK + "`) are forwarded verbatim to "
                            + "all internal consumers — the MAIN consumer "
                            + "(steady-state source-topic fetching), the "
                            + "RESTORE consumer (state-store changelog "
                            + "replay during startup or rebalance), and "
                            + "the GLOBAL consumer (GlobalKTable). Without "
                            + "`" + CLIENT_RACK + "`, every fetch by "
                            + "every StreamThread traverses the inter-AZ "
                            + "network in multi-AZ deployments — a paid "
                            + "cloud-provider dimension that on AWS/GCP/"
                            + "Azure is the single largest line-item of "
                            + "most Streams cloud bills (~$0.01-0.02/GB "
                            + "cross-AZ on AWS; intra-AZ is free). Streams "
                            + "workloads hit this hard because every "
                            + "StreamThread reads CONTINUOUSLY from every "
                            + "assigned partition; there are no batched-"
                            + "fetch windows or natural quiescent periods. "
                            + "Specific impacts of the absent key: (a) "
                            + "**AWS multi-AZ Streams app — the cross-AZ "
                            + "tax compounds across StreamThreads.** A "
                            + "Streams app with 12 pods × 4 StreamThreads "
                            + "= 48 consumer instances at 5 MB/sec each = "
                            + "~240 MB/sec aggregate; on a 3-AZ cluster "
                            + "with RF=3 rack-aware placement, ~67% of "
                            + "fetches cross an AZ boundary; at AWS $0.01/"
                            + "GB cross-AZ pricing × 86400s × 30 days × 6 "
                            + "AZ-pair directions ≈ $25k/month for a "
                            + "single Streams app. With `" + CLIENT_RACK
                            + "=${POD_ZONE}` set, drops to ~$2.5k/month. "
                            + "(b) **State restoration after pod restart "
                            + "— the RESTORE consumer hammers cross-AZ "
                            + "for minutes.** A 10 GB changelog replay at "
                            + "50 MB/sec takes ~3-4 minutes of continuous "
                            + "cross-AZ traffic for every pod restart "
                            + "(rolling deploy, node eviction, "
                            + "autoscaling). The restore consumer also "
                            + "obeys `" + CLIENT_RACK + "`. (c) **Strimzi "
                            + "with broker-side rack-awareness but "
                            + "consumer-side empty.** Strimzi's "
                            + "`KafkaNodePool.spec.template.brokerRack` "
                            + "is set; `replica.selector.class=Rack"
                            + "AwareReplicaSelector` is configured; "
                            + "broker side is ready. But the Streams "
                            + "app's .properties has no `" + CLIENT_RACK
                            + "` line; broker selector falls back to "
                            + "leader; the configured stack does "
                            + "nothing. Silent: no error, no log, no "
                            + "cost reduction. (d) **GlobalKTable "
                            + "broadcast traffic — partial mitigation.** "
                            + "GlobalKTable replicates all partitions to "
                            + "every instance; with `" + CLIENT_RACK
                            + "` set, each instance reads from same-AZ "
                            + "followers where available, cutting "
                            + "GlobalKTable broadcast traffic by ~67%. "
                            + "Common bug shapes: (1) AWS MSK / "
                            + "Confluent Cloud Dedicated — broker side "
                            + "already configured with rack-aware "
                            + "selector by default, only the consumer "
                            + "side needs `" + CLIENT_RACK + "` set; "
                            + "operators commonly assume the cluster is "
                            + "fully optimized; (2) Helm chart for "
                            + "Streams apps with no `" + CLIENT_RACK
                            + "` default — 50+ teams use the chart, each "
                            + "pays the cross-AZ tax; (3) Spring Boot "
                            + "Streams autoconfig — "
                            + "`KafkaStreamsConfiguration` bean does NOT "
                            + "inject `" + CLIENT_RACK + "`; (4) Apache "
                            + "Kafka docs treat KIP-392 as a footnote; "
                            + "most tutorials never mention it. Fix: ONE "
                            + "LINE. Set `" + CLIENT_RACK + "=${POD_ZONE}"
                            + "` (with POD_ZONE injected via the "
                            + "Kubernetes Downward API from the node's "
                            + "`topology.kubernetes.io/zone` label). "
                            + "Companion broker-side prerequisites "
                            + "(configure once per cluster): every "
                            + "broker MUST have `broker.rack=<az-of-"
                            + "broker>` set; the cluster MUST have "
                            + "`replica.selector.class=org.apache.kafka."
                            + "common.replica.RackAwareReplicaSelector` "
                            + "(default is leader-only). For Streams "
                            + "workloads doing standby-task "
                            + "rebalancing, also consider setting "
                            + "`rack.aware.assignment.tags=zone` (KIP-"
                            + "708, Apache Kafka 3.2, May 2022) — the "
                            + "STREAMS-SPECIFIC standby-placement rack-"
                            + "awareness, orthogonal to KIP-392's fetch-"
                            + "side rack-awareness but complementary. "
                            + "Sibling rule [[consumer-properties-client-"
                            + "rack-absent]] is the plain-consumer "
                            + "equivalent; sibling rule [[streams-"
                            + "properties-rack-aware-assignment-tags-"
                            + "absent]] catches the KIP-708 absence."));
        }
        return out;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
