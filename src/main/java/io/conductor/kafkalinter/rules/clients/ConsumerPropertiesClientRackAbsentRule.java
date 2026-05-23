package io.conductor.kafkalinter.rules.clients;

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
 * Project-scoped rule. Fires on a plain kafka-clients consumer .properties
 * file (identified by top-level {@code key.deserializer} or
 * {@code value.deserializer}) that does NOT set {@code client.rack}.
 *
 * <p>KIP-392 (Apache Kafka 2.4, December 2019) introduced consumer
 * rack-aware fetching. When {@code client.rack} is set to the consumer's
 * AZ identifier AND broker-side {@code broker.rack} is set per broker AND
 * broker-side {@code replica.selector.class=
 * org.apache.kafka.common.replica.RackAwareReplicaSelector}, the consumer
 * is allowed to fetch from any in-sync follower replica in the same rack
 * rather than always fetching from the partition leader. This cuts
 * cross-AZ data-transfer (a paid cloud-provider dimension at $0.01-0.02/GB
 * on AWS/GCP/Azure) by 50-90% on multi-AZ clusters.
 *
 * <p>INFO severity because (a) the optimization requires broker-side
 * coordination the linter cannot verify, (b) single-AZ deployments do not
 * benefit, (c) the rule is a documentation nudge.
 *
 * <p>A file is "consumer-shaped" when it sets {@code key.deserializer} OR
 * {@code value.deserializer} as a TOP-LEVEL key. Excludes Connect
 * ({@code connector.class}) and Streams ({@code application.id}).
 */
public final class ConsumerPropertiesClientRackAbsentRule implements ProjectScopedRule {

    private static final String KEY_DESERIALIZER = "key.deserializer";
    private static final String VALUE_DESERIALIZER = "value.deserializer";
    private static final String CONNECTOR_CLASS = "connector.class";
    private static final String APPLICATION_ID = "application.id";
    private static final String CLIENT_RACK = "client.rack";

    private final Severity severity;

    public ConsumerPropertiesClientRackAbsentRule(Severity severity) {
        this.severity = severity;
    }

    @Override
    public RuleId id() {
        return RuleId.CONSUMER_PROPERTIES_CLIENT_RACK_ABSENT;
    }

    @Override
    public List<Violation> check(ProjectContext ctx) {
        List<Violation> out = new ArrayList<>();
        for (Map.Entry<Path, Properties> e : ctx.propertiesFiles().entrySet()) {
            Properties p = e.getValue();
            String shapeKey = consumerShapeKey(p);
            if (shapeKey == null) continue;
            if (isNonEmpty(p.getProperty(CONNECTOR_CLASS))) continue;
            if (isNonEmpty(p.getProperty(APPLICATION_ID))) continue;
            if (isNonEmpty(p.getProperty(CLIENT_RACK))) continue;
            out.add(new Violation(
                    RuleId.CONSUMER_PROPERTIES_CLIENT_RACK_ABSENT, severity,
                    ctx.relativize(e.getKey()), "key:" + CLIENT_RACK, 0,
                    "kafka-clients consumer .properties file (detected via "
                            + "top-level `" + shapeKey + "`) does NOT set `"
                            + CLIENT_RACK + "`. KIP-392 (Apache Kafka 2.4, "
                            + "December 2019) introduced consumer rack-"
                            + "aware fetching: when the consumer-side `"
                            + CLIENT_RACK + "` is set to the consumer's "
                            + "availability-zone identifier (e.g., `us-"
                            + "east-1a`, `eu-west-3b`) AND the broker-side "
                            + "has `broker.rack` set per broker AND the "
                            + "broker-side `replica.selector.class` is "
                            + "`org.apache.kafka.common.replica."
                            + "RackAwareReplicaSelector`, the consumer is "
                            + "allowed to fetch from any IN-SYNC FOLLOWER "
                            + "replica IN THE SAME RACK rather than always "
                            + "fetching from the partition leader. The "
                            + "leader may be in a different AZ from the "
                            + "consumer; without `" + CLIENT_RACK + "`, "
                            + "every fetch traverses the inter-AZ network "
                            + "— a paid cloud-provider data-transfer "
                            + "dimension that on AWS/GCP/Azure is the "
                            + "single largest line-item of most Kafka "
                            + "cloud bills (~$0.01-0.02/GB cross-AZ on "
                            + "AWS; ~$0.01/GB on GCP; ~$0.01/GB on Azure; "
                            + "intra-AZ is free). With `" + CLIENT_RACK
                            + "` set correctly AND the broker-side stack "
                            + "configured, the consumer fetches from a "
                            + "same-AZ follower; the inter-AZ traversal "
                            + "is eliminated; the data-transfer line-item "
                            + "drops by 50-90% (depending on the topic's "
                            + "replication-factor and how many partitions "
                            + "have a follower in the consumer's AZ). "
                            + "Specific impacts of the absent key: (a) "
                            + "**AWS multi-AZ cluster (RF=3 across 3 AZs) "
                            + "— ~67% of fetches cross an AZ boundary "
                            + "because the leader is randomly distributed "
                            + "across all 3 AZs.** At 50 MB/sec cluster-"
                            + "wide consumer-side fetch throughput, that's "
                            + "~33 MB/sec cross-AZ × 86400s/day × 30 = "
                            + "~87 TB/month × $0.01/GB × 6 AZ-pairs (round-"
                            + "trip) ≈ $5200/month in AWS DataTransfer-"
                            + "Regional-Bytes charges. Setting `"
                            + CLIENT_RACK + "` per-consumer + broker-side "
                            + "`RackAwareReplicaSelector` drops the "
                            + "monthly bill to ~$500. (b) **GCP multi-"
                            + "region cluster (RF=5 across 3 regions) — "
                            + "~67% of fetches cross a region boundary; "
                            + "GCP inter-region traffic is $0.01-0.02/GB.** "
                            + "At 100 MB/sec total fetch × 67% × "
                            + "$0.01/GB ≈ $17.4k/month; with `"
                            + CLIENT_RACK + "` set, drops to ~$2k/month. "
                            + "(c) **MSK / Confluent Cloud Dedicated — "
                            + "broker-side ALREADY configured with rack-"
                            + "aware selector by default; only the "
                            + "consumer side needs `" + CLIENT_RACK + "` "
                            + "set.** Many operators do not realize the "
                            + "broker side is already there and never "
                            + "configure the consumer side. The cost "
                            + "savings are immediate the moment "
                            + CLIENT_RACK + " is set. (d) **Strimzi "
                            + "cluster — broker.rack set via "
                            + "`KafkaNodePool.spec.template.brokerRack` "
                            + "but consumer-side empty.** Broker side is "
                            + "configured; broker selector falls back to "
                            + "leader because the consumer-side rack is "
                            + "empty; the configured broker-side stack "
                            + "does nothing. Silent: no error, no log, "
                            + "just no cost reduction. Common bug shapes: "
                            + "(1) Helm chart with no `" + CLIENT_RACK
                            + "` default; 100+ teams use it; each pays "
                            + "the cross-AZ tax; (2) Spring Boot consumer "
                            + "config — Apache Kafka docs treat `"
                            + CLIENT_RACK + "` as a footnote; most "
                            + "tutorials never mention it; (3) Multi-"
                            + "tenant consumer library passing properties "
                            + "verbatim; library docs do not mention `"
                            + CLIENT_RACK + "`; no app ever sets it; (4) "
                            + "Confluent Cloud Basic / Standard tier — "
                            + "rack-aware fetch is Dedicated-tier only; "
                            + "operators migrating up tiers from on-prem "
                            + "set `" + CLIENT_RACK + "` correctly but "
                            + "operators starting on Cloud never learn "
                            + "about it. Fix: ONE LINE. Set `"
                            + CLIENT_RACK + "=${POD_ZONE}` (or per-pod "
                            + "environment-variable injection that "
                            + "resolves to the consumer's actual AZ at "
                            + "process start). For Kubernetes "
                            + "deployments, the standard pattern is to "
                            + "set the `topology.kubernetes.io/zone` "
                            + "label on the node and propagate to the "
                            + "pod via the Downward API. Companion "
                            + "broker-side prerequisites (configure once "
                            + "per cluster): every broker MUST have "
                            + "`broker.rack=<az-of-broker>` set in its "
                            + "server.properties; the cluster MUST have "
                            + "`replica.selector.class=org.apache.kafka."
                            + "common.replica.RackAwareReplicaSelector` "
                            + "(default is the leader-only selector — no "
                            + "rack-awareness). All three must be set "
                            + "together for the optimization to take "
                            + "effect. Sibling rule [[kafka-client-rack-"
                            + "placeholder]] catches the related bug of "
                            + "an UN-RESOLVED placeholder leaking through "
                            + "(`" + CLIENT_RACK + "=AZ_PLACEHOLDER` or "
                            + "`" + CLIENT_RACK + "=${UNRESOLVED_VAR}`)."));
        }
        return out;
    }

    private static String consumerShapeKey(Properties p) {
        if (isNonEmpty(p.getProperty(KEY_DESERIALIZER))) return KEY_DESERIALIZER;
        if (isNonEmpty(p.getProperty(VALUE_DESERIALIZER))) return VALUE_DESERIALIZER;
        return null;
    }

    private static boolean isNonEmpty(String v) {
        return v != null && !v.trim().isEmpty();
    }
}
