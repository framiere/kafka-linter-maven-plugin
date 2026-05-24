package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * RULE: ADMIN_RESULT_DISCARDED.
 *
 * <p>Fires when a STATE-MUTATING {@code Admin} method is invoked and its returned
 * {@code *Result} object is immediately discarded (the next significant bytecode
 * instruction is {@code POP}).
 *
 * <p>Why this is a serious bug — what actually happens at runtime:
 * <ol>
 *   <li>Every state-mutating {@code Admin} method ({@code createTopics},
 *       {@code deleteTopics}, {@code createAcls}, {@code alterConfigs},
 *       {@code incrementalAlterConfigs}, {@code createPartitions},
 *       {@code electLeaders}, {@code alter*}, {@code delete*ConsumerGroup*}, …) is
 *       asynchronous by construction. The call returns a {@code *Result} object
 *       whose internals are one or more {@code KafkaFuture<T>} handles for the
 *       in-flight broker operation(s). Discarding the Result with {@code POP}
 *       does NOT cancel the request — the AdminClient's background network
 *       thread will dispatch it to the broker regardless.</li>
 *   <li>The broker processes the request and responds. The response is delivered
 *       into the {@code KafkaFuture}(s) wrapped by the discarded Result. With no
 *       application-side reference, the Result is garbage-collected, its
 *       futures are collected with it, and the broker's response — success OR
 *       a structured error — is dropped on the floor.</li>
 *   <li>Common silent-failure modes: {@code TopicExistsException},
 *       {@code TopicAuthorizationException}, {@code InvalidReplicationFactorException},
 *       {@code PolicyViolationException}, {@code TopicDeletionDisabledException},
 *       {@code ClusterAuthorizationException}, {@code UnknownTopicOrPartitionException},
 *       broker timeouts, ZooKeeper/KRaft commit failures, fencing, schema
 *       conflicts. Each of these would surface as an {@code ExecutionException}
 *       from {@code .all().get()} — but since {@code .get()} is never called,
 *       the application proceeds as if the mutation had succeeded.</li>
 *   <li>The typical real-world shape is a deploy pipeline or migration script
 *       that issues a batch of admin operations in sequence, never chains
 *       {@code .all().get()}, and reports 'all operations succeeded' to its
 *       caller. The cluster state diverges from the application's belief about
 *       it; the divergence is discovered weeks later during an audit, an
 *       incident, or a user-reported anomaly.</li>
 * </ol>
 *
 * <p>What the rule catches (per-method scan, no slot tracking required):
 * <ol>
 *   <li>For every {@code INVOKEVIRTUAL}/{@code INVOKEINTERFACE} on
 *       {@code Admin}/{@code AdminClient} whose method name is in the
 *       state-mutating set ({@code createTopics}, {@code deleteTopics},
 *       {@code createPartitions}, {@code createAcls}, {@code deleteAcls},
 *       {@code alterConfigs}, {@code incrementalAlterConfigs},
 *       {@code alterReplicaLogDirs}, {@code updateFeatures},
 *       {@code deleteConsumerGroups}, {@code alterConsumerGroupOffsets},
 *       {@code deleteConsumerGroupOffsets}, {@code removeMembersFromConsumerGroup},
 *       {@code electLeaders}, {@code alterPartitionReassignments},
 *       {@code createDelegationToken}, {@code renewDelegationToken},
 *       {@code expireDelegationToken}, {@code alterClientQuotas},
 *       {@code alterUserScramCredentials}, {@code unregisterBroker},
 *       {@code abortTransaction}, {@code fenceProducers},
 *       {@code deleteRecords}).</li>
 *   <li>Check {@code AsmUtil.nextSignificant(insn)}. If the next significant
 *       instruction is {@code POP} (or {@code POP2}), fire at the call site.</li>
 *   <li>Read-only {@code describe*} / {@code list*} methods are NOT in the set —
 *       discarding their Result wastes a broker round-trip but does not corrupt
 *       cluster state.</li>
 * </ol>
 *
 * <p>This Bad class triggers SEVEN fires — seven state-mutating admin operations
 * each discarded via POP.
 */
public final class BadAdminResultDiscarded {

    /** createTopics result discarded — the topic may not be created (TopicExistsException, PolicyViolation, AuthZ). */
    public void createTopicsThenDiscard() {
        Admin admin = Admin.create(baseAdminProps());
        try {
            admin.createTopics(Collections.singletonList(new NewTopic("orders", 3, (short) 2))); // reported
        } finally {
            admin.close();
        }
    }

    /** deleteTopics result discarded — the topic may not be deleted (TopicDeletionDisabled, AuthZ, UnknownTopic). */
    public void deleteTopicsThenDiscard() {
        Admin admin = Admin.create(baseAdminProps());
        try {
            admin.deleteTopics(List.of("orders")); // reported
        } finally {
            admin.close();
        }
    }

    /** createAcls result discarded — ACL silently not applied (AuthZ, broker config drift, principal mismatch). */
    public void createAclsThenDiscard() {
        Admin admin = Admin.create(baseAdminProps());
        AclBinding binding = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL),
                new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW));
        try {
            admin.createAcls(List.of(binding)); // reported
        } finally {
            admin.close();
        }
    }

    /** alterConfigs (legacy) result discarded — config silently not applied. */
    @SuppressWarnings("deprecation")
    public void alterConfigsThenDiscard() {
        Admin admin = Admin.create(baseAdminProps());
        ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, "orders");
        Config cfg = new Config(List.of(new ConfigEntry("retention.ms", "604800000")));
        try {
            admin.alterConfigs(Map.of(cr, cfg)); // reported
        } finally {
            admin.close();
        }
    }

    /** incrementalAlterConfigs result discarded — the config change silently does not take effect. */
    public void incrementalAlterConfigsThenDiscard() {
        Admin admin = Admin.create(baseAdminProps());
        ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, "orders");
        AlterConfigOp op = new AlterConfigOp(new ConfigEntry("retention.ms", "604800000"), AlterConfigOp.OpType.SET);
        try {
            admin.incrementalAlterConfigs(Map.of(cr, List.of(op))); // reported
        } finally {
            admin.close();
        }
    }

    /** createPartitions result discarded — the partition count silently stays the same. */
    public void createPartitionsThenDiscard() {
        Admin admin = Admin.create(baseAdminProps());
        try {
            admin.createPartitions(Map.of("orders", NewPartitions.increaseTo(12))); // reported
        } finally {
            admin.close();
        }
    }

    /** electLeaders result discarded — the leader election silently does not happen. */
    public void electLeadersThenDiscard() {
        Admin admin = Admin.create(baseAdminProps());
        try {
            admin.electLeaders(ElectionType.PREFERRED, Set.of(new TopicPartition("orders", 0))); // reported
        } finally {
            admin.close();
        }
    }

    private static Properties baseAdminProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("client.id", "bad-admin-result-discarded");
        return p;
    }
}
