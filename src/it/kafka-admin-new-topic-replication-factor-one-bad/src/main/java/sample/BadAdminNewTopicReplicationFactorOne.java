package sample;

import org.apache.kafka.clients.admin.NewTopic;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * RULE: ADMIN_NEW_TOPIC_REPLICATION_FACTOR_ONE.
 *
 * Fires when {@code new NewTopic(name, partitions, (short) 1)} is constructed
 * with a literal {@code 1} replication factor. Replication factor 1 means every
 * partition lives on a single broker — any broker reboot takes the topic
 * offline, any disk loss loses the data.
 */
public final class BadAdminNewTopicReplicationFactorOne {

    /** Anti-pattern: literal (short) 1 replication factor — INVOKESPECIAL + ICONST_1. */
    public NewTopic singleTopicRfOne() {
        return new NewTopic("orders", 6, (short) 1); // FIRES — RF=1
    }

    /** Anti-pattern: same hazard inside a List.of(...) — common IaC shape. */
    public List<NewTopic> batchedProvisioning() {
        return List.of(
                new NewTopic("orders", 6, (short) 1),   // FIRES — RF=1
                new NewTopic("events", 12, (short) 1)   // FIRES — RF=1
        );
    }

    /** Anti-pattern: defaults extracted into local but still constructs RF=1. */
    public NewTopic devTopic(String name) {
        int partitions = 3;
        return new NewTopic(name, partitions, (short) 1); // FIRES — RF=1
    }

    /** Control: replication factor 3 — must NOT fire. */
    public NewTopic productionTopic() {
        return new NewTopic("orders", 6, (short) 3); // OK — RF=3
    }

    /** Control: replication factor 2 — must NOT fire. */
    public NewTopic twoReplicas() {
        return new NewTopic("orders", 6, (short) 2); // OK — RF=2
    }

    /** Control: explicit replicasAssignments Map ctor — different descriptor, must NOT fire. */
    public NewTopic explicitAssignment() {
        Map<Integer, List<Integer>> assignments = Map.of(
                0, List.of(0, 1, 2),
                1, List.of(1, 2, 0)
        );
        return new NewTopic("orders", assignments); // OK — different ctor (Map overload)
    }

    /** Control: Optional<Short> ctor with Optional.empty() — inherits cluster default; different descriptor. */
    public NewTopic clusterDefaults() {
        return new NewTopic("orders", Optional.of(6), Optional.<Short>empty()); // OK — Optional overload
    }

    /** Control: variable-shape replication factor — out of scope by design (HIGH-confidence literal-only rule). */
    public NewTopic variableShape(short rf) {
        return new NewTopic("orders", 6, rf); // OK — ALOAD, not literal
    }
}
