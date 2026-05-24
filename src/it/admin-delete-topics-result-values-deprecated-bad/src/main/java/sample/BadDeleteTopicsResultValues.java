package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * RULE: ADMIN_DELETE_TOPICS_RESULT_VALUES_DEPRECATED.
 *
 * <p>Exercises two shapes the rule must catch:
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on
 *       {@code DeleteTopicsResult.values()} — the legacy name-keyed Map
 *       accessor that throws {@code UnsupportedOperationException} at
 *       runtime when the underlying delete was made with
 *       {@code TopicCollection.ofTopicIds(...)}.</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code result::values} stored into a
 *       {@code Supplier<Map<String, KafkaFuture<Void>>>}. The user-class
 *       bytecode contains ZERO {@code INVOKE*} targeting {@code values};
 *       the call lives in the {@code LambdaMetafactory}-synthesized bridge.</li>
 * </ol>
 *
 * <h2>Why this overload is dangerous</h2>
 *
 * <p>{@code Admin.deleteTopics(TopicCollection.ofTopicIds(...))} is the
 * right shape for idempotent topic-cleanup against a topic that may have
 * been recreated under the same name (the topic-ID disambiguates the
 * incarnation). But the LEGACY {@code values()} accessor is name-keyed:
 * calling it on an id-keyed result throws UOE at result-collection time,
 * AFTER the brokers have already executed the deletes. The calling code
 * cannot collect the per-topic Futures it needs to distinguish
 * partial-success from total-success; naive retry logic re-runs the
 * delete against topics that no longer exist, surfacing as
 * {@code UnknownTopicOrPartitionException} on the second pass.
 */
public final class BadDeleteTopicsResultValues {

    private static Properties adminProps() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put(AdminClientConfig.CLIENT_ID_CONFIG, "bad-delete-topics-legacy");
        return p;
    }

    public Map<String, KafkaFuture<Void>> deleteByNameThenAskLegacyValues() {
        try (Admin admin = Admin.create(adminProps())) {
            DeleteTopicsResult result =
                    admin.deleteTopics(TopicCollection.ofTopicNames(List.of("orders", "payments")));
            // FIRES — direct INVOKEVIRTUAL on the legacy name-keyed accessor.
            // Even for a name-keyed delete the legacy accessor is deprecated
            // for KIP-516 disambiguation: the right shape is topicNameValues()
            // which makes the key-space lexically explicit and refuses silent
            // mis-keying.
            return result.values();
        }
    }

    public Map<String, KafkaFuture<Void>> deleteByIdsThenAskLegacyValuesThrowsUOE(Uuid id) {
        try (Admin admin = Admin.create(adminProps())) {
            DeleteTopicsResult result =
                    admin.deleteTopics(TopicCollection.ofTopicIds(List.of(id)));
            // FIRES — direct INVOKEVIRTUAL on the legacy accessor against
            // an id-keyed result. Throws UnsupportedOperationException at
            // runtime AFTER the brokers have already executed the deletes.
            return result.values();
        }
    }

    public Supplier<Map<String, KafkaFuture<Void>>> capturedValues(Uuid id) {
        try (Admin admin = Admin.create(adminProps())) {
            DeleteTopicsResult result =
                    admin.deleteTopics(TopicCollection.ofTopicIds(List.of(id)));
            // FIRES — INVOKEDYNAMIC method-ref capture. SAM Supplier#get
            // returns Object; descriptor erases to ()Ljava/util/Map; on
            // the captured handle. The user-class bytecode contains ZERO
            // INVOKE* targeting values; the call lives in the
            // LambdaMetafactory bridge, so a name-only MethodInsnNode
            // walk would miss it.
            return result::values;
        }
    }
}
