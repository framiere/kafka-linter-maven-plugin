package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * RULE: ADMIN_DESCRIBE_TOPICS_RESULT_LEGACY_DEPRECATED.
 *
 * <p>Exercises four shapes the rule must catch:
 *
 * <ol>
 *   <li>Direct {@code INVOKEVIRTUAL} on {@code DescribeTopicsResult.values()}
 *       — the legacy name-keyed Map accessor.</li>
 *   <li>Direct {@code INVOKEVIRTUAL} on {@code DescribeTopicsResult.all()}
 *       — the legacy name-keyed KafkaFuture<Map> accessor.</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code result::values} stored into a
 *       {@code Supplier<Map<String, KafkaFuture<TopicDescription>>>}. The
 *       user-class bytecode contains ZERO {@code INVOKE*} targeting
 *       {@code values}; the call lives only in the
 *       {@code LambdaMetafactory}-synthesized bridge.</li>
 *   <li>{@code INVOKEDYNAMIC} method-reference capture
 *       {@code result::all} stored into a
 *       {@code Supplier<KafkaFuture<Map<String, TopicDescription>>>}.</li>
 * </ol>
 *
 * <h2>Why this overload is dangerous</h2>
 *
 * <p>The {@code TopicCollection.ofTopicIds(...)} describe path is a real
 * production shape: when a topic gets recreated under the same name the
 * topic-ID changes, and ID-keyed describes are the only way to refuse
 * silent matches against the new incarnation. But the LEGACY
 * {@code values()} / {@code all()} accessors are name-keyed: they look up
 * the empty name-keyed slice of an ID-keyed result and return an EMPTY
 * map with NO exception, NO log line, NO missing-key signal. A topic-
 * management script that runs to completion silently returns
 * &laquo;no topics found&raquo; for every ID-keyed query, and downstream
 * (reconciler, cleanup job, healthcheck) makes the wrong decision
 * silently.
 *
 * <h2>Why so many sibling rules are OFF</h2>
 *
 * <p>The fixture intentionally leaves the Admin un-closed inside small
 * helper methods so the BAD/GOOD pair is symmetric and the build-failure
 * cause is attributable to
 * {@code ADMIN_DESCRIBE_TOPICS_RESULT_LEGACY_DEPRECATED} alone. Sibling
 * admin-lifecycle rules are silenced via the pom's {@code <severities>}.
 */
public final class BadDescribeTopicsResultLegacy {

    private static Properties adminProps() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put(AdminClientConfig.CLIENT_ID_CONFIG, "bad-describe-topics-legacy");
        return p;
    }

    public Map<String, KafkaFuture<TopicDescription>> describeNamesThenAskLegacyValues() {
        try (Admin admin = Admin.create(adminProps())) {
            DescribeTopicsResult result =
                    admin.describeTopics(TopicCollection.ofTopicNames(List.of("orders", "payments")));
            // FIRES — direct INVOKEVIRTUAL on the legacy name-keyed accessor.
            // Even on a name-keyed describe the legacy accessor's behavior is
            // deprecated for KIP-516 disambiguation: the right shape is
            // topicNameValues() — which explicitly communicates the key-space
            // and refuses to silently return empty on the wrong-keyed query.
            return result.values();
        }
    }

    public KafkaFuture<Map<String, TopicDescription>> describeIdsThenAskLegacyAllSilentEmpty(Uuid id) {
        try (Admin admin = Admin.create(adminProps())) {
            DescribeTopicsResult result =
                    admin.describeTopics(TopicCollection.ofTopicIds(List.of(id)));
            // FIRES — direct INVOKEVIRTUAL on the legacy name-keyed accessor
            // against an ID-keyed result. The future resolves to an empty Map
            // with NO exception, NO log, NO signal. Topic-management scripts
            // built on this empty result then make the WRONG decision silently.
            return result.all();
        }
    }

    public Supplier<Map<String, KafkaFuture<TopicDescription>>> capturedValues() {
        try (Admin admin = Admin.create(adminProps())) {
            DescribeTopicsResult result =
                    admin.describeTopics(TopicCollection.ofTopicNames(List.of("orders")));
            // FIRES — INVOKEDYNAMIC method-ref capture. SAM Supplier#get
            // returns Object; descriptor erases to ()Ljava/util/Map; on the
            // captured handle. The user-class bytecode contains ZERO INVOKE*
            // targeting values; the call lives in the LambdaMetafactory
            // bridge, so a name-only walk on MethodInsnNode would miss it.
            return result::values;
        }
    }

    public Supplier<KafkaFuture<Map<String, TopicDescription>>> capturedAll(Uuid id) {
        try (Admin admin = Admin.create(adminProps())) {
            DescribeTopicsResult result =
                    admin.describeTopics(TopicCollection.ofTopicIds(List.of(id)));
            // FIRES — INVOKEDYNAMIC method-ref capture targeting the legacy
            // KafkaFuture<Map> accessor. Same INVOKEDYNAMIC-bridge logic as
            // above; the rule's bsm-arg walk catches the handle's owner+name.
            return result::all;
        }
    }
}
