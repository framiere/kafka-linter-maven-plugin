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
 * RULE: ADMIN_DESCRIBE_TOPICS_RESULT_LEGACY_DEPRECATED — must NOT fire.
 *
 * <p>The four methods below exercise the supported, key-space-explicit
 * accessors introduced by KIP-516:
 *
 * <ol>
 *   <li>{@code topicNameValues()} direct call — name-keyed Map accessor for
 *       a name-keyed describe.</li>
 *   <li>{@code allTopicIds()} direct call — id-keyed KafkaFuture<Map>
 *       accessor for an id-keyed describe.</li>
 *   <li>{@code result::topicNameValues} method-ref capture into Supplier.</li>
 *   <li>{@code result::allTopicIds} method-ref capture into Supplier.</li>
 * </ol>
 *
 * <h2>Why this is safe</h2>
 *
 * <p>The new accessors are partitioned by key-space and refuse silent
 * mis-keying:
 *
 * <ul>
 *   <li>{@code topicNameValues()} / {@code allTopicNames()} are only valid
 *       on a result whose describe was made with names — calling them on
 *       an ID-keyed result yields an empty map AND the call site is now
 *       a code-review tell-tale (it lexically says &laquo;Name&raquo;).</li>
 *   <li>{@code topicIdValues()} / {@code allTopicIds()} are only valid
 *       on an ID-keyed describe, and the lexical &laquo;Id&raquo; in the
 *       method name surfaces the key-space assumption at the call site.</li>
 * </ul>
 *
 * <p>The rule resolver matches on (owner, name) for both INVOKEVIRTUAL and
 * INVOKEDYNAMIC. The names {@code topicNameValues}, {@code allTopicNames},
 * {@code topicIdValues}, {@code allTopicIds} are not in the deprecated set
 * — so neither direct calls nor method-ref captures fire here.
 */
public final class GoodDescribeTopicsResultKeyspaceExplicit {

    private static Properties adminProps() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put(AdminClientConfig.CLIENT_ID_CONFIG, "good-describe-topics-keyspace");
        return p;
    }

    public Map<String, KafkaFuture<TopicDescription>> describeNamesThenAskTopicNameValues() {
        try (Admin admin = Admin.create(adminProps())) {
            DescribeTopicsResult result =
                    admin.describeTopics(TopicCollection.ofTopicNames(List.of("orders", "payments")));
            // DOES NOT FIRE — supported key-space-explicit accessor. The
            // lexical "Name" in the method name documents that this call
            // is only meaningful for a name-keyed describe.
            return result.topicNameValues();
        }
    }

    public KafkaFuture<Map<Uuid, TopicDescription>> describeIdsThenAskAllTopicIds(Uuid id) {
        try (Admin admin = Admin.create(adminProps())) {
            DescribeTopicsResult result =
                    admin.describeTopics(TopicCollection.ofTopicIds(List.of(id)));
            // DOES NOT FIRE — supported id-keyed KafkaFuture<Map<Uuid, ...>>
            // accessor; the result's Map key is Uuid, matching the describe.
            return result.allTopicIds();
        }
    }

    public Supplier<Map<String, KafkaFuture<TopicDescription>>> capturedTopicNameValues() {
        try (Admin admin = Admin.create(adminProps())) {
            DescribeTopicsResult result =
                    admin.describeTopics(TopicCollection.ofTopicNames(List.of("orders")));
            // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture targeting a
            // SUPPORTED accessor. The bsmArg handle's name is
            // "topicNameValues", not in the LEGACY_NAMES set the rule
            // matches, so the descriptor walk rejects it.
            return result::topicNameValues;
        }
    }

    public Supplier<KafkaFuture<Map<Uuid, TopicDescription>>> capturedAllTopicIds(Uuid id) {
        try (Admin admin = Admin.create(adminProps())) {
            DescribeTopicsResult result =
                    admin.describeTopics(TopicCollection.ofTopicIds(List.of(id)));
            // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture targeting
            // allTopicIds, also supported.
            return result::allTopicIds;
        }
    }
}
