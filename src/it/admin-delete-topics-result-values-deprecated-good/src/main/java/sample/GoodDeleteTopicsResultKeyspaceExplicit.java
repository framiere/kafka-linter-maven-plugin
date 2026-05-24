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
 * RULE: ADMIN_DELETE_TOPICS_RESULT_VALUES_DEPRECATED — must NOT fire.
 *
 * <p>The three methods below exercise the supported, key-space-explicit
 * accessors introduced by KIP-516:
 *
 * <ol>
 *   <li>{@code topicNameValues()} direct call — name-keyed Map accessor
 *       for a name-keyed delete.</li>
 *   <li>{@code topicIdValues()} direct call — id-keyed Map accessor for
 *       an id-keyed delete.</li>
 *   <li>{@code result::topicIdValues} method-ref capture into a
 *       {@code Supplier<Map<Uuid, KafkaFuture<Void>>>}.</li>
 * </ol>
 *
 * <h2>Why this is safe</h2>
 *
 * <p>The new accessors are partitioned by key-space and refuse silent
 * mis-keying:
 *
 * <ul>
 *   <li>{@code topicNameValues()} returns {@code Map<String, KafkaFuture<Void>>}
 *       — valid only when the delete was made by NAME. The lexical
 *       &laquo;Name&raquo; surfaces the key-space assumption at the
 *       call site.</li>
 *   <li>{@code topicIdValues()} returns {@code Map<Uuid, KafkaFuture<Void>>}
 *       — valid only when the delete was made by ID. The {@code Uuid}
 *       key-type makes the assumption a compile-time contract: callers
 *       cannot accidentally feed it a name-keyed delete because the type
 *       won't unify.</li>
 * </ul>
 *
 * <p>The rule resolver matches on (owner, name) for both INVOKEVIRTUAL
 * and INVOKEDYNAMIC. The names {@code topicNameValues} and
 * {@code topicIdValues} are not in the deprecated set — so neither
 * direct calls nor method-ref captures fire here.
 */
public final class GoodDeleteTopicsResultKeyspaceExplicit {

    private static Properties adminProps() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put(AdminClientConfig.CLIENT_ID_CONFIG, "good-delete-topics-keyspace");
        return p;
    }

    public Map<String, KafkaFuture<Void>> deleteByNameThenAskTopicNameValues() {
        try (Admin admin = Admin.create(adminProps())) {
            DeleteTopicsResult result =
                    admin.deleteTopics(TopicCollection.ofTopicNames(List.of("orders", "payments")));
            // DOES NOT FIRE — supported key-space-explicit accessor.
            // The lexical "Name" in the method name documents that this
            // call is only meaningful for a name-keyed delete.
            return result.topicNameValues();
        }
    }

    public Map<Uuid, KafkaFuture<Void>> deleteByIdsThenAskTopicIdValues(Uuid id) {
        try (Admin admin = Admin.create(adminProps())) {
            DeleteTopicsResult result =
                    admin.deleteTopics(TopicCollection.ofTopicIds(List.of(id)));
            // DOES NOT FIRE — supported id-keyed accessor. The Map's
            // key-type Uuid mirrors the delete's key-space, so the
            // partial-success/total-success collection logic can run
            // without UnsupportedOperationException.
            return result.topicIdValues();
        }
    }

    public Supplier<Map<Uuid, KafkaFuture<Void>>> capturedTopicIdValues(Uuid id) {
        try (Admin admin = Admin.create(adminProps())) {
            DeleteTopicsResult result =
                    admin.deleteTopics(TopicCollection.ofTopicIds(List.of(id)));
            // DOES NOT FIRE — INVOKEDYNAMIC method-ref capture targeting
            // a SUPPORTED accessor. The bsmArg handle's name is
            // "topicIdValues", not in the deprecated set the rule
            // matches, so the walk does not flag this site.
            return result::topicIdValues;
        }
    }
}
