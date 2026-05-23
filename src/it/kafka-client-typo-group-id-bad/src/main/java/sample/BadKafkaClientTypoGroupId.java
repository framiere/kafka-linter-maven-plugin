package sample;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * RULE: KAFKA_CLIENT_TYPO_GROUP_ID.
 *
 * <p>Fires when a Kafka client config key is set on a
 * {@code Properties}, {@code Map}, or {@code HashMap} using one of
 * the well-known typo shapes — camelCase or snake_case — instead of
 * Kafka's canonical dot-separated form.
 *
 * <p>Why this rule has to exist — what happens at the broker:
 * <ol>
 *   <li>Kafka client config keys ARE FREE-FORM STRINGS at the
 *       call-site. {@code props.put("group.id", "foo")} and
 *       {@code props.put("groupId", "foo")} are equally legal Java —
 *       the second compiles, runs, and the test suite passes.</li>
 *   <li>Inside {@code AbstractConfig.parse} the client filters props
 *       through the {@code ConfigDef} of the client type. Keys that
 *       don't appear in the def are LOGGED as
 *       "The configuration 'groupId' was supplied but isn't a known
 *       config" at WARN level, then DISCARDED. The client does not
 *       throw; it just behaves as if no such key was set.</li>
 *   <li>For {@code group.id} the consequence is severe: the typo
 *       leaves group.id unset, so the consumer cannot join a group;
 *       it falls back to an assignor-less standalone consumer that
 *       re-reads from latest on every restart. Operators see "messages
 *       are being lost on deploy" with no clear cause.</li>
 *   <li>For {@code enable.auto.commit} the typo means the value
 *       (false) is ignored and the broker default (true) kicks in.
 *       Suddenly offsets are committed on a wall-clock timer and
 *       in-flight processing failures cause data loss.</li>
 *   <li>For {@code auto.offset.reset} the typo leaves the broker
 *       default (latest) in effect, even though the code explicitly
 *       wrote "earliest". A consumer freshly subscribed to an
 *       existing topic will skip every existing record.</li>
 *   <li>For {@code compression.type} the typo means producers stay
 *       on the broker default (none, which is "respect what the
 *       producer asked for, which is none") — sending uncompressed
 *       even though the code asked for snappy. Network bills rise;
 *       no error is ever logged at ERROR.</li>
 * </ol>
 *
 * <p>How the rule detects typos — bytecode lexical scan:
 * <ol>
 *   <li>For each {@code MethodInsnNode} in each method body, check
 *       whether the call is a {@code put} or {@code setProperty}
 *       on {@code java/util/Properties}, {@code java/util/Map}, or
 *       {@code java/util/HashMap}.</li>
 *   <li>Walk back two LDC slots: the key constant and the value
 *       constant. The rule only looks at LDC String constants —
 *       interpolated keys ({@code "group" + ".id"}) or
 *       keys-from-variables bypass the rule. This is fine because
 *       typos always show up as literal strings in code.</li>
 *   <li>If the key string is in the TYPOS set, fire and SUGGEST
 *       the canonical dot form in the message — make the fix
 *       trivially actionable from the IDE.</li>
 * </ol>
 *
 * <p>This Bad class triggers TEN fires — one per typo, covering
 * both camelCase and snake_case variants of all 5 dotted-key
 * families that the rule knows. We also alternate among the three
 * config-holder types and the two put-method variants
 * ({@code put} and {@code setProperty}) to assert that the rule
 * keys on the CONFIG_HOLDERS × CONFIG_PUT_METHODS cross-product:
 * <ol>
 *   <li>{@code Properties.put("groupId", ...)} — group.id camelCase.</li>
 *   <li>{@code Properties.setProperty("group_id", ...)} — group.id snake_case via setProperty.</li>
 *   <li>{@code Properties.put("bootstrapServers", ...)} — bootstrap.servers camelCase.</li>
 *   <li>{@code Properties.setProperty("bootstrap_servers", ...)} — bootstrap.servers snake_case.</li>
 *   <li>{@code HashMap.put("enableAutoCommit", ...)} — enable.auto.commit camelCase via HashMap.</li>
 *   <li>{@code HashMap.put("enable_auto_commit", ...)} — enable.auto.commit snake_case.</li>
 *   <li>{@code Map.put("autoOffsetReset", ...)} — auto.offset.reset camelCase via Map interface.</li>
 *   <li>{@code Map.put("auto_offset_reset", ...)} — auto.offset.reset snake_case.</li>
 *   <li>{@code Properties.put("compressionType", ...)} — compression.type camelCase.</li>
 *   <li>{@code Properties.setProperty("compression_type", ...)} — compression.type snake_case.</li>
 * </ol>
 */
public final class BadKafkaClientTypoGroupId {

    public Properties buildConsumerPropsBadGroupIdCamel() {
        Properties props = new Properties();
        props.put("groupId", "orders-processor"); // reported — should be group.id
        return props;
    }

    public Properties buildConsumerPropsBadGroupIdSnake() {
        Properties props = new Properties();
        props.setProperty("group_id", "orders-processor"); // reported — should be group.id
        return props;
    }

    public Properties buildBootstrapPropsBadCamel() {
        Properties props = new Properties();
        props.put("bootstrapServers", "kafka-1:9092,kafka-2:9092,kafka-3:9092"); // reported — should be bootstrap.servers
        return props;
    }

    public Properties buildBootstrapPropsBadSnake() {
        Properties props = new Properties();
        props.setProperty("bootstrap_servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092"); // reported — should be bootstrap.servers
        return props;
    }

    public HashMap<String, Object> buildAutoCommitMapBadCamel() {
        HashMap<String, Object> props = new HashMap<>();
        props.put("enableAutoCommit", "false"); // reported — should be enable.auto.commit
        return props;
    }

    public HashMap<String, Object> buildAutoCommitMapBadSnake() {
        HashMap<String, Object> props = new HashMap<>();
        props.put("enable_auto_commit", "false"); // reported — should be enable.auto.commit
        return props;
    }

    public Map<String, Object> buildOffsetResetMapBadCamel() {
        Map<String, Object> props = new HashMap<>();
        props.put("autoOffsetReset", "earliest"); // reported — should be auto.offset.reset
        return props;
    }

    public Map<String, Object> buildOffsetResetMapBadSnake() {
        Map<String, Object> props = new HashMap<>();
        props.put("auto_offset_reset", "earliest"); // reported — should be auto.offset.reset
        return props;
    }

    public Properties buildCompressionPropsBadCamel() {
        Properties props = new Properties();
        props.put("compressionType", "snappy"); // reported — should be compression.type
        return props;
    }

    public Properties buildCompressionPropsBadSnake() {
        Properties props = new Properties();
        props.setProperty("compression_type", "snappy"); // reported — should be compression.type
        return props;
    }
}
