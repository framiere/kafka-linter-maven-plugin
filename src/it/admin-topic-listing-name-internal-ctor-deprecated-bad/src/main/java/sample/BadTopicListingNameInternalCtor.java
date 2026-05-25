package sample;

import org.apache.kafka.clients.admin.TopicListing;

import java.util.function.BiFunction;

/**
 * RULE: ADMIN_TOPIC_LISTING_NAME_INTERNAL_CTOR_DEPRECATED — must fire on both
 * methods.
 *
 * <p>The two methods below exercise the two distinct bytecode shapes the rule
 * is required to catch for the deprecated 2-arg
 * {@code TopicListing(String name, boolean isInternal)} constructor:
 *
 * <ol>
 *   <li>Direct {@code INVOKESPECIAL} on the 2-arg constructor — the classic
 *       call site, where the user-class bytecode contains an explicit
 *       {@code NEW org/apache/kafka/clients/admin/TopicListing} followed by
 *       {@code INVOKESPECIAL <init>(Ljava/lang/String;Z)V}.</li>
 *   <li>{@code INVOKEDYNAMIC} constructor-ref capture
 *       {@code TopicListing::new} resolved against
 *       {@code BiFunction<String, Boolean, TopicListing>} — javac compiles
 *       the constructor-ref through {@code LambdaMetafactory} into an
 *       {@code INVOKEDYNAMIC} site whose bsm-args contain a
 *       {@code REF_newInvokeSpecial} handle pointing at
 *       {@code TopicListing.<init>(Ljava/lang/String;Z)V}. The user-class
 *       bytecode contains ZERO direct {@code INVOKESPECIAL} on the legacy
 *       ctor at this site — a name-only MethodInsnNode walk misses it
 *       entirely. The rule's bsm-arg walk via
 *       {@code AsmUtil.indyTargetHandle} catches this case.</li>
 * </ol>
 *
 * <h2>Why this constructor is deprecated</h2>
 *
 * <p>{@code TopicListing} is the row type returned by
 * {@code Admin.listTopics()} — one entry per topic the caller can see,
 * carrying the topic's name, its {@code Uuid topicId}, and the
 * {@code isInternal} flag. KIP-516 (Kafka 2.8) introduced topic IDs
 * cluster-wide: every topic receives a 16-byte {@code Uuid} on creation and
 * the broker carries that UUID through every protocol that references the
 * topic. Topic IDs survive recreation: a {@code delete-then-create} of topic
 * {@code orders} produces a different topicId on the new topic, which is the
 * exact discriminator the broker uses to reject stale produces or fetches
 * against the recreated topic.
 *
 * <p>The legacy 2-arg constructor predates topic IDs and constructs a
 * {@code TopicListing} whose {@code topicId()} returns {@code Uuid.ZERO_UUID}
 * — the documented sentinel for pre-Kafka-2.8 clusters. Three consequences
 * follow for test scaffolding built on this constructor: reconcilers that
 * detect topic recreation by {@code listing.topicId().equals(persistedId)}
 * compare a real broker-assigned UUID against {@code Uuid.ZERO_UUID} from
 * the fixture and always trigger the recreated branch; routing rules that
 * treat {@code Uuid.ZERO_UUID} as a special value (legacy-broker fallback,
 * pre-create placeholder) fire on every fixture-built {@code TopicListing};
 * and INVOKEDYNAMIC {@code TopicListing::new} captures silently bind to the
 * deprecated 2-arg ctor whenever the factory SAM arity is 2.
 */
public final class BadTopicListingNameInternalCtor {

    @SuppressWarnings("deprecation")
    public TopicListing buildDirect() {
        // MUST FIRE — direct INVOKESPECIAL on the deprecated 2-arg
        // ctor (String, boolean). Descriptor: (Ljava/lang/String;Z)V.
        // The resulting TopicListing carries Uuid.ZERO_UUID for its
        // topicId — a fixture-built listing that silently triggers the
        // `topic recreated` branch of any reconciler comparing topic IDs.
        return new TopicListing("orders", false);
    }

    @SuppressWarnings("deprecation")
    public BiFunction<String, Boolean, TopicListing> capturedLegacyFactory() {
        // MUST FIRE — INVOKEDYNAMIC constructor-ref capture targeting the
        // deprecated 2-arg ctor. javac resolves `TopicListing::new` to
        // the (String, boolean) overload by SAM arity, emitting an
        // INVOKEDYNAMIC site whose bsm-args contain a REF_newInvokeSpecial
        // handle on <init>(Ljava/lang/String;Z)V. The user-class bytecode
        // here contains ZERO direct INVOKESPECIAL on the legacy ctor —
        // only the INVOKEDYNAMIC + LambdaMetafactory bridge. A name-only
        // MethodInsnNode walk misses this entirely; the rule's bsm-arg
        // walk catches it.
        return TopicListing::new;
    }
}
