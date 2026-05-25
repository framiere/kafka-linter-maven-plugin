package sample;

import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Uuid;

/**
 * RULE: ADMIN_TOPIC_LISTING_NAME_INTERNAL_CTOR_DEPRECATED — must NOT fire.
 *
 * <p>The two methods below exercise the supported 3-arg modern
 * {@code TopicListing(String name, Uuid topicId, boolean isInternal)}
 * constructor:
 *
 * <ol>
 *   <li>Direct {@code INVOKESPECIAL} on the 3-arg constructor whose
 *       descriptor is
 *       {@code (Ljava/lang/String;Lorg/apache/kafka/common/Uuid;Z)V}. The
 *       rule's filter matches the legacy descriptor
 *       {@code (Ljava/lang/String;Z)V} exactly, so the 3-arg descriptor
 *       — which differs by a {@code Uuid} parameter in the middle slot
 *       — never matches.</li>
 *   <li>{@code INVOKEDYNAMIC} constructor-ref capture
 *       {@code TopicListing::new} resolved against a custom 3-arg
 *       {@code @FunctionalInterface} whose SAM matches the modern
 *       constructor. The bsm-arg handle's descriptor is
 *       {@code (Ljava/lang/String;Lorg/apache/kafka/common/Uuid;Z)V} —
 *       not the legacy {@code (Ljava/lang/String;Z)V}, so the rule's
 *       descriptor filter rejects the site even though the name
 *       {@code <init>} and the owner {@code TopicListing} both match.</li>
 * </ol>
 *
 * <h2>Why this is safe</h2>
 *
 * <p>The non-deprecated 3-arg constructor takes
 * {@code (String name, Uuid topicId, boolean isInternal)}. Test fixtures
 * that genuinely cannot supply a broker-assigned UUID should pass
 * {@code Uuid.ZERO_UUID} <em>explicitly</em> — that surfaces the intent
 * at the call site (the fixture is acknowledging it produces a
 * pre-Kafka-2.8 sentinel value), lets code reviewers flag fixtures that
 * should be using a real UUID, and keeps the constructor-resolution
 * unambiguous against the deprecated 2-arg overload.
 *
 * <p>Descriptor discrimination is mandatory here because {@code <init>} is
 * overloaded on {@code TopicListing}: a name-only filter would
 * false-positive on the supported migration target. Java has no built-in
 * functional interface for {@code (String, Uuid, boolean) -> TopicListing}
 * (the closest, {@code TriFunction}, doesn't exist in {@code java.util.function}),
 * so a custom 3-arg SAM is required to express the constructor-ref capture
 * shape — which is itself a realistic shape in parameterized-test sources
 * and inventory-reconciler test harnesses.
 */
public final class GoodTopicListingWithUuid {

    @FunctionalInterface
    interface TL3Factory {
        TopicListing make(String name, Uuid topicId, boolean isInternal);
    }

    public TopicListing buildModern() {
        // DOES NOT FIRE — 3-arg modern constructor. Descriptor:
        // (Ljava/lang/String;Lorg/apache/kafka/common/Uuid;Z)V.
        // Uuid.ZERO_UUID is passed explicitly so the fixture's intent
        // (pre-2.8 sentinel, no broker-assigned UUID available) is
        // visible at the call site.
        return new TopicListing("orders", Uuid.ZERO_UUID, false);
    }

    public TL3Factory capturedModernFactory() {
        // DOES NOT FIRE — INVOKEDYNAMIC constructor-ref capture targeting
        // the SUPPORTED 3-arg modern constructor. The bsm-arg handle's
        // descriptor is (Ljava/lang/String;Lorg/apache/kafka/common/Uuid;Z)V,
        // not the legacy (Ljava/lang/String;Z)V, so the rule's descriptor
        // filter rejects this site. This is precisely why descriptor
        // discrimination is mandatory: the name <init> and owner
        // TopicListing both match, but the descriptor difference protects
        // the supported migration target.
        return TopicListing::new;
    }
}
