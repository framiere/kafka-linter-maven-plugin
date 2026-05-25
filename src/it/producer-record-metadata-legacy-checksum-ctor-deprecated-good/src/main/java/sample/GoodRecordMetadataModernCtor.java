package sample;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * RULE: PRODUCER_RECORD_METADATA_LEGACY_CHECKSUM_CTOR_DEPRECATED — must NOT fire.
 *
 * <p>The two methods below exercise the supported 6-arg modern
 * {@code RecordMetadata} constructor:
 *
 * <ol>
 *   <li>Direct {@code INVOKESPECIAL} on the 6-arg
 *       {@code (TopicPartition, long, int, long, int, int)} constructor —
 *       the modern fully-primitive ctor whose descriptor's tail
 *       {@code JII)V} does NOT contain {@code Ljava/lang/Long;II)V}, so
 *       the rule's substring filter rejects it.</li>
 *   <li>{@code INVOKEDYNAMIC} constructor-ref capture
 *       {@code RecordMetadata::new} resolved against a custom 6-arg
 *       {@code @FunctionalInterface} whose SAM matches the modern
 *       constructor. The bsm-arg handle's descriptor is
 *       {@code (Lorg/apache/kafka/common/TopicPartition;JIJII)V} — no
 *       boxed {@code Long} anywhere, so the rule's descriptor filter
 *       rejects the site even though the name {@code <init>} matches.</li>
 * </ol>
 *
 * <h2>Why this is safe</h2>
 *
 * <p>The non-deprecated 6-arg constructor takes
 * {@code (topicPartition, baseOffset, batchIndex, timestamp,
 * serializedKeySize, serializedValueSize)} — all primitive, no checksum
 * slot. Mock-producer scaffolding built on this constructor produces
 * acknowledgment metadata that matches the shape the modern producer
 * ack path actually surfaces (real producers always emit {@code -1L}
 * for the post-KIP-98 batch-attributed checksum case); test assertions
 * cannot drift from production by reading a synthetic checksum value.
 *
 * <p>The rule's descriptor filter on {@code <init>} matches only the
 * substring {@code Ljava/lang/Long;II)V}; the modern ctor's descriptor
 * tail is {@code JII)V} (primitive long, int, int — no boxed Long), so
 * the filter never matches. Descriptor discrimination is mandatory
 * because {@code <init>} is overloaded and a name-only match would
 * false-positive on the supported migration target.
 */
public final class GoodRecordMetadataModernCtor {

    @FunctionalInterface
    interface RM6Factory {
        RecordMetadata make(TopicPartition tp, long baseOffset, int batchIndex,
                            long timestamp,
                            int serializedKeySize, int serializedValueSize);
    }

    public RecordMetadata buildModern() {
        // DOES NOT FIRE — 6-arg modern constructor. Descriptor:
        // (Lorg/apache/kafka/common/TopicPartition;JIJII)V. No boxed
        // Long; the rule's filter on Ljava/lang/Long;II)V does not match.
        return new RecordMetadata(
                new TopicPartition("orders", 0),
                42L,    // baseOffset
                0,      // batchIndex (primitive int, not long)
                1_700_000_000_000L, // timestamp
                4,      // serializedKeySize
                16);    // serializedValueSize
    }

    public RM6Factory capturedModernFactory() {
        // DOES NOT FIRE — INVOKEDYNAMIC constructor-ref capture targeting
        // the SUPPORTED 6-arg modern constructor. The bsm-arg handle's
        // descriptor (Lorg/apache/kafka/common/TopicPartition;JIJII)V
        // contains no boxed Long, so the rule's descriptor filter rejects
        // this site. This is precisely why descriptor discrimination is
        // mandatory: the name <init> would match a name-only filter, but
        // the descriptor difference protects the supported migration
        // target.
        return RecordMetadata::new;
    }
}
