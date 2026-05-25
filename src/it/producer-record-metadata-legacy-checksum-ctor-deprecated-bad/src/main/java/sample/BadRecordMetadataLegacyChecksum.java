package sample;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * RULE: PRODUCER_RECORD_METADATA_LEGACY_CHECKSUM_CTOR_DEPRECATED.
 *
 * <p>Exercises two shapes the rule must catch:
 *
 * <ol>
 *   <li>Direct {@code INVOKESPECIAL} on the 7-arg
 *       {@code (TopicPartition, long, long, long, Long, int, int)}
 *       constructor — the deprecated boxed-{@code Long} checksum
 *       overload. The descriptor's tail
 *       {@code Ljava/lang/Long;II)V} is what the rule matches; the modern
 *       6-arg constructor's tail is {@code JII)V} (no boxed Long) and is
 *       left untouched.</li>
 *   <li>{@code INVOKEDYNAMIC} constructor-ref capture
 *       {@code RecordMetadata::new} resolved against a custom 7-arg
 *       {@code @FunctionalInterface} whose SAM matches the deprecated
 *       constructor's parameter list. User-class bytecode contains ZERO
 *       direct {@code INVOKESPECIAL} on the legacy constructor; the call
 *       lives only in the {@code LambdaMetafactory}-synthesized bridge,
 *       so a name-only walk misses it.</li>
 * </ol>
 *
 * <h2>Why mock-producer scaffolding hits this</h2>
 *
 * <p>Mock-producer libraries (custom {@code MockProducer} extensions,
 * in-process broker simulators, callback-assertion harnesses) drive
 * tests by synthesizing {@code RecordMetadata} instances and handing
 * them to {@code Callback.onCompletion(...)} without a real broker
 * round-trip. When the factory functional interface is defined with the
 * full constructor parameter list (so each test can vary any field
 * including {@code checksum}), {@code RecordMetadata::new} resolves by
 * arity to the deprecated 7-arg ctor. The test asserts on
 * {@code metadata.checksum()} reading whatever the test wrote — but the
 * real producer always emits {@code -1L} for batch-attributed records,
 * so the assertion lies about production behavior.
 */
public final class BadRecordMetadataLegacyChecksum {

    @FunctionalInterface
    interface RM7Factory {
        RecordMetadata make(TopicPartition tp, long baseOffset, long batchIndex,
                            long timestamp, Long checksum,
                            int serializedKeySize, int serializedValueSize);
    }

    @SuppressWarnings("deprecation")
    public RecordMetadata buildLegacyChecksum() {
        // FIRES — direct INVOKESPECIAL on the 7-arg Long-checksum
        // constructor. Descriptor:
        // (Lorg/apache/kafka/common/TopicPartition;JJJLjava/lang/Long;II)V
        // — the tail Ljava/lang/Long;II)V is what the rule matches.
        return new RecordMetadata(
                new TopicPartition("orders", 0),
                42L,       // baseOffset
                0L,        // batchIndex
                1_700_000_000_000L, // timestamp
                Long.valueOf(123456789L), // <-- the informationless checksum slot
                4,         // serializedKeySize
                16);       // serializedValueSize
    }

    @SuppressWarnings("deprecation")
    public RM7Factory capturedLegacyChecksumFactory() {
        // FIRES — INVOKEDYNAMIC constructor-ref capture. The SAM arity
        // (7) and parameter types resolve `RecordMetadata::new` to the
        // deprecated 7-arg Long-checksum ctor. The bsm-arg handle's
        // descriptor contains the legacy Ljava/lang/Long;II)V tail and
        // matches the rule's substring filter.
        return RecordMetadata::new;
    }
}
