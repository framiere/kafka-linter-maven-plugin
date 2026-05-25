package sample;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import java.util.Optional;

/**
 * RULE: CONSUMER_RECORD_LEGACY_CHECKSUM_CTOR_DEPRECATED — must NOT fire.
 *
 * <p>The four methods below exercise the supported non-deprecated
 * {@code ConsumerRecord} constructor surface:
 *
 * <ol>
 *   <li>The 5-arg shortcut
 *       {@code (String, int, long, K, V)} — descriptor does NOT contain
 *       {@code TimestampType} at all, so neither legacy fragment can
 *       appear.</li>
 *   <li>The 11-arg modern constructor
 *       {@code (String, int, long, long, TimestampType, int, int, K, V, Headers, Optional<Integer>)}
 *       — descriptor contains {@code TimestampType;I} (int keySize follows
 *       directly), NOT {@code TimestampType;J} or
 *       {@code TimestampType;Ljava/lang/Long;}, so the rule's descriptor
 *       filter rejects this site.</li>
 *   <li>{@code INVOKEDYNAMIC} constructor-ref capture against a 5-arg
 *       custom {@code @FunctionalInterface} resolving to the shortcut
 *       constructor.</li>
 *   <li>{@code INVOKEDYNAMIC} constructor-ref capture against an 11-arg
 *       custom {@code @FunctionalInterface} resolving to the modern
 *       constructor. The bsm-arg handle's descriptor contains
 *       {@code TimestampType;I} — descriptor discrimination rejects this
 *       site even though the name {@code <init>} matches.</li>
 * </ol>
 *
 * <h2>Why this is safe</h2>
 *
 * <p>The non-deprecated 11-arg constructor surfaces both
 * {@link org.apache.kafka.common.header.Headers} (KIP-82 record headers
 * — tracing context, schema-id propagation, transactional metadata) and
 * {@code Optional<Integer>} {@code leaderEpoch} (KIP-320 — the broker
 * epoch that wrote the record, used by KIP-360 consumer-side log
 * truncation detection). Test scaffolding built on this constructor
 * produces records that match the broker-produced shape; any downstream
 * leader-epoch reset or header-driven router behaves identically against
 * synthetic and real records.
 *
 * <p>The rule's descriptor filter on {@code <init>} matches only
 * descriptors containing {@code TimestampType;J} or
 * {@code TimestampType;Ljava/lang/Long;}; the modern constructors have
 * {@code TimestampType;I} (followed by primitive int keySize) so they
 * never fire — descriptor discrimination is mandatory because
 * {@code <init>} is overloaded and a name-only match would false-positive
 * on the supported migration target.
 */
public final class GoodConsumerRecordModernCtor {

    @FunctionalInterface
    interface CR5Factory {
        ConsumerRecord<String, byte[]> make(String topic, int partition, long offset,
                                            String key, byte[] value);
    }

    @FunctionalInterface
    interface CR11ModernFactory {
        ConsumerRecord<String, byte[]> make(String topic, int partition, long offset,
                                            long timestamp, TimestampType timestampType,
                                            int keySize, int valueSize,
                                            String key, byte[] value,
                                            Headers headers, Optional<Integer> leaderEpoch);
    }

    public ConsumerRecord<String, byte[]> buildShortcut() {
        // DOES NOT FIRE — 5-arg shortcut constructor. Descriptor does
        // NOT contain TimestampType, so neither legacy fragment matches.
        return new ConsumerRecord<>("orders", 0, 42L, "key-1", new byte[]{1, 2, 3, 4});
    }

    public ConsumerRecord<String, byte[]> buildModernFull() {
        // DOES NOT FIRE — 11-arg modern constructor without checksum.
        // Descriptor contains TimestampType;II (int keySize follows the
        // TimestampType arg). The rule's filter matches only ;J or
        // ;Ljava/lang/Long; — int does not appear in either pattern.
        return new ConsumerRecord<>(
                "orders", 0, 42L, 1_700_000_000_000L, TimestampType.CREATE_TIME,
                4, 16, "key-1", new byte[]{1, 2, 3, 4},
                new RecordHeaders(), Optional.of(7));
    }

    public CR5Factory capturedShortcutFactory() {
        // DOES NOT FIRE — INVOKEDYNAMIC constructor-ref capture targeting
        // the SUPPORTED 5-arg shortcut. The bsmArg handle's descriptor is
        // (Ljava/lang/String;IJLjava/lang/Object;Ljava/lang/Object;)V —
        // no TimestampType, no legacy fragment.
        return ConsumerRecord::new;
    }

    public CR11ModernFactory capturedModernFactory() {
        // DOES NOT FIRE — INVOKEDYNAMIC constructor-ref capture targeting
        // the SUPPORTED 11-arg modern constructor. The bsmArg handle's
        // descriptor contains TimestampType;II (int keySize follows), so
        // the rule's descriptor filter rejects this site even though the
        // name <init> matches. This is precisely why descriptor
        // discrimination is mandatory.
        return ConsumerRecord::new;
    }
}
