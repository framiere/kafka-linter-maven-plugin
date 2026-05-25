package sample;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import java.util.Optional;

/**
 * RULE: CONSUMER_RECORD_LEGACY_CHECKSUM_CTOR_DEPRECATED.
 *
 * <p>Exercises four shapes the rule must catch — each constructor
 * descriptor contains either the {@code TimestampType;J} fragment
 * (primitive long checksum) or the {@code TimestampType;Ljava/lang/Long;}
 * fragment (boxed Long checksum):
 *
 * <ol>
 *   <li>Direct {@code INVOKESPECIAL} on the 10-arg
 *       {@code (String, int, long, long, TimestampType, long, int, int, K, V)}
 *       constructor — primitive {@code long} checksum slot.</li>
 *   <li>Direct {@code INVOKESPECIAL} on the 12-arg
 *       {@code (String, int, long, long, TimestampType, Long, int, int, K, V, Headers, Optional<Integer>)}
 *       constructor — boxed {@code Long} checksum slot.</li>
 *   <li>{@code INVOKEDYNAMIC} constructor-ref {@code ConsumerRecord::new}
 *       resolved against a custom 10-arg {@code @FunctionalInterface}
 *       whose SAM signature matches the deprecated primitive-checksum
 *       constructor. User-class bytecode contains ZERO direct
 *       {@code INVOKESPECIAL} on the legacy constructor; the call lives
 *       only in the {@code LambdaMetafactory}-synthesized bridge.</li>
 *   <li>{@code INVOKEDYNAMIC} constructor-ref {@code ConsumerRecord::new}
 *       resolved against a custom 12-arg {@code @FunctionalInterface}
 *       whose SAM matches the deprecated boxed-Long checksum
 *       constructor — descriptor discrimination must keep this distinct
 *       from the non-deprecated 11-arg constructor without checksum.</li>
 * </ol>
 *
 * <h2>Why test scaffolding hits this</h2>
 *
 * <p>Parameterized-test sources, fuzz-record generators, and replay
 * tooling commonly factor record construction into a
 * {@code ConsumerRecordFactory} functional interface so that test
 * skeletons can vary one field per test case. When the factory SAM is
 * defined with the full constructor parameter list (so callers can
 * inject any field), {@code ConsumerRecord::new} silently binds to
 * whichever overload matches the arity — which, for any factory
 * accepting a checksum, is the deprecated constructor. Tests pass
 * because synthetic checksums look fine to {@code record.checksum()};
 * production-shaped records (with KIP-320 leaderEpoch and KIP-82
 * headers) drift further from the synthetic shape every Kafka release.
 */
public final class BadConsumerRecordLegacyChecksumCtor {

    @FunctionalInterface
    interface CR10Factory {
        ConsumerRecord<String, byte[]> make(String topic, int partition, long offset,
                                            long timestamp, TimestampType timestampType,
                                            long checksum, int keySize, int valueSize,
                                            String key, byte[] value);
    }

    @FunctionalInterface
    interface CR12Factory {
        ConsumerRecord<String, byte[]> make(String topic, int partition, long offset,
                                            long timestamp, TimestampType timestampType,
                                            Long checksum, int keySize, int valueSize,
                                            String key, byte[] value,
                                            Headers headers, Optional<Integer> leaderEpoch);
    }

    @SuppressWarnings("deprecation")
    public ConsumerRecord<String, byte[]> buildLegacyLongChecksum() {
        // FIRES — direct INVOKESPECIAL on the 10-arg primitive-long
        // checksum constructor. Descriptor contains TimestampType;J.
        return new ConsumerRecord<>(
                "orders", 0, 42L, 1_700_000_000_000L, TimestampType.CREATE_TIME,
                123456789L, 4, 16, "key-1", new byte[]{1, 2, 3, 4});
    }

    @SuppressWarnings("deprecation")
    public ConsumerRecord<String, byte[]> buildLegacyBoxedChecksum() {
        // FIRES — direct INVOKESPECIAL on the 12-arg boxed-Long checksum
        // constructor. Descriptor contains TimestampType;Ljava/lang/Long;.
        // The explicit Long.valueOf disambiguates from the primitive-long
        // 10-arg overload.
        return new ConsumerRecord<>(
                "orders", 0, 42L, 1_700_000_000_000L, TimestampType.CREATE_TIME,
                Long.valueOf(123456789L), 4, 16, "key-1", new byte[]{1, 2, 3, 4},
                new RecordHeaders(), Optional.of(7));
    }

    @SuppressWarnings("deprecation")
    public CR10Factory capturedLegacyLongChecksumFactory() {
        // FIRES — INVOKEDYNAMIC constructor-ref capture. The SAM arity
        // (10) and parameter types (..., primitive long checksum, ...)
        // resolve `ConsumerRecord::new` to the deprecated 10-arg
        // primitive-long constructor. The bsm-arg handle's descriptor
        // contains TimestampType;J and matches the rule.
        return ConsumerRecord::new;
    }

    @SuppressWarnings("deprecation")
    public CR12Factory capturedLegacyBoxedChecksumFactory() {
        // FIRES — INVOKEDYNAMIC constructor-ref capture. The SAM arity
        // (12) and parameter types (..., boxed Long checksum, ...,
        // Headers, Optional) resolve `ConsumerRecord::new` to the
        // deprecated 12-arg boxed-Long constructor. The bsm-arg handle's
        // descriptor contains TimestampType;Ljava/lang/Long;.
        return ConsumerRecord::new;
    }
}
