package sample;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.List;

/**
 * RULE: PRODUCER_RECORD_PARTITION_AND_KEY — per-call-site bytecode rule.
 *
 * The rule fires once per call site where the JVM emits
 *   INVOKESPECIAL org/apache/kafka/clients/producer/ProducerRecord.<init>
 * with a method descriptor that STARTS WITH
 *   (Ljava/lang/String;Ljava/lang/Integer;...
 * — i.e., the second constructor argument is the boxed Integer
 * partition. This descriptor prefix covers the four
 * partition-bearing ProducerRecord overloads:
 *
 *   1. ProducerRecord(String topic, Integer partition, K key, V value)
 *      desc: (Ljava/lang/String;Ljava/lang/Integer;Ljava/lang/Object;Ljava/lang/Object;)V
 *
 *   2. ProducerRecord(String topic, Integer partition, K key, V value,
 *                     Iterable&lt;Header&gt; headers)
 *      desc: (Ljava/lang/String;Ljava/lang/Integer;Ljava/lang/Object;Ljava/lang/Object;Ljava/lang/Iterable;)V
 *
 *   3. ProducerRecord(String topic, Integer partition, Long timestamp,
 *                     K key, V value)
 *      desc: (Ljava/lang/String;Ljava/lang/Integer;Ljava/lang/Long;Ljava/lang/Object;Ljava/lang/Object;)V
 *
 *   4. ProducerRecord(String topic, Integer partition, Long timestamp,
 *                     K key, V value, Iterable&lt;Header&gt; headers)
 *      desc: (Ljava/lang/String;Ljava/lang/Integer;Ljava/lang/Long;Ljava/lang/Object;Ljava/lang/Object;Ljava/lang/Iterable;)V
 *
 * Why this matters semantically:
 *
 *   - Kafka's partitioner uses the record KEY (when present) to
 *     decide partition placement. The contract `same key → same
 *     partition` is what makes per-key ORDERING work: every record
 *     for `userId=42` lands on the same partition, the broker
 *     appends them to that partition's log in arrival order, and a
 *     single consumer in the consumer group reads them in that
 *     order.
 *
 *   - When you pass an explicit Integer partition, the partitioner
 *     is BYPASSED — the producer routes the record to that exact
 *     partition regardless of the key. This silently breaks:
 *       (a) Key-based ordering: future records for the same key
 *           may land on a DIFFERENT partition if the call site is
 *           changed or computes the partition number from a value
 *           that drifts (e.g., shard id, hash function, modulo a
 *           changed partition count).
 *       (b) Log compaction by key: compaction is a per-partition
 *           operation that retains the LATEST value for each key
 *           WITHIN a partition. If "the same logical key" is split
 *           across partitions because some writers picked partition
 *           explicitly and others let the partitioner choose, the
 *           old values never get compacted away.
 *       (c) Adaptive partitioning (KIP-794, Kafka 3.3+): the
 *           "uniform sticky" partitioner steers traffic away from
 *           slow brokers. Explicit-partition sends opt out of this
 *           — under broker degradation they keep hammering the
 *           degraded broker while keyless sends drain to healthy
 *           ones.
 *
 * Legitimate uses (replay tooling that must reproduce a historical
 * partition assignment, admin scripts that intentionally target one
 * partition for a backfill, controlled fan-out where placement is
 * proven externally) exist — but they're rare enough that the rule
 * defaults to flagging the shape and asking the developer to
 * suppress per-class when intentional.
 *
 * Each method below contains exactly one Bad call site → one fire.
 * Total: 4 violations in this class.
 */
public class BadProducerRecordPartitionAndKey {

    private final Producer<String, String> producer;

    public BadProducerRecordPartitionAndKey(Producer<String, String> producer) {
        this.producer = producer;
    }

    /**
     * Shape 1: 4-arg ProducerRecord(topic, partition, key, value).
     * Source: developer computed a partition number externally
     * (e.g., from a shard table) and is passing it directly.
     * Bytecode: INVOKESPECIAL ProducerRecord.&lt;init&gt; with desc
     *   (Ljava/lang/String;Ljava/lang/Integer;Ljava/lang/Object;Ljava/lang/Object;)V
     * The key arg is still present but no longer drives placement —
     * exactly the silent breakage the rule catches.
     */
    public void sendWithExplicitPartition4Arg(int partitionNumber, String key, String value) {
        // FIRES: 4-arg ctor with explicit Integer partition.
        ProducerRecord<String, String> record =
                new ProducerRecord<>("orders", partitionNumber, key, value);
        producer.send(record);
    }

    /**
     * Shape 2: 5-arg ProducerRecord(topic, partition, key, value,
     * headers). Common when the caller adds tracing or schema-id
     * headers AND has its own partition logic — the headers are
     * orthogonal but the explicit partition is the violation.
     * Bytecode descriptor:
     *   (Ljava/lang/String;Ljava/lang/Integer;Ljava/lang/Object;Ljava/lang/Object;Ljava/lang/Iterable;)V
     */
    public void sendWithExplicitPartition5ArgHeaders(int partitionNumber, String key, String value) {
        List<Header> headers = List.of(
                new RecordHeader("trace-id", "abc-123".getBytes()));
        // FIRES: 5-arg ctor (partition + headers) with explicit Integer partition.
        ProducerRecord<String, String> record =
                new ProducerRecord<>("orders", partitionNumber, key, value, headers);
        producer.send(record);
    }

    /**
     * Shape 3: 5-arg ProducerRecord(topic, partition, timestamp,
     * key, value). The replay / backfill shape: the writer is
     * reproducing a historical event and wants both the original
     * timestamp AND the original partition assignment. The
     * descriptor distinguishes this from Shape 2 by Long instead of
     * Iterable in the third slot:
     *   (Ljava/lang/String;Ljava/lang/Integer;Ljava/lang/Long;Ljava/lang/Object;Ljava/lang/Object;)V
     * If this is a legitimate replay tool, the developer should
     * silence the rule per-class with a severity override (see
     * sibling Good class shapes).
     */
    public void sendWithExplicitPartition5ArgTimestamp(int partitionNumber, long timestamp,
                                                       String key, String value) {
        // FIRES: 5-arg ctor (partition + timestamp) with explicit Integer partition.
        ProducerRecord<String, String> record =
                new ProducerRecord<>("orders", partitionNumber, timestamp, key, value);
        producer.send(record);
    }

    /**
     * Shape 4: 6-arg ProducerRecord(topic, partition, timestamp,
     * key, value, headers). The "full" overload — replay tooling
     * that needs every dimension of the original record. Descriptor:
     *   (Ljava/lang/String;Ljava/lang/Integer;Ljava/lang/Long;Ljava/lang/Object;Ljava/lang/Object;Ljava/lang/Iterable;)V
     * This is the form most likely to be a TRUE positive: anyone
     * needing this much control over placement and timestamping
     * should know they've opted out of the partitioner's
     * key-routing contract.
     */
    public void sendWithExplicitPartition6Arg(int partitionNumber, long timestamp,
                                              String key, String value) {
        List<Header> headers = List.of(
                new RecordHeader("trace-id", "abc-123".getBytes()),
                new RecordHeader("schema-id", new byte[] {0, 0, 0, 1}));
        // FIRES: 6-arg ctor (partition + timestamp + headers) with explicit Integer partition.
        ProducerRecord<String, String> record =
                new ProducerRecord<>("orders", partitionNumber, timestamp, key, value, headers);
        producer.send(record);
    }
}
