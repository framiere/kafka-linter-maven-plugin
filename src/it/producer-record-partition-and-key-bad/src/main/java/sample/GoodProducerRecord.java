package sample;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Silent: every ProducerRecord constructor call below uses an
 * overload whose method descriptor does NOT start with
 *   (Ljava/lang/String;Ljava/lang/Integer;...
 * — so the rule's descriptor-prefix check abstains.
 *
 * Kafka 3.7's ProducerRecord exposes exactly six public
 * constructors:
 *   (a) (String topic, V value)                                  — silent
 *   (b) (String topic, K key, V value)                           — silent
 *   (c) (String topic, Integer partition, K key, V value)        — FIRES
 *   (d) (String topic, Integer partition, K key, V value,
 *        Iterable&lt;Header&gt; headers)                              — FIRES
 *   (e) (String topic, Integer partition, Long timestamp,
 *        K key, V value)                                         — FIRES
 *   (f) (String topic, Integer partition, Long timestamp,
 *        K key, V value, Iterable&lt;Header&gt; headers)               — FIRES
 *
 * Note: there is NO overload taking headers without also taking a
 * partition, and NO overload taking a timestamp without also taking
 * a partition. So the only ways to construct a partition-free
 * ProducerRecord are the 2-arg and 3-arg forms below — if a caller
 * wants headers on a key-routed record, they construct with the
 * 3-arg ctor and then mutate via `record.headers().add(...)`. This
 * is the canonical pattern and is shown in Shape C.
 *
 * These are the canonical good-citizen shapes: let the partitioner
 * route by key (Murmur2 hash modulo partition count, or the
 * uniform-sticky adaptive partitioner from KIP-794). The producer
 * does the partition-choice work and the application code is
 * insulated from partition-count changes, broker degradation, and
 * the subtle invariants of key-based ordering / compaction.
 */
public class GoodProducerRecord {

    private final Producer<String, String> producer;

    public GoodProducerRecord(Producer<String, String> producer) {
        this.producer = producer;
    }

    /**
     * Shape A: 2-arg ProducerRecord(topic, value). No key, no
     * partition — partitioner uses its round-robin / sticky
     * heuristic to spread records across partitions. Descriptor:
     *   (Ljava/lang/String;Ljava/lang/Object;)V
     * — the second arg is Object (the value), not Integer. Rule
     * abstains.
     *
     * Use case: telemetry / log streams where per-key ordering does
     * not matter and the goal is even load distribution.
     */
    public void sendKeyless(String value) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>("orders", value);
        producer.send(record);
    }

    /**
     * Shape B: 3-arg ProducerRecord(topic, key, value). The
     * canonical "key drives placement" form — partitioner hashes the
     * key and routes the record. Same key → same partition; ordering
     * by key is preserved across the topic's lifetime so long as the
     * partition count is stable. Descriptor:
     *   (Ljava/lang/String;Ljava/lang/Object;Ljava/lang/Object;)V
     * — the second arg is Object (the key), not Integer. Rule
     * abstains.
     *
     * This is the form 95% of producer call sites should use.
     */
    public void sendWithKey(String key, String value) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>("orders", key, value);
        producer.send(record);
    }

    /**
     * Shape C: 3-arg ctor + post-construction header mutation. The
     * Kafka API exposes no constructor for "key + value + headers"
     * without a partition slot — but `record.headers()` returns a
     * mutable Headers instance, so headers can be added after
     * construction. Descriptor of the ctor INVOKESPECIAL:
     *   (Ljava/lang/String;Ljava/lang/Object;Ljava/lang/Object;)V
     * — second arg is Object (the key), not Integer. Rule abstains.
     *
     * Use case: key-routed records that need orthogonal metadata
     * (trace id, schema id, content type). This is the canonical
     * shape for "I want headers AND key-based routing".
     */
    public void sendWithKeyAndHeaders(String key, String value) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>("orders", key, value);
        record.headers().add("trace-id", "abc-123".getBytes());
        producer.send(record);
    }
}
