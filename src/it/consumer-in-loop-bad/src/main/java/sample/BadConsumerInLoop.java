package sample;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * RULE: CONSUMER_IN_LOOP.
 *
 * <p>Fires when {@code new KafkaConsumer(...)} appears inside any
 * iteration context — a classic {@code for}/{@code while} loop, OR
 * an iterating lambda body. This is structurally the same shape as
 * PRODUCER_IN_LOOP, but the production failure mode is materially
 * different and considerably worse — instantiating a KafkaConsumer
 * in a loop doesn't just waste resources locally, it triggers a
 * REBALANCE STORM that pauses every other consumer in the group.
 *
 * <p>What construction of a KafkaConsumer actually does:
 * <ol>
 *   <li><b>Join the consumer group.</b> The consumer sends a
 *       {@code JoinGroupRequest} to the group coordinator broker.
 *       The coordinator collects all current members, then triggers
 *       a REBALANCE — every existing member is asked to revoke its
 *       partitions, the coordinator computes a new assignment, and
 *       every member receives its new partitions in a
 *       {@code SyncGroupResponse}.</li>
 *   <li><b>Coordinator handshake.</b> {@code FindCoordinatorRequest},
 *       multiple round-trips to discover and connect to the
 *       coordinator broker.</li>
 *   <li><b>Fetcher initialization.</b> Open TCP connections to every
 *       leader for every assigned partition, send initial
 *       {@code ListOffsetsRequest} to determine starting position,
 *       and seek to the committed offset.</li>
 *   <li><b>Heartbeat thread startup.</b> Daemon thread named
 *       "kafka-consumer-heartbeat-thread" begins sending
 *       {@code HeartbeatRequest} every {@code heartbeat.interval.ms}
 *       (default 3s).</li>
 * </ol>
 *
 * <p>Each of those steps is expensive on its own. The catastrophic
 * cost, however, is the REBALANCE — and the rebalance is the
 * one cost that affects OTHER applications in the group, not just
 * the misbehaving one:
 * <ul>
 *   <li>A group with N consumers and one in-loop consumer constructor
 *       generates ONE rebalance per loop iteration. Every iteration
 *       pauses ALL N consumers (including the in-loop one) while
 *       the rebalance completes — typically 1–10 seconds.</li>
 *   <li>During the rebalance, NO records are processed. A 10-iteration
 *       loop in a group of 100 consumers can pause the entire group
 *       for 100 seconds of cumulative wall-clock — lag spikes by
 *       millions of records.</li>
 *   <li>The group coordinator broker sees a JoinGroup/SyncGroup
 *       request storm. On a multi-tenant cluster, this saturates
 *       the broker's group-coordinator thread pool and can starve
 *       other consumer groups whose coordinator happens to be the
 *       same broker.</li>
 * </ul>
 *
 * <p>What it looks like in production:
 * <ol>
 *   <li>An ETL job processes a batch of topics: "for each topic in
 *       the input set, spin up a consumer, drain it, close it." The
 *       constructor sits inside the {@code for (topic : topics)} loop
 *       and try-with-resources cleans up after each iteration.</li>
 *   <li>Result: every iteration triggers a rebalance, every other
 *       consumer on the same group-id sees its partitions revoked
 *       and reassigned. The consumer lag dashboard goes red for the
 *       entire group, not just for this app.</li>
 *   <li>Discovery: broker logs show GroupCoordinator entries for the
 *       offending group at high rate; consumer-side metrics show
 *       repeated rebalances and zero records-consumed during them.</li>
 * </ol>
 *
 * <p>What the rule catches: every {@code NEW org/apache/kafka/clients/consumer/KafkaConsumer}
 * instruction whose containing instruction is inside a back-edge
 * (classic loop) or inside a method body classified as an
 * iterating-lambda body ({@code forEach}, stream operations).
 *
 * <p>The implementation is the {@code ProducerInLoopRule} generic
 * detector parameterized with target type {@code KafkaConsumer} —
 * the same code that catches PRODUCER_IN_LOOP, with a different
 * target.
 *
 * <p>Correct pattern: instantiate ONCE for the lifetime of the
 * application, share across the iteration:
 * <pre>{@code
 *   try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
 *       consumer.subscribe(allTopics);
 *       while (running) {
 *           ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
 *           processRecords(recs);
 *       }
 *   }
 * }</pre>
 */
public final class BadConsumerInLoop {

    private static final String BROKERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092";

    private static Properties props() {
        Properties p = new Properties();
        p.put("bootstrap.servers", BROKERS);
        p.put("group.id", "batch-etl");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return p;
    }

    /** Anti-pattern: new KafkaConsumer inside a classic for-loop — FIRES. */
    public void newConsumerInForLoop(List<String> topics) {
        for (String topic : topics) {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props())) { // FIRES
                consumer.subscribe(List.of(topic));
                ConsumerRecords<String, String> ignored = consumer.poll(Duration.ofMillis(500));
            }
        }
    }

    /** Anti-pattern: new KafkaConsumer inside a while-loop — FIRES. */
    public void newConsumerInWhileLoop(int iterations) {
        int i = 0;
        while (i < iterations) {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props())) { // FIRES
                consumer.subscribe(List.of("events"));
                ConsumerRecords<String, String> ignored = consumer.poll(Duration.ofMillis(500));
            }
            i++;
        }
    }

    /** Anti-pattern: new KafkaConsumer inside a forEach iterating lambda — FIRES. */
    public void newConsumerInForEachLambda(List<String> topics) {
        topics.forEach(topic -> {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props())) { // FIRES
                consumer.subscribe(List.of(topic));
                ConsumerRecords<String, String> ignored = consumer.poll(Duration.ofMillis(500));
            }
        });
    }

    /** Control: consumer instantiated ONCE outside the loop, reused — must NOT fire. */
    public void consumerInstantiatedOutsideLoop(List<String> topics) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props())) {
            consumer.subscribe(topics);
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<String, String> ignored = consumer.poll(Duration.ofMillis(500));
            }
        }
    }
}
