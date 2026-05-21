package sample;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * RULE: CONSUMER_SUBSCRIBE_IN_LOOP.
 *
 * Fires when {@code consumer.subscribe(...)} or {@code consumer.assign(...)}
 * is invoked from inside a loop body. Each iteration triggers a coordinator
 * rebalance (or assignment reset) — the group never converges, lag grows,
 * the broker rebalance throttle activates.
 */
public final class BadConsumerSubscribeInLoop {

    private KafkaConsumer<String, String> consumer(Properties p) {
        return new KafkaConsumer<>(p);
    }

    /** Anti-pattern: subscribe() inside the poll loop. */
    public void resubscribePerPoll(Properties p) {
        KafkaConsumer<String, String> c = consumer(p);
        while (true) {                              // poll loop
            c.subscribe(List.of("orders"));         // FIRES — subscribe in loop
            ConsumerRecords<String, String> rs = c.poll(Duration.ofMillis(100));
            if (rs.isEmpty()) break;
        }
        c.close();
    }

    /** Anti-pattern: subscribe() inside a for loop iterating topic names. */
    public void resubscribePerTopic(Properties p, List<String> topics) {
        KafkaConsumer<String, String> c = consumer(p);
        for (String t : topics) {                   // for-each loop
            c.subscribe(List.of(t));                // FIRES — subscribe in loop
        }
        c.close();
    }

    /** Anti-pattern: assign() inside a loop — same lifecycle reset semantics. */
    public void reassignPerIteration(Properties p, List<TopicPartition> tps) {
        KafkaConsumer<String, String> c = consumer(p);
        for (TopicPartition tp : tps) {             // for-each loop
            c.assign(List.of(tp));                  // FIRES — assign in loop
        }
        c.close();
    }

    /** Anti-pattern: subscribe inside an iterating-lambda body (forEach on a Stream). */
    public void resubscribeFromStream(Properties p, List<String> topics) {
        KafkaConsumer<String, String> c = consumer(p);
        topics.forEach(t -> c.subscribe(List.of(t))); // FIRES — iterating lambda body
        c.close();
    }

    /** Control: subscribe() called exactly once before the loop. Must NOT fire. */
    public void subscribeOnceThenPoll(Properties p) {
        KafkaConsumer<String, String> c = consumer(p);
        c.subscribe(List.of("orders")); // OK — outside the loop
        while (true) {
            ConsumerRecords<String, String> rs = c.poll(Duration.ofMillis(100));
            if (rs.isEmpty()) break;
        }
        c.close();
    }
}
