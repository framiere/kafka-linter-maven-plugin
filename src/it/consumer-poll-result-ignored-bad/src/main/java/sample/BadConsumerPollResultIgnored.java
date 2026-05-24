package sample;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;

/**
 * RULE: CONSUMER_POLL_RESULT_IGNORED.
 *
 * <p>Fires when {@code consumer.poll(...)} returning
 * {@code ConsumerRecords} is immediately followed by {@code POP} —
 * the returned records are silently discarded but the consumer's
 * internal {@code position} advances past them. The next {@code poll()}
 * fetches records AFTER the lost ones, and any subsequent commit
 * tells the broker the consumer processed through the advanced
 * position. Net effect: silent data loss with no error signal.
 *
 * <p>The bytecode signature is unambiguous:
 * <pre>
 *   INVOKEVIRTUAL/INVOKEINTERFACE poll(Duration)ConsumerRecords
 *   POP
 * </pre>
 *
 * <p>This shape appears most often when:
 * <ol>
 *   <li>A developer is testing a consumer setup in a unit test and
 *       writes {@code consumer.poll(Duration.ofSeconds(1));} to
 *       "just call poll once" — to verify the consumer joins the
 *       group, to log a position, etc. The throwaway test code
 *       then gets copied into production startup or never removed
 *       from the test class that runs in production.</li>
 *   <li>A developer misunderstands {@code poll()} as a heartbeat
 *       primitive and uses bare {@code consumer.poll(timeout);}
 *       calls to "keep the consumer in the group" during long
 *       processing windows. The correct heartbeat-during-stall
 *       mechanism is {@code consumer.pause(partitions)} +
 *       {@code consumer.poll(Duration.ZERO)} in a loop, or tuning
 *       {@code max.poll.interval.ms} — not silently dropping
 *       records.</li>
 *   <li>An "assignment priming" pattern is misremembered: the
 *       correct shape is {@code consumer.poll(Duration.ZERO);
 *       consumer.seek(partition, offset);} where the discarded
 *       poll triggers the group join and partition assignment
 *       BEFORE any records exist; the explicit {@code seek()} then
 *       rewinds. Without the {@code seek()}, the discarded records
 *       are simply lost.</li>
 * </ol>
 *
 * <p>The rule's detection is tight: it matches only the
 * {@code INVOKE poll(...) returning ConsumerRecords} immediately
 * followed by {@code POP} (skipping LineNumberNode / LabelNode /
 * FrameNode trivia). It does NOT match
 * {@code var records = consumer.poll(...)} (consumed by ASTORE),
 * {@code consumer.poll(...).iterator()} (consumed by INVOKEINTERFACE),
 * {@code consumer.poll(...).isEmpty()} (same), or
 * {@code return consumer.poll(...);} (consumed by ARETURN).
 */
public final class BadConsumerPollResultIgnored {

    /** Anti-pattern #1: bare poll(Duration) statement — FIRES. */
    public void primeAssignmentWithoutSeek(KafkaConsumer<String, String> consumer) {
        consumer.poll(Duration.ofSeconds(1)); // FIRES — records discarded, position advances
    }

    /** Anti-pattern #2: bare poll(Duration.ZERO) statement WITHOUT seek — FIRES. */
    public void primeAssignmentZeroTimeoutWithoutSeek(KafkaConsumer<String, String> consumer) {
        consumer.poll(Duration.ZERO); // FIRES — same shape, same loss
    }

    /** Anti-pattern #3: bare poll inside a loop — every iteration discards records — FIRES. */
    public void heartbeatLoop(KafkaConsumer<String, String> consumer) {
        for (int i = 0; i < 10; i++) {
            consumer.poll(Duration.ofMillis(100)); // FIRES — each iteration loses a batch
        }
    }

    /** Control #1: result stored in local — must NOT fire. */
    public void processBatch(KafkaConsumer<String, String> consumer) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> r : records) {
            System.out.println(r.value());
        }
    }

    /** Control #2: result iterated directly (chained) — must NOT fire. */
    public void iterateInline(KafkaConsumer<String, String> consumer) {
        for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofSeconds(1))) {
            System.out.println(r.value());
        }
    }

    /** Control #3: result returned from method — must NOT fire. */
    public ConsumerRecords<String, String> fetchAndReturn(KafkaConsumer<String, String> consumer) {
        return consumer.poll(Duration.ofSeconds(1));
    }

    /** Control #4: result chained into method call — must NOT fire. */
    public int countFetched(KafkaConsumer<String, String> consumer) {
        return consumer.poll(Duration.ofSeconds(1)).count();
    }
}
