package sample;

import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Control for CONSUMER_SEEK_BEFORE_POLL.
 *
 * <p>Each method subscribes, then performs a {@code poll(...)} which
 * causes the group join + partition assignment to complete and
 * populates {@code assignment()}. Only AFTER that poll does the
 * method call {@code seek(...)}. The rule's state machine moves
 * SUBSCRIBED → POLLED on the poll, and seek() while POLLED is
 * silent — the violation list stays empty.
 *
 * <p>Note: a single {@code poll()} call is enough for the rule to
 * consider the consumer "ready" — even {@code poll(Duration.ZERO)}
 * would advance the state. (In real code that wouldn't actually
 * complete the join on the first call; a robust pattern uses a
 * {@code ConsumerRebalanceListener.onPartitionsAssigned} to seek
 * exactly when assignment is known. But for the lint rule's
 * lexical state machine, the poll is what flips the state.)
 */
public final class GoodConsumerSeekAfterPoll {

    public void seekAfterFirstPoll(Consumer<String, String> consumer,
                                   TopicPartition partition) {
        consumer.subscribe(List.of("orders"));
        consumer.poll(Duration.ofSeconds(1)); // poll happens — state becomes POLLED
        consumer.seek(partition, 0L);          // silent: state is POLLED, not SUBSCRIBED
    }

    public void seekMultiplePartitionsAfterPoll(Consumer<String, String> consumer,
                                                TopicPartition p0,
                                                TopicPartition p1) {
        consumer.subscribe(List.of("orders", "payments"));
        consumer.poll(Duration.ofSeconds(1));
        consumer.seek(p0, 100L); // silent
        consumer.seek(p1, 200L); // silent
    }
}
