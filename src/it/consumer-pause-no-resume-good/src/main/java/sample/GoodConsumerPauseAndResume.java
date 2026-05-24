package sample;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.List;

/**
 * Pause and resume split across separate methods of the same class — the
 * canonical backpressure shape. The rule must NOT fire.
 */
public final class GoodConsumerPauseAndResume {

    private final KafkaConsumer<String, String> consumer;

    public GoodConsumerPauseAndResume(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    public void onDownstreamFull(List<TopicPartition> assignment) {
        consumer.pause(assignment);
    }

    public void onDownstreamDrained(List<TopicPartition> assignment) {
        consumer.resume(assignment);
    }
}
