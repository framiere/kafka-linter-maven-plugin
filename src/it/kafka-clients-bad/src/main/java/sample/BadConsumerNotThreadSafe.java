package sample;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * RULE: CONSUMER_NOT_THREAD_SAFE.
 *
 * The consumer is captured in lambdas dispatched to other threads:
 *  - pool.submit(() -> consumer.commitSync(...))
 *  - new Thread(() -> consumer.poll(...)).start()
 *  - CompletableFuture.runAsync(() -> consumer.poll(...))
 *
 * KafkaConsumer is single-threaded; only wakeup() is safe to call from another thread.
 */
public final class BadConsumerNotThreadSafe {

    private final KafkaConsumer<String, String> consumer;
    private final ExecutorService pool = Executors.newFixedThreadPool(4);

    public BadConsumerNotThreadSafe() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1.prod.example.com:9092");
        p.put("client.id", "bad-consumer-thread-safety");
        p.put("group.id", "bad-consumer-thread-safety");
        p.put("enable.auto.commit", "false");
        this.consumer = new KafkaConsumer<>(p);
    }

    public void runFanOut() {
        consumer.subscribe(List.of("t"));
        while (running()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : records) {
                pool.submit(() -> {
                    handle(r);
                    consumer.commitSync();
                });
            }
        }
    }

    public void runWithThread() {
        new Thread(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : records) {
                handle(r);
            }
        }).start();
    }

    public void runWithCompletableFuture() {
        CompletableFuture.runAsync(() -> consumer.poll(Duration.ofMillis(500)));
    }

    public void shutdown() {
        new Thread(consumer::wakeup).start();
        consumer.close(Duration.ofSeconds(10));
    }

    private boolean running() { return true; }
    private void handle(ConsumerRecord<String, String> r) {}
}
