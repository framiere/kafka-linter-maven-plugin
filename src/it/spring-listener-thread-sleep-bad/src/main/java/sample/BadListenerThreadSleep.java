package sample;

import org.springframework.kafka.annotation.KafkaListener;

import java.util.concurrent.TimeUnit;

/**
 * RULE: SPRING_LISTENER_THREAD_SLEEP.
 *
 * Fires when a {@code @KafkaListener}-annotated method body contains a call
 * to {@code Thread.sleep(...)} or {@code TimeUnit.sleep(...)}. Both park
 * the consumer thread; while it sleeps, {@code consumer.poll()} does not
 * advance {@code lastPolledTimestamp}. If the cumulative sleep + processing
 * time exceeds {@code max.poll.interval.ms} (default 5 min) the group
 * coordinator fences the consumer out, triggering a rebalance cascade.
 */
public final class BadListenerThreadSleep {

    /** Anti-pattern: Thread.sleep(long) inside a listener — direct rule hit. */
    @KafkaListener(topics = "orders")
    public void onOrder(String payload) throws InterruptedException {
        Thread.sleep(2000); // FIRES — Thread.sleep(long)
    }

    /** Anti-pattern: Thread.sleep(long, int) overload also blocks the listener thread. */
    @KafkaListener(topics = "payments")
    public void onPayment(String payload) throws InterruptedException {
        Thread.sleep(1000, 500_000); // FIRES — Thread.sleep(long, int)
    }

    /** Anti-pattern: TimeUnit.SECONDS.sleep — same effect via TimeUnit indirection. */
    @KafkaListener(topics = "audit")
    public void onAudit(String payload) throws InterruptedException {
        TimeUnit.SECONDS.sleep(5); // FIRES — TimeUnit.sleep
    }

    /** Anti-pattern: multiple sleeps in one listener body — one Violation per call site. */
    @KafkaListener(topics = "throttled")
    public void onThrottled(String payload) throws InterruptedException {
        Thread.sleep(100); // FIRES (1)
        process(payload);
        Thread.sleep(100); // FIRES (2)
    }

    /** Control: not annotated @KafkaListener — must NOT fire even though it sleeps. */
    public void notAListener() throws InterruptedException {
        Thread.sleep(1000);
    }

    private void process(String s) { /* no-op */ }
}
