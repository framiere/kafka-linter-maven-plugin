package sample;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

/**
 * Control class for COMMIT_ASYNC_NO_FINAL_SYNC.
 *
 * <p>Uses commitAsync in the poll loop (throughput) AND commitSync in
 * a finally block (durability on shutdown). The class-scope detector
 * sees that the class contains at least one commitSync call site, so
 * the rule does NOT fire — even though commitAsync is present.
 *
 * <p>This file is a SEPARATE top-level class from {@code BadCommitAsyncNoFinalSync}
 * so the linter analyses them as independent ClassNodes. The rule's
 * class scope is what makes that separation necessary: putting both
 * shapes in the same class would suppress the BAD case (because the
 * class would contain a commitSync), and that's not the test we want
 * to run.
 */
public final class GoodCommitAsyncWithFinalSync {

    private final Consumer<String, String> consumer;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public GoodCommitAsyncWithFinalSync(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    /** Control: poll loop uses commitAsync, finally block uses commitSync — must NOT fire. */
    public void runPollLoop() {
        try {
            while (running.get()) {
                ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
                process(recs);
                consumer.commitAsync(); // throughput path
            }
        } catch (WakeupException wakeUp) {
            // shutdown requested
        } finally {
            try {
                consumer.commitSync(); // durability path — class-scope detector finds this
            } finally {
                consumer.close();
            }
        }
    }

    public void requestShutdown() {
        running.set(false);
        consumer.wakeup();
    }

    private void process(ConsumerRecords<String, String> recs) {
        if (recs.isEmpty()) {
            return;
        }
    }
}
