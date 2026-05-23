package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Silent: every consumer method call in this class executes on the
 * SAME thread that owns the consumer. No lambda body touches the
 * consumer, and no executor / Thread / CompletableFuture dispatch
 * passes a consumer-touching lambda across threads.
 *
 * This is the canonical "single-thread per consumer" shape that
 * KafkaConsumer's documentation explicitly recommends:
 *   - one consumer instance owned by one thread (the poll thread)
 *   - that thread loops: poll → process → commit
 *   - if record processing is expensive, the poll thread submits
 *     RECORDS (not the consumer) to a worker pool for processing
 *   - commit happens back on the poll thread after the batch
 *
 * The rule's first gate is "lambda body invokes a non-wakeup method
 * on the consumer". None of the lambdas below do that, so the rule
 * cannot fire — the dispatch-site check never runs.
 */
public class GoodConsumerOnPollThread {

    private final Consumer<String, String> consumer;
    private final ExecutorService workerPool = Executors.newFixedThreadPool(4);

    public GoodConsumerOnPollThread(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    /**
     * Shape A: synchronous poll loop. consumer.poll is called
     * directly on the calling thread — there is no lambda, no
     * INVOKEDYNAMIC, so the rule has nothing to flag. The
     * `processRecord` calls run synchronously on the same thread.
     */
    public void pollLoopSynchronous() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> r : records) {
                processRecord(r);
            }
            consumer.commitSync();
        }
    }

    /**
     * Shape B: poll on this thread, OFFLOAD ONLY THE RECORD VALUES
     * to a worker pool. The lambda passed to executor.submit
     * captures `r` (the ConsumerRecord) but NOT the consumer — its
     * body invokes `processRecord(r)`, not `consumer.anything()`.
     *
     * Bytecode: the lambda compiles to `lambda$pollAndOffload$N`
     * with body
     *   INVOKEVIRTUAL GoodConsumerOnPollThread.processRecord(...)
     * The lambda-body scan finds no INVOKE on a CONSUMER_OWNERS
     * type — so it is not added to consumerTouchingLambdas, and the
     * dispatch-site check abstains.
     *
     * This is the recommended pattern for parallel record
     * processing: keep poll() on the poll thread, parallelize the
     * per-record work.
     */
    public void pollAndOffload() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> r : records) {
            // SILENT: lambda body does not touch the consumer at all.
            workerPool.submit(() -> processRecord(r));
        }
        // Commit on the poll thread after the batch is dispatched.
        // (Real code would await worker completion before committing —
        // omitted here for fixture clarity; the rule cares about
        // thread-safety, not commit ordering.)
        consumer.commitSync();
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        System.out.println("processing: " + record.value());
    }
}
