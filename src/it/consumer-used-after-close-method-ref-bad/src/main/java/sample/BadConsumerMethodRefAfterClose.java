package sample;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * RULE: CONSUMER_USED_AFTER_CLOSE — method-reference variant.
 *
 * <p>Each method in this fixture closes a local {@code KafkaConsumer} and
 * THEN captures a {@code consumer::<lifecycle>} method reference into a
 * downstream consumer (an {@link ExecutorService#submit submit()}, a
 * {@link java.util.List#forEach forEach()}, or a
 * {@link CompletableFuture#runAsync runAsync()}). When the captured
 * functional interface is later invoked — on the executor thread, on the
 * forEach loop iteration, or on the async stage — the actual call into
 * the consumer hits an internal {@code closed=true} flag and the consumer
 * throws {@code IllegalStateException("This consumer has already been
 * closed")}. In an executor / callback path that exception is silently
 * swallowed and the last batch of offsets is silently dropped.
 *
 * <h2>Why this is a tricky linting target — and why the naive scan misses it</h2>
 *
 * javac compiles {@code consumer::commitSync} to an {@code INVOKEDYNAMIC}
 * instruction whose call-site descriptor consumes the consumer reference
 * from the stack ({@code (LKafkaConsumer;)LRunnable;}) and whose
 * bootstrap-method arguments contain a direct
 * {@code REF_invokeVirtual KafkaConsumer.commitSync:()V} handle. The
 * deferred call materialises only when the {@code Runnable.run()}
 * dispatch happens on the executor thread.
 *
 * <p><strong>The outer method's bytecode therefore contains zero
 * {@code INVOKEVIRTUAL} instructions targeting {@code commitSync}.</strong>
 * A rule that walks only {@code MethodInsnNode}s sees the {@code close()}
 * but completely misses the captured {@code commitSync} — and reports
 * ZERO violations, when the operator is staring at four use-after-close
 * bugs.
 *
 * <p>The fix: also walk every {@code InvokeDynamicInsnNode} and inspect
 * its bsm-arg handles for any {@code REF_invoke*
 * KafkaConsumer.<lifecycle>}. Resolve the indy's captured receiver slot
 * by backward stack-effect simulation (the ALOAD that fed the indy's
 * capture arg), and fire if that slot is already in the closed set. This
 * is exactly what the IT asserts: building with
 * {@code invoker.buildResult=failure} requires at least one ERROR to be
 * emitted — without the indy walk, ZERO are emitted and the IT would
 * (incorrectly) pass success.
 */
public final class BadConsumerMethodRefAfterClose {

    /**
     * close() then executor.execute(consumer::commitSync). The execute() captures
     * the commitSync reference; the executor thread invokes run() later, which
     * calls commitSync() on a consumer with {@code closed=true} and throws into
     * the executor's uncaught-exception handler — silent swallow.
     *
     * <p>{@code execute(Runnable)} (not {@code submit}) is used here because
     * {@code KafkaConsumer.commitSync} has four overloads and Java cannot
     * disambiguate the method reference when the receiver-type for {@code submit}
     * itself has two overloads ({@code Runnable} and {@code Callable<T>});
     * {@code execute} has a single {@code Runnable} signature, forcing the
     * method-reference resolution to {@code commitSync()} (void zero-arg).
     */
    public void closeThenExecuteCommitSyncRef() {
        Properties props = baseConsumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        ExecutorService exec = Executors.newSingleThreadExecutor();
        consumer.close();
        exec.execute(consumer::commitSync); // reported
        exec.shutdown();
    }

    /**
     * close() then topics.forEach(consumer::subscribe). Each iteration's
     * Consumer.accept(List) lands in a dead consumer and throws
     * IllegalStateException — the rebalance never happens.
     */
    public void closeThenForEachSubscribeRef(List<List<String>> topicLists) {
        Properties props = baseConsumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.close();
        topicLists.forEach(consumer::subscribe); // reported
    }

    /**
     * close() then CompletableFuture.runAsync(consumer::unsubscribe). The
     * async unsubscribe stage runs on the common pool after close() has
     * already left the group; unsubscribe() throws into a stage no one
     * .join()s.
     */
    public void closeThenAsyncUnsubscribeRef() {
        Properties props = baseConsumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.close();
        CompletableFuture.runAsync(consumer::unsubscribe); // reported
    }

    /**
     * close() then execute(consumer::commitAsync). The classic
     * "shutdown-then-flush-offsets" reversal — by the time the executor
     * picks up the Runnable, the heartbeat thread is gone and the offsets
     * are silently dropped.
     */
    public void closeThenExecuteCommitAsyncRef() {
        Properties props = baseConsumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        ExecutorService exec = Executors.newSingleThreadExecutor();
        consumer.close();
        exec.execute(consumer::commitAsync); // reported
        exec.shutdown();
    }

    private static Properties baseConsumerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("group.id", "bad-consumer-method-ref-after-close");
        p.put("client.id", "bad-consumer-method-ref-after-close");
        p.put("auto.offset.reset", "earliest");
        p.put("enable.auto.commit", "false");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return p;
    }
}
