package sample;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * RULE: PRODUCER_USED_AFTER_CLOSE — method-reference variant.
 *
 * <p>Each method in this fixture closes a local {@code KafkaProducer} and
 * THEN captures a {@code producer::<lifecycle>} method reference into a
 * downstream consumer (an {@link ExecutorService#submit submit()}, a
 * {@link java.util.List#forEach forEach()}, or a
 * {@link CompletableFuture#runAsync runAsync()}). When the captured
 * functional interface is later invoked — on the executor thread, on the
 * forEach loop iteration, or on the async stage — the actual call into
 * the producer hits an internal {@code closed=true} flag and the producer
 * throws {@code IllegalStateException("Cannot perform operation after
 * producer has been closed")}. In an executor / callback path that
 * exception is silently swallowed and the record is lost.
 *
 * <h2>Why this is a tricky linting target — and why the naive scan misses it</h2>
 *
 * javac compiles {@code producer::send} to an {@code INVOKEDYNAMIC}
 * instruction whose call-site descriptor consumes the producer reference
 * from the stack ({@code (LKafkaProducer;)LFunction;}) and whose
 * bootstrap-method arguments contain a direct
 * {@code REF_invokeVirtual KafkaProducer.send:(LProducerRecord;)LFuture;}
 * handle. The deferred call materialises only when the
 * {@code Function.apply(...)} dispatch happens on the executor thread.
 *
 * <p><strong>The outer method's bytecode therefore contains zero
 * {@code INVOKEVIRTUAL} instructions targeting {@code send}.</strong> A
 * rule that walks only {@code MethodInsnNode}s sees the {@code close()}
 * but completely misses the captured {@code send} — and reports ZERO
 * violations, when the operator is staring at four use-after-close
 * bugs.
 *
 * <p>The fix: also walk every {@code InvokeDynamicInsnNode} and inspect
 * its bsm-arg handles for any {@code REF_invoke*
 * KafkaProducer.<lifecycle>}. Resolve the indy's captured receiver
 * slot by backward stack-effect simulation (the ALOAD that fed the
 * indy's capture arg), and fire if that slot is already in the closed
 * set. This is exactly what the IT asserts: building with
 * {@code invoker.buildResult=failure} requires at least one ERROR to
 * be emitted — without the indy walk, ZERO are emitted and the IT
 * would (incorrectly) pass success.
 *
 * <h2>Lambda form remains an acknowledged false-negative</h2>
 *
 * A closure like {@code () -> producer.send(record)} compiles to a
 * synthetic {@code lambda$0(KafkaProducer, ProducerRecord)} method
 * whose body holds the {@code INVOKEVIRTUAL send}; the outer method's
 * indy bootstrap handle only points at {@code lambda$0}, not at
 * {@code send}. We could chase the synthetic, but it requires a
 * second-level scan and risks false positives on lambdas that
 * conditionally guard the call. We accept that as an out-of-scope
 * limitation — the method-reference shape (which is the more idiomatic
 * one for use-after-close bugs) is fully covered.
 */
public final class BadProducerMethodRefAfterClose {

    /**
     * close() then executor.submit(producer::flush). The submit() captures
     * the flush reference; the executor thread invokes run() later, which
     * calls flush() on a producer with {@code closed=true} and throws into
     * a {@code Future} no one ever calls {@code .get()} on — silent
     * swallow.
     */
    public void closeThenSubmitFlushRef() {
        Properties props = baseProducerProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ExecutorService exec = Executors.newSingleThreadExecutor();
        producer.close();
        exec.submit(producer::flush); // reported
        exec.shutdown();
    }

    /**
     * close() then records.forEach(producer::send). The most idiomatic
     * shape — refactoring routinely moves close() above the forEach by
     * accident; each iteration's Consumer.accept(record) lands in a dead
     * producer.
     */
    public void closeThenForEachSendRef(List<ProducerRecord<String, String>> records) {
        Properties props = baseProducerProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.close();
        records.forEach(producer::send); // reported
    }

    /**
     * close() then CompletableFuture.runAsync(producer::flush). The async
     * flush stage runs on the common pool after close() has already
     * drained-and-shutdown the Sender; flush() throws into a stage no one
     * .join()s.
     */
    public void closeThenAsyncFlushRef() {
        Properties props = baseProducerProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.close();
        CompletableFuture.runAsync(producer::flush); // reported
    }

    /**
     * Transactional: close() then submit(producer::commitTransaction).
     * Doubly bad — close() already aborts any in-flight transaction, and
     * the deferred commitTransaction() throws on a producer whose
     * TransactionManager is gone. Downstream {@code read_committed}
     * consumers see nothing of what the author thought they committed.
     */
    public void closeThenSubmitCommitTxnRef() {
        Properties props = transactionalProducerProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();
        producer.beginTransaction();
        ExecutorService exec = Executors.newSingleThreadExecutor();
        producer.close();
        exec.submit(producer::commitTransaction); // reported
        exec.shutdown();
    }

    private static Properties baseProducerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("client.id", "bad-producer-method-ref-after-close");
        p.put("compression.type", "lz4");
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return p;
    }

    private static Properties transactionalProducerProps() {
        Properties p = baseProducerProps();
        p.put("transactional.id", "bad-producer-method-ref-after-close-txn");
        return p;
    }
}
