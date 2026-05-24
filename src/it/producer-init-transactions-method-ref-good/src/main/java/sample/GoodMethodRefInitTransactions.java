package sample;

import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Control for PRODUCER_INIT_TRANSACTIONS_NOT_CALLED — method-reference variant.
 *
 * <p>This fixture asserts that the project-wide {@code initTransactions()}
 * detector accepts a method reference as proof of intent, not just a direct
 * {@code producer.initTransactions()} call.
 *
 * <p>The pattern shown here — lazy / asynchronous initialization via
 * {@code CompletableFuture.runAsync(producer::initTransactions)} — appears
 * routinely in production code that wants to overlap the (network-bound)
 * coordinator handshake with other startup work. The synchronous
 * {@code initTransactions()} call performs a {@code FindCoordinator},
 * {@code InitProducerId}, and {@code AddPartitionsToTxn} round-trip; running
 * it on the main thread blocks startup for tens or hundreds of milliseconds
 * depending on the cluster's distance from the workload. Pushing the call
 * onto a worker thread via a method reference is the cleanest shape.
 *
 * <h2>Why this is a tricky linting target</h2>
 *
 * javac compiles {@code producer::initTransactions} to an
 * {@code INVOKEDYNAMIC} whose bootstrap-method arg list contains a direct
 * {@code REF_invokeInterface Producer.initTransactions:()V} handle
 * (or {@code REF_invokeVirtual KafkaProducer.initTransactions:()V} when the
 * static type is the concrete class). The deferred invocation — when
 * {@code Runnable.run()} dispatches the {@code CallSite} — produces the
 * actual {@code INVOKEVIRTUAL} / {@code INVOKEINTERFACE} inside synthetic
 * runtime code, NOT inside the user's class.
 *
 * <p><strong>The user-class bytecode therefore contains zero
 * {@code INVOKE*} instructions targeting {@code initTransactions}.</strong>
 * A naive {@code MethodInsnNode} scan over every class in the project
 * reports {@code hasInit = false}, every txn-lifecycle site below becomes a
 * fire, and the rule emits 4 false-positive violations claiming the project
 * never initialised transactions.
 *
 * <p>The fix: extend the project-wide scan to also walk every
 * {@code INVOKEDYNAMIC}'s {@code bsmArgs} for any
 * {@code Handle} whose owner is in {@code KafkaTypes.PRODUCER_OWNERS}, name
 * is {@code "initTransactions"}, and descriptor is {@code "()V"}. That
 * recovery is what this fixture asserts.
 *
 * <p>Expected violations from this fixture: ZERO.
 */
public final class GoodMethodRefInitTransactions {

    /**
     * Lazy / asynchronous initialisation. The ONLY mention of
     * {@code initTransactions} anywhere in this project is the method
     * reference captured here. Compiles to {@code INVOKEDYNAMIC #N:run:
     * (Lorg/apache/kafka/clients/producer/Producer;)Ljava/lang/Runnable;}
     * with a {@code REF_invokeInterface
     * org/apache/kafka/clients/producer/Producer.initTransactions:()V}
     * bootstrap-method handle.
     */
    public CompletableFuture<Void> initAsync(Producer<String, String> producer) {
        return CompletableFuture.runAsync(producer::initTransactions);
    }

    /** Lifecycle usage — must NOT fire because the project contains the method-ref init above. */
    public void beginAndCommit(Producer<String, String> producer,
                               ProducerRecord<String, String> record) {
        producer.beginTransaction();
        producer.send(record);
        producer.commitTransaction();
    }

    /** Abort path — must NOT fire either, same reason. */
    public void rollback(Producer<String, String> producer) {
        producer.beginTransaction();
        producer.abortTransaction();
    }
}
