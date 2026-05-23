package sample;

import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * RULE: PRODUCER_INIT_TRANSACTIONS_NOT_CALLED.
 *
 * <p>Project-scoped rule. Fires when SOME class in the project calls
 * a Kafka producer's transactional LIFECYCLE method
 * ({@code beginTransaction}, {@code commitTransaction},
 * {@code abortTransaction}, or {@code sendOffsetsToTransaction})
 * but NO class anywhere in the project calls
 * {@code initTransactions()}.
 *
 * <p>Why this is a real problem:
 * <ol>
 *   <li>A KafkaProducer's internal {@code TransactionManager} starts
 *       life in state {@code UNINITIALIZED}. In that state the
 *       producer has no producer-id, no producer-epoch, and is not
 *       registered with the transaction coordinator on any broker.</li>
 *   <li>{@code initTransactions()} is the ONLY method that promotes
 *       the manager from {@code UNINITIALIZED} → {@code READY}. It
 *       performs a {@code FindCoordinator} request to discover the
 *       transaction coordinator broker, then an {@code InitProducerId}
 *       request that allocates the producer-id/epoch pair, then
 *       fences any prior producer that was using the same
 *       {@code transactional.id}.</li>
 *   <li>Every transactional method short-circuits if the manager is
 *       not in {@code READY}. The check inside the producer is, in
 *       effect: {@code if (!transactionManager.isTransactional()
 *       || transactionManager.currentState != READY) throw new
 *       KafkaException("Cannot perform '<op>' before transactions
 *       have been initialized. Please ensure that
 *       initTransactions() has been called.")}.</li>
 *   <li>So in production, the very first
 *       {@code producer.beginTransaction()} (or commit / abort /
 *       sendOffsets) throws {@code KafkaException}. The producer is
 *       unusable for transactional writes until someone fixes the
 *       startup wiring.</li>
 * </ol>
 *
 * <p>Why this slips through code review:
 * <ol>
 *   <li>{@code initTransactions()} only needs to be called ONCE —
 *       typically right after construction, often in a setup /
 *       {@code @PostConstruct} method that lives far from the
 *       business-logic methods that use {@code beginTransaction()}.
 *       In code review the lifecycle calls look fine; the missing
 *       setup is in a different file (or a different module, or
 *       nowhere).</li>
 *   <li>It's easy to set {@code transactional.id} in producer
 *       configuration (which makes the producer "transactional" by
 *       config) and then assume the lifecycle is "automatic." It is
 *       not — config says "I intend to be transactional", the
 *       {@code initTransactions()} call actually performs the
 *       handshake.</li>
 *   <li>Integration tests often construct a fresh producer per
 *       test, and a copy-paste of the setup block usually has the
 *       {@code initTransactions()} call. Production wiring, where
 *       the producer is a singleton bean built once and reused,
 *       is a different code path — that's the path that breaks.</li>
 * </ol>
 *
 * <p>What the rule checks (project-scope, two-phase):
 * <ol>
 *   <li>PHASE 1 — walk every {@code .class} file in {@code
 *       target/classes} and look for two kinds of MethodInsnNode
 *       sites whose owner is in {@code PRODUCER_OWNERS} (either
 *       the {@code Producer} interface or the {@code KafkaProducer}
 *       impl):
 *       <ul>
 *         <li>{@code initTransactions()V} — sets a project-wide
 *             {@code hasInit = true} flag.</li>
 *         <li>{@code beginTransaction} / {@code commitTransaction}
 *             / {@code abortTransaction} /
 *             {@code sendOffsetsToTransaction} — recorded as a
 *             "txn site" (class, method, line, called-method).</li>
 *       </ul>
 *   </li>
 *   <li>PHASE 2 — after walking everything, if {@code hasInit} is
 *       false AND at least one txn site was recorded, emit one
 *       violation per site. Each violation points at the lifecycle
 *       call and tells the developer to call
 *       {@code initTransactions()} once at construction.</li>
 * </ol>
 *
 * <p>This class deliberately uses all four lifecycle methods to
 * confirm each is recognized by the rule. The companion class
 * {@code OtherBadProducerNoInit} adds a second class that also
 * calls a lifecycle method — proving the rule scans the WHOLE
 * project, not just one class.
 */
public final class BadProducerNoInitTransactions {

    /** Anti-pattern: beginTransaction without initTransactions anywhere in the project. */
    public void startTransaction(Producer<String, String> producer) {
        producer.beginTransaction(); // reported
    }

    /** Anti-pattern: commitTransaction lifecycle call, still no initTransactions. */
    public void commit(Producer<String, String> producer,
                       ProducerRecord<String, String> record) {
        producer.send(record);
        producer.commitTransaction(); // reported
    }

    /** Anti-pattern: abortTransaction is also a lifecycle method that requires INIT. */
    public void abort(Producer<String, String> producer) {
        producer.abortTransaction(); // reported
    }

    /** Anti-pattern: sendOffsetsToTransaction is the consume-process-produce pattern. */
    public void emitOffsetsAtomically(Producer<String, String> producer,
                                      Map<TopicPartition, OffsetAndMetadata> offsets,
                                      String consumerGroupId) {
        producer.sendOffsetsToTransaction(offsets, consumerGroupId); // reported
    }
}
