package sample;

import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Control for PRODUCER_INIT_TRANSACTIONS_NOT_CALLED.
 *
 * <p>This project contains lifecycle calls AND a call to
 * {@code initTransactions()}. The project-scoped rule walks every
 * class, sees the {@code initTransactions()} site, sets its
 * project-wide {@code hasInit = true} flag, and then — even though
 * lifecycle sites are recorded — returns an EMPTY violation list
 * because the second guard ({@code if (sites.isEmpty() || hasInit)
 * return List.of();}) trips on {@code hasInit}.
 *
 * <p>Where you'd really call {@code initTransactions()} in
 * production code: ONCE, immediately after constructing the
 * producer, in a setup method (or {@code @PostConstruct} of a
 * singleton bean). Calling it more than once on the same producer
 * is harmless but pointless — the rule only cares that there's at
 * least one call somewhere.
 *
 * <p>Note: {@code initTransactions()} requires the
 * {@code transactional.id} producer config to be set, otherwise it
 * throws at runtime. The rule does NOT verify that config is set —
 * that would be a separate properties-rule. The rule only enforces
 * the LIFECYCLE: if you use beginTransaction et al., you must have
 * called initTransactions somewhere.
 */
public final class GoodProducerWithInitTransactions {

    /** Setup site: initTransactions called once, satisfies the project-scoped guard. */
    public void setup(Producer<String, String> producer) {
        producer.initTransactions();
    }

    /** Lifecycle usage — silent because the project contains an initTransactions call (in setup()). */
    public void beginAndCommit(Producer<String, String> producer,
                               ProducerRecord<String, String> record) {
        producer.beginTransaction();
        producer.send(record);
        producer.commitTransaction();
    }

    /** Lifecycle usage — silent, same reason. */
    public void emitOffsetsAtomically(Producer<String, String> producer,
                                      Map<TopicPartition, OffsetAndMetadata> offsets,
                                      String consumerGroupId) {
        producer.beginTransaction();
        producer.sendOffsetsToTransaction(offsets, consumerGroupId);
        producer.commitTransaction();
    }

    /** Abort path — silent, same reason. */
    public void rollback(Producer<String, String> producer) {
        producer.beginTransaction();
        producer.abortTransaction();
    }
}
