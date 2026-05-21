package sample;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * RULE: CONSUMER_ENFORCE_REBALANCE_NO_REASON.
 *
 * Fires when {@code Consumer.enforceRebalance()} is called without a
 * reason String. The bytecode descriptor of the no-reason 0-arg overload
 * does NOT contain {@code Ljava/lang/String;}.
 */
public final class BadConsumerEnforceRebalanceNoReason {

    /** Anti-pattern: no reason on Consumer interface (INVOKEINTERFACE) — FIRES. */
    public void interfaceNoReason(Consumer<String, String> consumer) {
        consumer.enforceRebalance(); // FIRES — no reason String
    }

    /** Anti-pattern: no reason on KafkaConsumer concrete (INVOKEVIRTUAL) — FIRES. */
    public void concreteNoReason(KafkaConsumer<String, String> consumer) {
        consumer.enforceRebalance(); // FIRES — no reason String
    }

    /** Control: explicit reason String — must NOT fire. */
    public void withReason(Consumer<String, String> consumer) {
        consumer.enforceRebalance("autoscaler scale-out svc-payments-prod 21→30");
    }
}
