package sample;

import java.time.Duration;
import org.apache.kafka.clients.consumer.Consumer;

/**
 * Control for POLL_IN_REBALANCE_CALLBACK.
 *
 * <p>This class does NOT implement {@code ConsumerRebalanceListener}.
 * The rule's first guard ({@code implementsRebalanceListener}) trips
 * immediately and returns an empty list — every method body is
 * skipped without inspection. Even though this class has a method
 * literally named {@code onPartitionsAssigned} (a common helper
 * name in user code), the rule does not consider it a callback:
 * the listener-implementer check is what makes those method names
 * "callbacks" for the purposes of the rule.
 *
 * <p>This control proves that the listener-interface filter is the
 * gate — random calls to {@code consumer.poll()} from a method that
 * happens to share a name with a callback are not flagged.
 */
public final class GoodPollOutsideListener {

    /** Regular poll in a regular method — silent (class doesn't implement the listener). */
    public void pollLoop(Consumer<String, String> consumer) {
        consumer.poll(Duration.ofMillis(100));
    }

    /** Method name COLLIDES with a callback name, but class doesn't implement the listener — silent. */
    public void onPartitionsAssigned(Consumer<String, String> consumer) {
        consumer.poll(Duration.ofMillis(50));
    }
}
