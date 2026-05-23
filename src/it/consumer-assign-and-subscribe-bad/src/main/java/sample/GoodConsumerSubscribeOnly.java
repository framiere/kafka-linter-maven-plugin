package sample;

import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;

/**
 * Control for CONSUMER_ASSIGN_AND_SUBSCRIBE.
 *
 * <p>Group-managed shape: only subscribe() — no assign() anywhere
 * in the class. The class-scope rule sees subscribe but never finds
 * an assign site, so the violation list stays empty.
 */
public final class GoodConsumerSubscribeOnly {

    public void joinGroup(Consumer<String, String> consumer) {
        consumer.subscribe(List.of("orders", "payments"));
    }

    public void joinAnotherGroup(Consumer<String, String> consumer) {
        consumer.subscribe(List.of("audit-log"));
    }
}
