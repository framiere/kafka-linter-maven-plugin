package sample;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Control for CONSUMER_USED_AFTER_CLOSE.
 *
 * <p>Three methods that each avoid the rule for a DIFFERENT reason —
 * the three orthogonal exits the rule's check supports:
 *
 * <ol>
 *   <li>{@code pollThenCloseInFinally} — the textbook correct order:
 *       poll (USE) then close (CLOSED). The lifecycle method is
 *       reached BEFORE close, so when the rule scans linearly the
 *       slot is not yet in {@code closedSlots} at the poll site.
 *       Silent.</li>
 *   <li>{@code closeOnlyNeverUseAfter} — close happens, but nothing
 *       lifecycle-relevant happens AFTER close on the same slot. The
 *       slot is in {@code closedSlots} from then on, but no
 *       subsequent lifecycle call references it. Silent. (This is
 *       also what try-with-resources looks like in bytecode: the
 *       synthetic close() is the last call on the slot.)</li>
 *   <li>{@code closeFirstSlotUseSecondSlot} — two DIFFERENT consumer
 *       slots in the same method. The rule tracks both slots
 *       independently. Closing slot A and then calling poll() on
 *       slot B does NOT fire — the poll's receiver resolves to B
 *       which is not closed. Silent. This is the case the rule's
 *       use of {@code resolveReceiverSlot} (rather than a "any
 *       previous close means we're done" approximation) is designed
 *       to handle.</li>
 * </ol>
 */
public final class GoodConsumerCloseOrder {

    /** Textbook order: subscribe + poll first, then close. Slot not yet in closedSlots at poll site. */
    public void pollThenCloseInFinally() {
        Properties props = baseConsumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        try {
            consumer.subscribe(List.of("orders"));        // silent: closedSlots empty at this point
            consumer.poll(Duration.ofSeconds(1));          // silent: closedSlots empty at this point
        } finally {
            consumer.close();
        }
    }

    /** Close is the only lifecycle call. No use-after. */
    public void closeOnlyNeverUseAfter() {
        Properties props = baseConsumerProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.close(); // and that's the entire body. Silent.
    }

    /** Two slots: closing one and then using the OTHER must not fire. */
    public void closeFirstSlotUseSecondSlot() {
        Properties props = baseConsumerProps();
        KafkaConsumer<String, String> first = new KafkaConsumer<>(props);   // slot A
        KafkaConsumer<String, String> second = new KafkaConsumer<>(props);  // slot B
        first.close();                                  // closedSlots = { A }
        second.subscribe(List.of("orders"));            // receiver resolves to B, B not closed — silent
        second.poll(Duration.ofSeconds(1));             // silent
        second.close();
    }

    private static Properties baseConsumerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("group.id", "good-consumer-close-order");
        p.put("client.id", "good-consumer-close-order");
        p.put("auto.offset.reset", "earliest");
        p.put("enable.auto.commit", "false");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return p;
    }
}
