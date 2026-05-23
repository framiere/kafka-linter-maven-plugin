package sample;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Control for PRODUCER_USED_AFTER_CLOSE.
 *
 * <p>Three methods that each avoid the rule for a DIFFERENT reason —
 * the three orthogonal exits the rule's check supports:
 *
 * <ol>
 *   <li>{@code sendThenCloseInFinally} — the textbook correct order:
 *       send (USE) then close (CLOSED). The lifecycle method is
 *       reached BEFORE close, so when the rule scans linearly the
 *       slot is not yet in {@code closedSlots} at the send site.
 *       Silent.</li>
 *   <li>{@code closeOnlyNeverUseAfter} — close happens, but nothing
 *       lifecycle-relevant happens AFTER close on the same slot. The
 *       slot is in {@code closedSlots} from then on, but no
 *       subsequent lifecycle call references it. Silent. (This is
 *       also what try-with-resources looks like in bytecode: the
 *       synthetic close() is the last call on the slot.)</li>
 *   <li>{@code closeFirstSlotUseSecondSlot} — two DIFFERENT producer
 *       slots in the same method. The rule tracks both slots
 *       independently. Closing slot A and then calling send() on
 *       slot B does NOT fire — the send's receiver resolves to B
 *       which is not closed. Silent. This is the case the rule's
 *       use of {@code resolveReceiverSlot} (rather than a "any
 *       previous close means we're done" approximation) is designed
 *       to handle.</li>
 * </ol>
 */
public final class GoodProducerCloseOrder {

    /** Textbook order: send first, then close. Slot not yet in closedSlots at send site. */
    public void sendThenCloseInFinally(ProducerRecord<String, String> record) {
        Properties props = baseProducerProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        try {
            producer.send(record); // silent: closedSlots empty at this point
        } finally {
            producer.close();
        }
    }

    /** Close is the only lifecycle call. No use-after. */
    public void closeOnlyNeverUseAfter() {
        Properties props = baseProducerProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.close(); // and that's the entire body. Silent.
    }

    /** Two slots: closing one and then using the OTHER must not fire. */
    public void closeFirstSlotUseSecondSlot(ProducerRecord<String, String> record) {
        Properties props = baseProducerProps();
        KafkaProducer<String, String> first = new KafkaProducer<>(props);   // slot A
        KafkaProducer<String, String> second = new KafkaProducer<>(props);  // slot B
        first.close();          // closedSlots = { A }
        second.send(record);    // receiver resolves to B, B not closed — silent
        second.close();
    }

    private static Properties baseProducerProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("client.id", "good-producer-close-order");
        p.put("compression.type", "lz4");
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return p;
    }
}
