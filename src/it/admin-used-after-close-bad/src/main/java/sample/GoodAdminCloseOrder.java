package sample;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Control for ADMIN_USED_AFTER_CLOSE.
 *
 * <p>Three methods that each avoid the rule for a DIFFERENT reason —
 * the three orthogonal exits the rule's check supports:
 *
 * <ol>
 *   <li>{@code listTopicsThenCloseInFinally} — the textbook correct
 *       order: listTopics (USE) then close (CLOSED). The lifecycle
 *       method is reached BEFORE close, so when the rule scans linearly
 *       the slot is not yet in {@code closedSlots} at the listTopics
 *       site. Silent.</li>
 *   <li>{@code closeOnlyNeverUseAfter} — close happens, but nothing
 *       lifecycle-relevant happens AFTER close on the same slot. The
 *       slot is in {@code closedSlots} from then on, but no
 *       subsequent lifecycle call references it. Silent. (This is
 *       also what try-with-resources looks like in bytecode: the
 *       synthetic close() is the last call on the slot.)</li>
 *   <li>{@code closeFirstSlotUseSecondSlot} — two DIFFERENT admin slots
 *       in the same method. The rule tracks both slots independently.
 *       Closing slot A and then calling listTopics() on slot B does NOT
 *       fire — the listTopics's receiver resolves to B which is not
 *       closed. Silent. This is the case the rule's use of
 *       {@code resolveReceiverSlot} (rather than a "any previous close
 *       means we're done" approximation) is designed to handle.</li>
 * </ol>
 */
public final class GoodAdminCloseOrder {

    /** Textbook order: createTopics + listTopics first, then close. Slot not yet in closedSlots at the use sites. */
    public void listTopicsThenCloseInFinally() {
        Properties props = baseAdminProps();
        Admin admin = Admin.create(props);
        try {
            admin.createTopics(Collections.singletonList(new NewTopic("orders", 3, (short) 2))); // silent: closedSlots empty
            admin.listTopics(); // silent: closedSlots empty
        } finally {
            admin.close();
        }
    }

    /** Close is the only lifecycle call. No use-after. */
    public void closeOnlyNeverUseAfter() {
        Properties props = baseAdminProps();
        Admin admin = Admin.create(props);
        admin.close(); // and that's the entire body. Silent.
    }

    /** Two slots: closing one and then using the OTHER must not fire. */
    public void closeFirstSlotUseSecondSlot() {
        Properties props = baseAdminProps();
        Admin first = Admin.create(props);   // slot A
        Admin second = Admin.create(props);  // slot B
        first.close();                       // closedSlots = { A }
        second.createTopics(List.of(new NewTopic("orders", 3, (short) 2))); // receiver resolves to B — silent
        second.listTopics();                 // silent
        second.close();
    }

    private static Properties baseAdminProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        p.put("client.id", "good-admin-close-order");
        return p;
    }
}
