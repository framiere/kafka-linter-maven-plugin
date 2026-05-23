package sample;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Silent: the 1-arg send() overload — `send(record)` returns the
 * Future without a Callback. This is the syntactic shape that
 * makes "fire-and-forget" explicit at the call site.
 *
 * The rule's descriptor check is "the LAST argument is the
 * Callback interface" — the 1-arg overload's descriptor is
 * `(Lorg/apache/kafka/clients/producer/ProducerRecord;)
 * Ljava/util/concurrent/Future;`, whose last arg is ProducerRecord,
 * not Callback. So the rule abstains.
 *
 * The 1-arg form has the SAME runtime semantics as the 2-arg
 * `send(record, null)` — both swallow errors. But it's the
 * lesser evil:
 *   (a) the syntactic shape itself is a marker that the caller
 *       did not provide error handling;
 *   (b) the SIBLING rule PRODUCER_SEND_NO_CALLBACK targets exactly
 *       this 1-arg form WHEN THE RETURNED FUTURE IS DISCARDED
 *       (the typical bug shape).
 *
 * This rule and the sibling rule are deliberately split because
 * they catch different syntactic shapes:
 *   - PRODUCER_SEND_NO_CALLBACK: `send(record)` with discarded
 *     Future — the 1-arg form
 *   - PRODUCER_SEND_NULL_CALLBACK: `send(record, null)` — the
 *     2-arg form with a deliberate null Callback
 * The 2-arg null form is worse in code review (looks deliberate)
 * but has the same runtime semantics as the 1-arg form.
 */
public class GoodProducerSendOneArgForm {

    private final Producer<String, String> producer;

    public GoodProducerSendOneArgForm(Producer<String, String> producer) {
        this.producer = producer;
    }

    /**
     * 1-arg send — this rule does not fire. The sibling rule
     * PRODUCER_SEND_NO_CALLBACK would target this shape (but is
     * OFF in this IT's pom.xml since it's not the rule under
     * test).
     */
    public void sendOneArg(String key, String value) {
        producer.send(new ProducerRecord<>("orders", key, value));
    }
}
