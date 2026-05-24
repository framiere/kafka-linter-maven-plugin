package sample;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Silent: each shape below sits OUTSIDE the lambda-empty-body
 * detector by design.
 *
 *   - send(record): the rule's descriptor check ("last arg is
 *     Callback") rejects the 1-arg overload — descriptor's last
 *     arg is ProducerRecord, not Callback.
 *
 *   - send(record, null): preceding instruction is ACONST_NULL,
 *     not INVOKEDYNAMIC. Handled by the sibling rule
 *     PRODUCER_SEND_NULL_CALLBACK (turned OFF in this IT's
 *     severities so it does not co-fire and obscure the result).
 *
 *   - send(record, this.fieldCallback): preceding instruction is
 *     GETFIELD, not INVOKEDYNAMIC.
 *
 *   - send(record, this::onSend) — method reference. The
 *     INVOKEDYNAMIC's impl-handle owner is the current class but
 *     the handle name is the real method name (`onSend`) — does
 *     NOT start with `lambda$`. Even if the method body happens to
 *     be empty, this rule abstains by design (out-of-scope, kept
 *     to preserve HIGH confidence; a separate rule on empty
 *     instance-method callbacks could be added later).
 *
 *   - send(record, new Callback() { ... }): preceding instruction
 *     is INVOKESPECIAL of the anonymous class constructor, not
 *     INVOKEDYNAMIC.
 */
public class GoodProducerSendNotLambda {

    private final Producer<String, String> producer;
    private final Callback fieldCallback = (md, ex) -> {
        if (ex != null) {
            System.err.println("field callback: " + ex.getMessage());
        }
    };

    public GoodProducerSendNotLambda(Producer<String, String> producer) {
        this.producer = producer;
    }

    /**
     * Shape A: 1-arg overload. Descriptor's last arg is
     * ProducerRecord, not Callback. Rule abstains.
     */
    public void sendOneArg(String key, String value) {
        producer.send(new ProducerRecord<>("orders", key, value));
    }

    /**
     * Shape B: 2-arg overload with literal null Callback. Preceding
     * instruction is ACONST_NULL, not INVOKEDYNAMIC. Sibling rule
     * PRODUCER_SEND_NULL_CALLBACK would target this shape; in this
     * IT it is configured OFF to avoid co-firing.
     */
    public void sendNullCallback(String key, String value) {
        producer.send(new ProducerRecord<>("orders", key, value), null);
    }

    /**
     * Shape C: Callback supplied from an instance field. Preceding
     * instruction is GETFIELD this.fieldCallback, not
     * INVOKEDYNAMIC. The field initialiser is itself a lambda with
     * a non-empty body, declared as a class-level Callback — but
     * the SEND-site preceding instruction is the GETFIELD, so the
     * rule abstains regardless of the field's body.
     */
    public void sendFieldCallback(String key, String value) {
        producer.send(new ProducerRecord<>("orders", key, value), fieldCallback);
    }

    /**
     * Shape D: instance-method reference `this::onSend`. The
     * INVOKEDYNAMIC's impl-handle owner is the current class but
     * the name is `onSend` — does NOT start with `lambda$`. The
     * rule's name-prefix check excludes this case to keep HIGH
     * confidence (instance-method bodies might be empty for valid
     * reasons; a dedicated rule could target them later).
     */
    public void sendMethodReferenceEmpty(String key, String value) {
        producer.send(new ProducerRecord<>("orders", key, value), this::onSend);
    }

    /**
     * Shape E: anonymous Callback subclass with empty body. The
     * preceding instruction is INVOKESPECIAL on the anonymous
     * inner class constructor, not INVOKEDYNAMIC. Rule abstains.
     * (The shape is still a bug; flagging it would require a
     * separate detector for anonymous-inner-class empty bodies.)
     */
    public void sendAnonymousEmpty(String key, String value) {
        producer.send(new ProducerRecord<>("orders", key, value), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                // intentionally empty — out of this rule's scope
            }
        });
    }

    private void onSend(RecordMetadata metadata, Exception exception) {
        // body intentionally empty — referenced as `this::onSend` above
    }
}
