package sample;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Silent: every 2-arg send() in this class is called with a
 * non-null Callback. The rule's check is "the instruction
 * preceding the INVOKE is ACONST_NULL" — for each shape below, the
 * preceding instruction is one of:
 *   - INVOKEDYNAMIC (lambda metafactory output)
 *   - GETFIELD (Callback field)
 *   - INVOKEVIRTUAL of a Callback-returning method
 *   - NEW + INVOKESPECIAL (Callback subclass instantiation)
 * None of these is ACONST_NULL, so the rule does not fire.
 *
 * These are the canonical good-citizen shapes — every producer.send
 * has a Callback that at minimum logs the exception and increments
 * a metric counter on failure.
 */
public class GoodProducerSendWithRealCallback {

    private final Producer<String, String> producer;

    // Pre-built Callback held as a field — useful when the Callback
    // is expensive to allocate or when the same handler is used
    // across many call sites.
    private final Callback metricsCallback = (metadata, exception) -> {
        if (exception != null) {
            System.err.println("send failed: " + exception.getMessage());
        }
    };

    public GoodProducerSendWithRealCallback(Producer<String, String> producer) {
        this.producer = producer;
    }

    /**
     * Shape A: inline lambda Callback. javac compiles this to
     * INVOKEDYNAMIC against the LambdaMetafactory bootstrap,
     * returning a Callback instance — the instruction before the
     * INVOKEINTERFACE Producer.send is the INVOKEDYNAMIC, not
     * ACONST_NULL.
     */
    public void sendWithLambda(String key, String value) {
        producer.send(new ProducerRecord<>("orders", key, value), (metadata, exception) -> {
            if (exception != null) {
                System.err.println("inline lambda send failed: " + exception.getMessage());
            }
        });
    }

    /**
     * Shape B: Callback from a field. The instruction before the
     * INVOKE is GETFIELD this.metricsCallback — not ACONST_NULL.
     */
    public void sendWithFieldCallback(String key, String value) {
        producer.send(new ProducerRecord<>("orders", key, value), metricsCallback);
    }

    /**
     * Shape C: anonymous Callback subclass. The bytecode pattern is
     *   NEW sample/GoodProducerSendWithRealCallback$1
     *   DUP
     *   INVOKESPECIAL sample/GoodProducerSendWithRealCallback$1.<init>()V
     * — the instruction directly preceding the producer.send INVOKE
     * (after the standard prevSignificant skip) is the
     * INVOKESPECIAL of the anonymous class constructor, not
     * ACONST_NULL.
     */
    public void sendWithAnonymousCallback(String key, String value) {
        producer.send(new ProducerRecord<>("orders", key, value), new Callback() {
            @Override
            public void onCompletion(org.apache.kafka.clients.producer.RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    System.err.println("anon callback send failed: " + exception.getMessage());
                }
            }
        });
    }
}
