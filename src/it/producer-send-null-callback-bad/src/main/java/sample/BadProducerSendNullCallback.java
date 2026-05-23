package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * RULE: PRODUCER_SEND_NULL_CALLBACK — per-call-site bytecode rule.
 *
 * The rule fires once per call site where:
 *   (a) the INVOKEVIRTUAL / INVOKEINTERFACE is on a recognized
 *       Producer owner — both the concrete `KafkaProducer` class
 *       AND the `Producer` interface count, since most app code
 *       holds a producer via the interface type;
 *   (b) the method name is "send";
 *   (c) the method descriptor's LAST argument type is the
 *       org.apache.kafka.clients.producer.Callback interface —
 *       i.e., this is the 2-arg `send(record, callback)` overload,
 *       not the 1-arg `send(record)` overload (sibling rule
 *       PRODUCER_SEND_NO_CALLBACK targets that one);
 *   (d) the instruction immediately preceding the INVOKE is
 *       ACONST_NULL — the source-level `null` literal that javac
 *       compiles directly to the JVM null constant.
 *
 * The shape this rule catches is `producer.send(record, null)` —
 * a source-level pattern where the developer explicitly chose the
 * 2-arg overload but passed a null Callback. The runtime semantics
 * are IDENTICAL to the 1-arg overload: any exception raised by the
 * sender thread (serialization failure, broker NACK, retry
 * exhaustion, OutOfOrderSequenceException, RecordTooLargeException,
 * NotEnoughReplicasException, etc.) is silently swallowed. The
 * application-side caller never knows the send failed.
 *
 * The semantic problem with `send(record, null)` over `send(record)`
 * is that it LOOKS deliberate in source — a reviewer scanning the
 * code sees an explicit null Callback and assumes the author knew
 * what they were doing. The 1-arg form at least makes the
 * fire-and-forget intent syntactically explicit. Both forms have
 * the same runtime semantics, but the 2-arg-with-null form is the
 * worse failure mode in code review.
 *
 * Each method below contains exactly one Bad call site → one fire.
 */
public class BadProducerSendNullCallback {

    private final Producer<String, String> producer;

    public BadProducerSendNullCallback(Producer<String, String> producer) {
        this.producer = producer;
    }

    /**
     * Shape 1: call through the Producer interface (most common —
     * dependency-injected producers are usually held as the
     * interface type). INVOKEINTERFACE on Producer.send(record,
     * Callback), preceding ACONST_NULL.
     */
    public void sendInterfaceNullCallback(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>("orders", key, value);
        // FIRES: ACONST_NULL preceding INVOKEINTERFACE Producer.send(...).
        producer.send(record, null);
    }

    /**
     * Shape 2: call through the concrete KafkaProducer class
     * (less common but appears in tests and tightly-coupled code).
     * INVOKEVIRTUAL on KafkaProducer.send(record, Callback),
     * preceding ACONST_NULL.
     */
    public void sendConcreteNullCallback(KafkaProducer<String, String> p, String key, String value) {
        // FIRES: ACONST_NULL preceding INVOKEVIRTUAL KafkaProducer.send(...).
        p.send(new ProducerRecord<>("orders", key, value), null);
    }

    /**
     * Shape 3: fire-and-forget loop pattern — looks like a
     * deliberately optimized send path because the 2-arg form
     * "skips" callback allocation. In reality the null callback
     * does NOT save any allocation (the sender thread checks for
     * null and skips invocation), and the only behavioral effect
     * is suppressing error visibility.
     *
     * The rule fires once per call site, so this loop contributes
     * one violation regardless of iteration count — the bytecode
     * instruction is what's flagged, not the dynamic invocation.
     */
    public void fireAndForgetLoop(int n, String key, String value) {
        for (int i = 0; i < n; i++) {
            // FIRES: still one violation reported (per static call site).
            producer.send(new ProducerRecord<>("metrics", key + i, value), null);
        }
    }
}
