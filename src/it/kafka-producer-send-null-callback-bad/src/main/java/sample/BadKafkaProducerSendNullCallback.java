package sample;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * RULE: PRODUCER_SEND_NULL_CALLBACK.
 *
 * Fires when the two-argument {@code send(ProducerRecord, Callback)} overload
 * is invoked with a literal {@code null} Callback. The compiler picked the
 * callback overload, so error-handling was clearly considered — but the caller
 * hard-coded {@code null}, providing zero failure visibility.
 */
public final class BadKafkaProducerSendNullCallback {

    private KafkaProducer<String, String> newProducer(Properties p) {
        return new KafkaProducer<>(p);
    }

    /** Anti-pattern: literal null Callback on KafkaProducer (concrete class, INVOKEVIRTUAL). */
    public void sendWithNullCallbackConcrete(Properties p) {
        try (KafkaProducer<String, String> producer = newProducer(p)) {
            ProducerRecord<String, String> r = new ProducerRecord<>("orders", "k", "v");
            producer.send(r, null); // FIRES — explicit null callback
        }
    }

    /** Anti-pattern: literal null Callback on Producer interface (INVOKEINTERFACE). */
    public void sendWithNullCallbackInterface(Producer<String, String> producer) {
        ProducerRecord<String, String> r = new ProducerRecord<>("orders", "k", "v");
        producer.send(r, null); // FIRES — same hazard, different invoke opcode
    }

    /** Anti-pattern: result discarded AND null callback — worst of both. */
    public void sendNullCallbackResultDiscarded(Properties p) {
        try (KafkaProducer<String, String> producer = newProducer(p)) {
            ProducerRecord<String, String> r = new ProducerRecord<>("orders", "k", "v");
            producer.send(r, null); // FIRES
        }
    }

    /** Anti-pattern: future captured but callback still null — Future may also be ignored downstream. */
    public Future<RecordMetadata> sendNullCallbackReturned(Properties p) {
        KafkaProducer<String, String> producer = newProducer(p);
        ProducerRecord<String, String> r = new ProducerRecord<>("orders", "k", "v");
        return producer.send(r, null); // FIRES
    }

    /** Control: real callback — must NOT fire. */
    public void sendWithRealCallback(Properties p) {
        try (KafkaProducer<String, String> producer = newProducer(p)) {
            ProducerRecord<String, String> r = new ProducerRecord<>("orders", "k", "v");
            producer.send(r, (meta, exc) -> {
                if (exc != null) {
                    System.err.println("send failed: " + exc.getMessage());
                }
            }); // OK — observable failure mode
        }
    }

    /** Control: callback stored in a local var (NOT a literal null) — must NOT fire (variable shape is by design out of scope). */
    public void sendWithCallbackVariable(Properties p) {
        try (KafkaProducer<String, String> producer = newProducer(p)) {
            ProducerRecord<String, String> r = new ProducerRecord<>("orders", "k", "v");
            Callback cb = (meta, exc) -> { /* no-op */ };
            producer.send(r, cb); // OK — concrete callback reference
        }
    }

    /** Control: one-arg send (no Callback overload) — covered by PRODUCER_SEND_NO_CALLBACK, not this rule. */
    public Future<RecordMetadata> sendNoCallbackOverload(Properties p) {
        try (KafkaProducer<String, String> producer = newProducer(p)) {
            ProducerRecord<String, String> r = new ProducerRecord<>("orders", "k", "v");
            return producer.send(r); // OK for THIS rule — different bytecode shape
        }
    }
}
