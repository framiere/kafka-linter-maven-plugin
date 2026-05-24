package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * RULE: LAMBDA_PRODUCER_CALLBACK_EMPTY — per-call-site bytecode rule.
 *
 * The rule fires once per call site where:
 *   (a) the INVOKEVIRTUAL / INVOKEINTERFACE is on a recognized
 *       Producer owner — both the concrete `KafkaProducer` class
 *       AND the `Producer` interface count, since most app code
 *       holds a producer via the interface type;
 *   (b) the method name is "send";
 *   (c) the method descriptor's LAST argument type is the
 *       org.apache.kafka.clients.producer.Callback interface —
 *       i.e., this is the 2-arg `send(record, callback)` overload;
 *   (d) the instruction immediately preceding the INVOKE is an
 *       INVOKEDYNAMIC whose bootstrap is
 *       `java/lang/invoke/LambdaMetafactory.metafactory`;
 *   (e) the indy's bsmArgs contain a MethodHandle whose owner is
 *       the current class AND whose name starts with `lambda$`
 *       (the javac convention for synthetic lambda bodies — keeps
 *       method-refs `this::onSend` out of scope);
 *   (f) the synthetic lambda method's instruction list, after
 *       stripping LabelNode / LineNumberNode / FrameNode trivia,
 *       has exactly one significant instruction: RETURN.
 *
 * The shape this rule catches is `producer.send(record, (md, ex) -> {})`.
 * Runtime semantics are identical to `send(record, null)` — every
 * sender-thread failure (serialization error, broker NACK, retry
 * exhaustion, OutOfOrderSequenceException, RecordTooLargeException,
 * NotEnoughReplicasException, TLS handshake failure, schema-registry
 * outage, etc.) is silently swallowed — but the source-code shape
 * looks DELIBERATE. A reviewer scans the code, sees the `(md, ex)`
 * parameter pair, and assumes the author thought about error
 * handling. The empty body has zero observable effect at runtime.
 *
 * Concrete operational failure modes (lifted from the rule doc):
 *
 *   - Schema-registry outage. Every send completes with
 *     RestClientException in the callback; empty body → silent
 *     drop. The application logs "produced N records" while the
 *     broker received zero. The operator only notices once
 *     downstream consumers report lag = 0 with no traffic — a
 *     symptom that points everywhere except at the producer.
 *
 *   - Wrong topic name. The producer has a typo — writes to
 *     `orders` instead of `order`. The broker either rejects with
 *     UnknownTopicOrPartitionException or auto-creates a
 *     1-partition shadow (the ALLOW_AUTO_CREATE_TOPICS_TRUE rule
 *     makes this worse). Empty body → silent. Operator finds
 *     nothing in the right topic, eventually finds the wrong one.
 *
 *   - TLS handshake failure. Cert rotation breaks producer TLS;
 *     every send fails with SslAuthenticationException on the
 *     sender thread; empty body → silent. The downstream lag
 *     dashboard shows `lag = 0, no traffic` — the wrong reason.
 *
 * The fix is two lines and never wrong:
 *   `(md, ex) -> { if (ex != null) {
 *       log.error("send failed: topic={}", record.topic(), ex);
 *       errorMeter.increment();
 *   } }`
 *
 * Each method below contains exactly one Bad call site → one fire.
 */
public class BadLambdaProducerCallbackEmpty {

    private final Producer<String, String> producer;

    public BadLambdaProducerCallbackEmpty(Producer<String, String> producer) {
        this.producer = producer;
    }

    /**
     * Shape 1: call through the Producer interface (most common —
     * dependency-injected producers are usually held as the
     * interface type). Empty lambda body, INVOKEINTERFACE on
     * Producer.send(record, Callback) preceded by INVOKEDYNAMIC.
     */
    public void sendInterfaceEmptyLambda(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>("orders", key, value);
        // FIRES: empty (md, ex) -> {} preceding INVOKEINTERFACE Producer.send(...).
        producer.send(record, (md, ex) -> {});
    }

    /**
     * Shape 2: call through the concrete KafkaProducer class (less
     * common but appears in tests and tightly-coupled code).
     * INVOKEVIRTUAL on KafkaProducer.send(record, Callback)
     * preceded by INVOKEDYNAMIC for an empty lambda.
     */
    public void sendConcreteEmptyLambda(KafkaProducer<String, String> p, String key, String value) {
        // FIRES: empty (md, ex) -> {} preceding INVOKEVIRTUAL KafkaProducer.send(...).
        p.send(new ProducerRecord<>("orders", key, value), (md, ex) -> {});
    }

    /**
     * Shape 3: fire-and-forget loop pattern, the cargo-cult shape
     * where the empty callback was "added for safety" but never
     * filled in. The rule fires once per static call site, so this
     * loop contributes one violation regardless of iteration count.
     */
    public void fireAndForgetLoop(int n, String key, String value) {
        for (int i = 0; i < n; i++) {
            // FIRES: one violation reported per static call site.
            producer.send(new ProducerRecord<>("metrics", key + i, value), (md, ex) -> {});
        }
    }

    /**
     * Shape 4: lambda body that captures outer-scope state but
     * still leaves the body empty. The capture itself is consumed
     * by the INVOKEDYNAMIC's bsm arguments — it does NOT add an
     * instruction to the synthetic method's body. The rule still
     * sees exactly RETURN inside the lambda after trivia stripping.
     */
    public void emptyLambdaWithCapture(String key, String value) {
        final String topic = "orders";
        // FIRES: capture-of-topic does not add instructions to lambda body.
        producer.send(new ProducerRecord<>(topic, key, value), (md, ex) -> {});
    }
}
