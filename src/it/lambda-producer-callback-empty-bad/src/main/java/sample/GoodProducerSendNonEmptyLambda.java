package sample;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Silent: every 2-arg send() in this class is called with a lambda
 * whose body is NOT empty. The rule's body check, after trivia
 * stripping (LabelNode / LineNumberNode / FrameNode), requires the
 * first significant instruction to be RETURN and nothing after it.
 * Each lambda below has at least one extra instruction (a method
 * invoke, a field load, a comparison branch, …) before the RETURN,
 * so the body shape does not match.
 */
public class GoodProducerSendNonEmptyLambda {

    private final Producer<String, String> producer;

    public GoodProducerSendNonEmptyLambda(Producer<String, String> producer) {
        this.producer = producer;
    }

    /**
     * Shape A: the canonical "two-line fix" the rule documentation
     * recommends. `if (ex != null) log.error(...)` plus an error
     * meter. Lambda body has IFNULL + GETSTATIC + INVOKE — clearly
     * not the empty shape.
     */
    public void sendWithErrorLogging(String key, String value) {
        producer.send(new ProducerRecord<>("orders", key, value), (md, ex) -> {
            if (ex != null) {
                System.err.println("send failed: " + ex.getMessage());
            }
        });
    }

    /**
     * Shape B: lambda that only loads metadata on the happy path
     * (e.g. for logging the assigned partition). The body is not
     * IDEAL — it still ignores the exception — but it is not the
     * EMPTY-body shape this rule targets. A separate, future rule
     * may flag "exception parameter never loaded"; that detection
     * requires slot-flow analysis and is intentionally out of
     * scope here to keep HIGH confidence on this rule.
     */
    public void sendThatLoadsMetadata(String key, String value) {
        producer.send(new ProducerRecord<>("orders", key, value), (md, ex) -> {
            System.out.println("sent to partition " + md.partition());
        });
    }

    /**
     * Shape C: lambda body is a single expression-statement that
     * still produces a real instruction in bytecode. A trivial
     * `int n = 1;` or similar would still emit ICONST + ISTORE
     * before the RETURN, breaking the single-RETURN shape.
     */
    public void sendWithSideEffect(String key, String value) {
        producer.send(new ProducerRecord<>("orders", key, value), (md, ex) -> {
            int unused = md == null ? 0 : 1;
        });
    }
}
