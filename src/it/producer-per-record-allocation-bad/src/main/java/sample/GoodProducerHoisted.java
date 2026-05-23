package sample;

import jakarta.ws.rs.POST;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.web.bind.annotation.PostMapping;

/**
 * Negative control for PRODUCER_PER_RECORD_ALLOCATION — every method
 * in this class is silent, and the reader should be able to articulate
 * the precise reason for each silence:
 *
 * <ol>
 *   <li>{@code emitOrderEventHoisted} — annotated as an HTTP handler
 *       BUT the producer is a private final field initialised by the
 *       constructor. The handler body contains only a
 *       {@code producer.send(...)} — there is no
 *       {@code INVOKESPECIAL <init> KafkaProducer} bytecode anywhere
 *       in the handler, so the rule has nothing to fire on. This is
 *       the SHAPE the linter is trying to push everyone toward: one
 *       singleton producer reused across every request.</li>
 *   <li>{@code initialiseProducer} (the constructor body) DOES contain
 *       {@code new KafkaProducer(...)}, but the constructor carries
 *       no HTTP-handler annotation, so the rule's first guard
 *       ({@code isHttpHandler}) returns false and the entire method
 *       is skipped. This proves that the rule is annotation-gated
 *       and that legitimate producer construction sites
 *       (constructors, {@code @PostConstruct}, factory bean methods)
 *       are not collateral damage.</li>
 *   <li>{@code factoryMethod} — package-private helper that returns
 *       a brand-new producer. Looks dangerous out of context — the
 *       call site of this method might be a handler — but the
 *       rule operates per-method and only flags the syntactic call
 *       site of the {@code <init>} insn. Since the {@code <init>}
 *       insn lives in {@code factoryMethod} (which has no HTTP
 *       annotation), and not at the caller, the rule does not fire.
 *       This is a known limitation worth being honest about: a
 *       sufficiently determined misuse can hide the construction
 *       behind a helper. The mitigation is the spirit of the rule
 *       (long-lived producer pattern), not a strict syntactic
 *       guarantee.</li>
 * </ol>
 */
public final class GoodProducerHoisted {

    private final KafkaProducer<String, String> producer;

    public GoodProducerHoisted() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props); // silent — constructor has no HTTP annotation
    }

    @PostMapping("/orders/hoisted")
    public void emitOrderEventHoistedSpring(String payload) {
        producer.send(new ProducerRecord<>("orders", payload)); // silent — no `new KafkaProducer` here
    }

    @POST
    public void emitOrderEventHoistedJakarta(String payload) {
        producer.send(new ProducerRecord<>("orders", payload)); // silent — no `new KafkaProducer` here
    }

    static KafkaProducer<String, String> factoryMethod() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props); // silent — factory method has no HTTP annotation
    }
}
