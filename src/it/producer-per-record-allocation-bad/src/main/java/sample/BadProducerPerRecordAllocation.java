package sample;

import jakarta.ws.rs.POST;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;

/**
 * RULE: PRODUCER_PER_RECORD_ALLOCATION.
 *
 * <p>Fires when {@code new KafkaProducer(...)} appears inside a method
 * annotated with an HTTP/REST handler annotation. The rule looks at:
 * <ol>
 *   <li>{@code MethodNode.visibleAnnotations} and
 *       {@code invisibleAnnotations} — every annotation actually
 *       attached to the method's bytecode (parameter and class-level
 *       annotations are ignored; only method-level annotations
 *       count).</li>
 *   <li>If any annotation's descriptor matches the set of HTTP-handler
 *       descriptors maintained in {@code KafkaTypes.HTTP_HANDLER_ANNOTATIONS}
 *       (Spring MVC's {@code @RequestMapping} / {@code @GetMapping} /
 *       {@code @PostMapping} / {@code @PutMapping} / {@code @DeleteMapping}
 *       / {@code @PatchMapping}, JAX-RS jakarta and legacy javax variants:
 *       {@code @Path} / {@code @GET} / {@code @POST} / {@code @PUT} /
 *       {@code @DELETE} / {@code @PATCH} / {@code @HEAD} /
 *       {@code @OPTIONS}), then the method is "an HTTP handler".</li>
 *   <li>For each instruction in such a method: if it is an
 *       {@code INVOKESPECIAL} on {@code <init>} of
 *       {@code org/apache/kafka/clients/producer/KafkaProducer},
 *       fire — one violation per allocation site.</li>
 * </ol>
 *
 * <p>Why this matters — anatomy of a KafkaProducer construction:
 * <ol>
 *   <li>The constructor synchronously fetches cluster metadata.
 *       {@code max.block.ms} defaults to 60 seconds; under a flaky
 *       network or a partial broker outage the constructor will sit
 *       there for up to a minute before either succeeding or
 *       throwing. Inside an HTTP handler that means: every inbound
 *       request can spend up to 60 s parked in producer
 *       construction before the handler can do any work.</li>
 *   <li>The constructor spawns a {@code Sender} thread (long-lived
 *       I/O loop) and allocates the {@code RecordAccumulator}.
 *       {@code buffer.memory} defaults to 32 MiB — every per-request
 *       producer reserves 32 MiB of off-heap buffer that is freed
 *       only when the producer is closed. A web server handling 1000
 *       concurrent requests with a per-request producer is asking
 *       the JVM for 32 GiB of native memory.</li>
 *   <li>The first send forces a TLS handshake (if SSL is enabled),
 *       a SASL/OAUTHBEARER token round-trip (if SASL is enabled),
 *       and a Cluster metadata fetch. Per-request producers pay
 *       this latency tax on every request — typically 30-300 ms
 *       of unbatched overhead before any record actually goes onto
 *       the wire.</li>
 *   <li>{@code KafkaProducer} is documented as thread-safe and
 *       designed to be shared across threads as a long-lived
 *       singleton. The accumulator's whole point is to coalesce
 *       records from multiple threads into a single batch — and
 *       linger.ms / batch.size do nothing useful if every request
 *       has its own dedicated producer with one record in flight.</li>
 *   <li>Even if the producer were closed at the end of each
 *       handler (it usually isn't — see PRODUCER_NOT_CLOSED), the
 *       construct/destruct churn per HTTP request is wasted
 *       work: Sender thread creation, TLS handshake, metadata
 *       fetch, accumulator allocation, all on every request.</li>
 * </ol>
 *
 * <p>The correct shape: construct ONE producer in a constructor /
 * {@code @PostConstruct} / {@code @Bean} method and inject it as a
 * private final field. The producer survives for the application
 * lifetime; HTTP handlers reuse it via {@code producer.send(...)}.
 *
 * <p>Why we test three annotation families: the rule covers a UNION
 * of three production HTTP stacks (Spring MVC, JAX-RS Jakarta,
 * JAX-RS legacy javax). Each maps to a distinct ASM annotation
 * descriptor — a regression that drops one family from the
 * descriptor set would not be caught by exercising only one. The
 * three fires below each pin one descriptor family.
 *
 * <p>Bad class triggers THREE fires — one per annotation family:
 * <ol>
 *   <li>{@code emitOrderEventSpring} — annotated
 *       {@code @PostMapping} (Spring MVC).</li>
 *   <li>{@code emitOrderEventJakarta} — annotated
 *       {@code @jakarta.ws.rs.POST} (JAX-RS Jakarta namespace).</li>
 *   <li>{@code emitOrderEventLegacy} — annotated
 *       {@code @javax.ws.rs.POST} (JAX-RS pre-Jakarta legacy
 *       namespace).</li>
 * </ol>
 */
public final class BadProducerPerRecordAllocation {

    @PostMapping("/orders")
    public void emitOrderEventSpring(String payload) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props); // reported
        producer.send(new ProducerRecord<>("orders", payload));
        producer.close();
    }

    @POST
    public void emitOrderEventJakarta(String payload) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props); // reported
        producer.send(new ProducerRecord<>("orders", payload));
        producer.close();
    }

    @javax.ws.rs.POST
    public void emitOrderEventLegacy(String payload) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props); // reported
        producer.send(new ProducerRecord<>("orders", payload));
        producer.close();
    }

    /** Extra: the rule scans every method, so an annotated method that does NOT construct a producer is silent. */
    @GetMapping("/orders/health")
    public String health() {
        return "ok"; // silent — HTTP handler, but no `new KafkaProducer`.
    }
}
