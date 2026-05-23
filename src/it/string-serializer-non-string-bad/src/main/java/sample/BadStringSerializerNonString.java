package sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * RULE: STRING_SERIALIZER_NON_STRING — class-scoped bytecode rule
 * that combines a CLASS-LEVEL gate with a PER-CONSTRUCTOR-CALL check.
 *
 * Class-level gate (must be true for ANY violation to fire on this
 * class): SOMEWHERE in the class body there is a call shape
 *   `<configHolder>.put("value.serializer", StringSerializer.class)`
 *   (or the FQCN string form "org.apache.kafka.common.serialization.StringSerializer")
 * — i.e., the class declares it serializes values as String.
 *
 * Per-call check (fires once per qualifying `new ProducerRecord<>(...)`):
 * the rule walks back from the constructor's INVOKESPECIAL to find the
 * instruction that produced the VALUE argument (skipping over the key
 * and topic args using ASM stack-effect arithmetic), then checks whether
 * that producing instruction has a STATICALLY-PROVABLY non-String type.
 * Provably-non-String sources include:
 *   - LDC of a non-String constant (e.g., class literal of a non-String
 *     type) — note that LDC of a String DOES return String, so a string
 *     literal is silent.
 *   - INVOKEVIRTUAL/INVOKESTATIC/INVOKEINTERFACE whose return type
 *     descriptor is not "Ljava/lang/String;".
 *   - GETFIELD/GETSTATIC whose field descriptor is not "Ljava/lang/String;".
 *   - NEW <NonString> (TypeInsnNode with non-"java/lang/String" desc).
 *   - ANEWARRAY of any element type (always Object[] or T[] — not String,
 *     except for ANEWARRAY java/lang/String which is String[], still not
 *     String — but the rule checks `!"java/lang/String".equals(tn.desc)`
 *     for ANEWARRAY, so ANEWARRAY of java/lang/String returns false →
 *     does NOT fire on String[]).
 *   - NEWARRAY / MULTIANEWARRAY — primitive arrays are always non-String.
 *   - CHECKCAST <NonString> — cast to a non-String type.
 *
 * NOT provably non-String (conservative — accepted false-negative):
 *   - ALOAD <local-variable> — type-inference would require a full data-flow
 *     analyzer, so the rule abstains rather than guess.
 *   - ACONST_NULL — null is intrinsically untyped, and a null value with
 *     StringSerializer is a tombstone shape (Kafka special-cases null
 *     payload to skip serialization entirely).
 *
 * This class declares value.serializer = StringSerializer at the class
 * level via the constructor's producer-config setup, then exercises
 * each provably-non-String value-producer shape in a dedicated method.
 * Each Bad method fires once.
 */
public class BadStringSerializerNonString {

    private byte[] cachedPayload = new byte[]{1, 2, 3};

    // The producer is declared as RAW TYPE — this is the shape the
    // rule actually catches in production code. When a producer is
    // parameterized as `KafkaProducer<String, String>`, javac will
    // reject a non-String value argument at compile time and the
    // rule's per-call check would never even fire (the code wouldn't
    // compile). The raw-type / Object-erasure shape (or
    // `KafkaProducer<String, Object>`) is the real-world incidence —
    // typically appears in legacy code, in code that builds the
    // producer via a factory method returning a raw KafkaProducer,
    // or in code that has been retro-fitted from one value type to
    // another without updating the configured serializer.
    @SuppressWarnings({"rawtypes", "unchecked"})
    private final KafkaProducer producer;

    public BadStringSerializerNonString() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put("client.id", "bad-string-serializer-non-string");
        props.put("acks", "all");
        props.put("enable.idempotence", "true");
        props.put("compression.type", "zstd");
        props.put("key.serializer", StringSerializer.class.getName());
        // The class-level gate — this LDC of "value.serializer" followed by
        // the StringSerializer class literal is what enables the rule's
        // per-call scan to fire on the ProducerRecord constructors below.
        props.put("value.serializer", StringSerializer.class);
        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Shape 1: NEW <DomainEvent> as the value argument.
     *
     * The bytecode for `new DomainEvent("id", "body")` is
     *   NEW sample/DomainEvent
     *   DUP
     *   LDC "id"
     *   LDC "body"
     *   INVOKESPECIAL sample/DomainEvent.<init>(Ljava/lang/String;Ljava/lang/String;)V
     * Walking back from the ProducerRecord constructor, the rule arrives
     * at the INVOKESPECIAL of DomainEvent's ctor — its return type is V
     * (constructor), but the actual stack producer is the preceding NEW.
     * The rule's findArgProducer follows stack-effect arithmetic; it
     * lands on the NEW TypeInsnNode whose desc is "sample/DomainEvent".
     * Since that's not "java/lang/String", the rule fires.
     */
    public void sendDomainEventDirectConstruction() {
        // FIRES: value arg is `new DomainEvent(...)` — provably non-String.
        producer.send(new ProducerRecord<>("orders", "k", new DomainEvent("o-42", "checkout")));
    }

    /**
     * Shape 2: method return type non-String.
     *
     * `buildEvent()` returns `DomainEvent`. The bytecode at the call
     * site is INVOKEVIRTUAL .../buildEvent()Lsample/DomainEvent; — the
     * rule reads the return type descriptor and sees it is not
     * "Ljava/lang/String;" → provably non-String.
     */
    public void sendFromMethodReturn() {
        // FIRES: value arg is the return of `buildEvent()` — return type
        // is sample/DomainEvent, provably non-String.
        producer.send(new ProducerRecord<>("orders", "k", buildEvent()));
    }

    /**
     * Shape 3: GETFIELD of non-String descriptor.
     *
     * `this.cachedPayload` has field descriptor `[B` (byte[]). The
     * rule reads the FieldInsnNode's desc and sees it is not
     * "Ljava/lang/String;" → provably non-String. Real-world incidence:
     * services that hold a cached protobuf/avro payload as a `byte[]`
     * field and accidentally ship it through a StringSerializer-configured
     * producer.
     */
    public void sendFromByteArrayField() {
        // FIRES: value arg is GETFIELD this.cachedPayload — descriptor [B,
        // provably non-String.
        producer.send(new ProducerRecord<>("orders", "k", cachedPayload));
    }

    /**
     * Shape 4: NEWARRAY (primitive byte array).
     *
     * `new byte[16]` compiles to NEWARRAY T_BYTE. The rule treats
     * NEWARRAY and MULTIANEWARRAY as unconditionally non-String
     * (primitive arrays have no String form).
     */
    public void sendNewPrimitiveArray() {
        // FIRES: value arg is NEWARRAY — provably non-String.
        producer.send(new ProducerRecord<>("orders", "k", new byte[16]));
    }

    /**
     * Shape 5: ANEWARRAY of a non-String reference type.
     *
     * `new Integer[8]` compiles to ANEWARRAY java/lang/Integer. The rule
     * checks the TypeInsnNode desc and sees it is not "java/lang/String"
     * → provably non-String. Note: ANEWARRAY java/lang/String would be
     * silent (rule treats String[] as a special case — see the Good
     * fixture's commentary).
     */
    public void sendNewIntegerArray() {
        // FIRES: value arg is ANEWARRAY java/lang/Integer — provably non-String.
        producer.send(new ProducerRecord<>("orders", "k", new Integer[8]));
    }

    private DomainEvent buildEvent() {
        return new DomainEvent("o-99", "refund");
    }
}
