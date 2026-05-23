package sample;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * RULE: DESER_JSON_TYPE_INFO_NO_ALLOWLIST — good shape, explicit subtypes.
 *
 * This file is silent. The rule only watches for method calls
 * `activateDefaultTyping` / `enableDefaultTyping` on `ObjectMapper`.
 * Declaring polymorphism with `@JsonTypeInfo` + `@JsonSubTypes`
 * annotations on a sealed-by-listing base class is the
 * recommended-by-Jackson safe shape: the deserializer can ONLY
 * pick from the listed subtypes, no matter what the JSON
 * payload claims. There is no method to flag because there is
 * no method call.
 *
 * The base class `Event` enumerates its subtypes inline; any
 * incoming `"type": "..."` value not in the list is rejected
 * with `InvalidTypeIdException` at deserialization time.
 *
 * This is the canonical "use explicit `@JsonSubTypes`" guidance
 * from the rule's docs — the lint should stay silent here.
 */
public final class GoodExplicitSubtypes {

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = OrderCreated.class, name = "ORDER_CREATED"),
            @JsonSubTypes.Type(value = OrderCancelled.class, name = "ORDER_CANCELLED")
    })
    public abstract static class Event {
    }

    public static final class OrderCreated extends Event {
        public String orderId;
    }

    public static final class OrderCancelled extends Event {
        public String orderId;
        public String reason;
    }

    /**
     * Deserializes an `Event` using the explicit subtype enumeration.
     * No `activateDefaultTyping` / `enableDefaultTyping` call on
     * the mapper — the rule has nothing to fire on.
     */
    public Event deserialize(byte[] payload) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(payload, Event.class);
    }
}
