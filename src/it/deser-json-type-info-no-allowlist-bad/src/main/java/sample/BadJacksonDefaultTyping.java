package sample;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;

/**
 * RULE: DESER_JSON_TYPE_INFO_NO_ALLOWLIST — bad shapes.
 *
 * Jackson polymorphic deserialization is the canonical RCE-class
 * footgun for any service that consumes JSON from an untrusted
 * source — and Kafka topics qualify as untrusted from a security
 * perspective regardless of how internal they feel. The rule
 * matches any call to one of two methods on `ObjectMapper`:
 *
 *   - `activateDefaultTyping(...)` — the modern API (Jackson 2.10+);
 *     all three overloads end up here.
 *   - `enableDefaultTyping(...)` — the deprecated API (still callable
 *     in Jackson 2.16, removed in 3.0).
 *
 * The rule does NOT inspect the `PolymorphicTypeValidator` argument
 * because:
 *   (a) the validator is selected at the call site, not the
 *       deserialization site — even a "safe" allow-list validator
 *       can be defeated by a downstream `setSubtypeResolver` call
 *       or by a custom validator that returns Validity.INDETERMINATE
 *       for anything in `java.util.*`;
 *   (b) the *common* mistake is to use `LaissezFaireSubTypeValidator`
 *       — which short-circuits ALL checks and is functionally
 *       equivalent to the pre-2.10 "no validator" mode that the
 *       CVE family exploited;
 *   (c) "did the developer actually think about the threat model
 *       at this call site" is the question the lint wants to force.
 *       Flag-every-call is the right tradeoff.
 *
 * Each method below contains exactly one call to a tracked method.
 * Expected fires: 4 (one per method).
 */
public final class BadJacksonDefaultTyping {

    /**
     * Bad #1 — `activateDefaultTyping(PolymorphicTypeValidator)`.
     *
     * The 1-arg overload uses the default applicability
     * (`OBJECT_AND_NON_CONCRETE`) and the default include-as
     * (`PROPERTY` → `@class` property). Even with the validator,
     * the JSON payload itself carries the class-name to instantiate
     * — and any class on the classpath whose name passes the
     * validator's `validateBaseType` check is reachable.
     *
     * `LaissezFaireSubTypeValidator.instance` is the validator
     * that returns `Validity.ALLOWED` for everything. Code review
     * usually doesn't catch it because the symbol looks innocuous.
     */
    public ObjectMapper oneArgActivate() {
        ObjectMapper mapper = new ObjectMapper();
        PolymorphicTypeValidator ptv = LaissezFaireSubTypeValidator.instance;
        mapper.activateDefaultTyping(ptv);  // FIRES
        return mapper;
    }

    /**
     * Bad #2 — `activateDefaultTyping(PolymorphicTypeValidator, DefaultTyping)`.
     *
     * The 2-arg overload lets the caller widen applicability to
     * `NON_FINAL` (every non-final class can be a deserialization
     * target — i.e., the gadget chains love this) or `EVERYTHING`
     * (truly the most permissive). The fix is not "pick a narrower
     * applicability" — it is "don't use default typing at all;
     * use explicit `@JsonSubTypes` enumerations instead."
     */
    public ObjectMapper twoArgActivateNonFinal() {
        ObjectMapper mapper = new ObjectMapper();
        PolymorphicTypeValidator ptv = LaissezFaireSubTypeValidator.instance;
        mapper.activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.NON_FINAL);  // FIRES
        return mapper;
    }

    /**
     * Bad #3 — `activateDefaultTyping(PolymorphicTypeValidator, DefaultTyping, JsonTypeInfo.As)`.
     *
     * The 3-arg overload also lets the caller control how the
     * type-name is embedded — `PROPERTY`, `WRAPPER_ARRAY`,
     * `WRAPPER_OBJECT`, `EXISTING_PROPERTY`. The choice changes
     * the JSON shape but does not change the threat model: the
     * type-name still comes from the payload.
     */
    public ObjectMapper threeArgActivateWrapperArray() {
        ObjectMapper mapper = new ObjectMapper();
        PolymorphicTypeValidator ptv = LaissezFaireSubTypeValidator.instance;
        mapper.activateDefaultTyping(  // FIRES
                ptv,
                ObjectMapper.DefaultTyping.NON_FINAL,
                JsonTypeInfo.As.WRAPPER_ARRAY);
        return mapper;
    }

    /**
     * Bad #4 — `enableDefaultTyping()` (deprecated form).
     *
     * The pre-2.10 API. Calling it on Jackson 2.10+ delegates to
     * `activateDefaultTyping(LaissezFaireSubTypeValidator.instance)`
     * — i.e., the worst possible defaults. Code that still calls
     * `enableDefaultTyping` is either pre-2.10 code that was
     * upgraded without an audit, or a copy-paste from a stale
     * Stack Overflow answer.
     *
     * The deprecation warning is suppressed locally so the IT
     * compiles cleanly under Jackson 2.16 — production code that
     * sees this warning should treat it as a code-action,
     * not a checkbox to silence.
     */
    @SuppressWarnings("deprecation")
    public ObjectMapper deprecatedEnableNoArg() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enableDefaultTyping();  // FIRES
        return mapper;
    }
}
