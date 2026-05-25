package sample;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;

/**
 * RULE: STREAMS_BRANCH_DEPRECATED — must fire on all three methods.
 *
 * <p>The three methods below exercise the three distinct bytecode
 * shapes the rule is required to catch for the deprecated
 * {@code KStream.branch(...)} overloads:
 *
 * <ol>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code KStream.branch(Predicate...)} — descriptor
 *       {@code ([Lorg/apache/kafka/streams/kstream/Predicate;)[Lorg/apache/kafka/streams/kstream/KStream;}.</li>
 *   <li>Direct {@code INVOKEINTERFACE} on
 *       {@code KStream.branch(Named, Predicate...)} — descriptor
 *       {@code (Lorg/apache/kafka/streams/kstream/Named;[Lorg/apache/kafka/streams/kstream/Predicate;)[Lorg/apache/kafka/streams/kstream/KStream;}.
 *       Same method name, but a distinct descriptor — the rule must
 *       catch both shapes because both are deprecated.</li>
 *   <li>{@code INVOKEDYNAMIC} unbound method-ref capture
 *       {@code KStream::branch} bound to a {@code Router} SAM whose
 *       signature is {@code (KStream<K,V>, Predicate<K,V>[]) ->
 *       KStream<K,V>[]} — javac resolves the method-ref to the
 *       legacy zero-named-arg branch overload by matching the SAM's
 *       receiver+arg-erasure (KStream, Predicate[]) and return-type
 *       erasure (KStream[]). javac emits an {@code INVOKEDYNAMIC}
 *       whose bsm-args contain a {@code REF_invokeInterface} handle
 *       pointing at the legacy method. The user-class bytecode at
 *       this site contains ZERO direct {@code INVOKEINTERFACE} on
 *       branch — only the {@code INVOKEDYNAMIC} + LambdaMetafactory
 *       bridge.</li>
 * </ol>
 *
 * <h2>Why this method is deprecated</h2>
 *
 * <p>{@code KStream.branch(Predicate...)} returns
 * {@code KStream<K,V>[]} — a raw Java array indexed by predicate
 * argument position. Four well-known correctness traps: (1) silent
 * drop on no-match (records matching no predicate vanish with no
 * error, no metric, no log line); (2) array-index coupling
 * (reordering the predicates silently reroutes records); (3) no
 * named branches in JMX (each branch becomes an anonymous processor
 * with a generated name); (4) {@code INVOKEDYNAMIC stream::branch}
 * captures silently bind to the legacy method whenever the SAM has
 * matching arity and argument erasure. KIP-418 (Kafka 2.8) replaced
 * the API with {@code KStream.split(Named).branch(Predicate,
 * Branched.as(name)).defaultBranch(Branched.as(other))} — named
 * branches addressable by string key, reorder-safe, drop-safe.
 */
public final class BadBranch {

    @SuppressWarnings({"deprecation", "unchecked"})
    public KStream<String, String>[] routeByPredicates(KStream<String, String> stream) {
        // MUST FIRE — direct INVOKEINTERFACE on the deprecated
        // KStream.branch(Predicate...) with descriptor
        // ([LPredicate;)[LKStream;.
        Predicate<String, String> isHot = (k, v) -> v.startsWith("hot:");
        Predicate<String, String> isWarm = (k, v) -> v.startsWith("warm:");
        Predicate<String, String> isCold = (k, v) -> v.startsWith("cold:");
        return stream.branch(isHot, isWarm, isCold);
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    public KStream<String, String>[] routeByNamedPredicates(KStream<String, String> stream) {
        // MUST FIRE — direct INVOKEINTERFACE on the deprecated
        // KStream.branch(Named, Predicate...) with descriptor
        // (LNamed;[LPredicate;)[LKStream;. Same method name as
        // above, distinct descriptor — both shapes are deprecated.
        Predicate<String, String> isHot = (k, v) -> v.startsWith("hot:");
        Predicate<String, String> isWarm = (k, v) -> v.startsWith("warm:");
        return stream.branch(Named.as("router"), isHot, isWarm);
    }

    @SuppressWarnings("deprecation")
    public Router<String, String> capturedRouter() {
        // MUST FIRE — INVOKEDYNAMIC unbound method-ref capture
        // targeting the deprecated branch(Predicate...) overload.
        // javac resolves `KStream::branch` by matching the Router
        // SAM's (receiver, arg-erasure, return-erasure) triple
        // (KStream, Predicate[], KStream[]) against the available
        // branch overloads, picking the legacy one. javac emits an
        // INVOKEDYNAMIC site whose bsm-args contain a
        // REF_invokeInterface handle on
        // ([LPredicate;)[LKStream;. The user-class bytecode here
        // contains ZERO direct INVOKEINTERFACE on the legacy
        // method — only the INVOKEDYNAMIC + LambdaMetafactory
        // bridge. A name-only MethodInsnNode walk misses this
        // entirely; the rule's bsm-arg walk catches it.
        return KStream::branch;
    }

    @FunctionalInterface
    public interface Router<K, V> {
        KStream<K, V>[] route(KStream<K, V> stream, Predicate<K, V>[] predicates);
    }
}
